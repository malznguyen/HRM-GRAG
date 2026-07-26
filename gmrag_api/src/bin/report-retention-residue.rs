//! Operator LIFE-007: object/vector nào còn sót lại sau delete.
//!
//! Read-only tuyệt đối — không xoá point/object, không enqueue outbox, không ghi
//! `audit_events`. Muốn dọn thì dùng `cleanup-qdrant-orphans` / `cleanup-storage-objects`.

use gmrag_api::retention_report::{
    RETENTION_EXIT_ERROR, RetentionReportOptions, format_human_report, retention_exit_code,
    run_retention_report,
};
use gmrag_api::retrieval::RetrievalClient;
use gmrag_api::storage::{StorageClient, StorageConfig};
use sqlx::postgres::PgPoolOptions;

#[derive(Debug)]
struct Args {
    json: bool,
    skip_vectors: bool,
    skip_objects: bool,
    sample_limit: usize,
    scroll_page_size: usize,
}

impl Default for Args {
    fn default() -> Self {
        let defaults = RetentionReportOptions::default();
        Self {
            json: false,
            skip_vectors: false,
            skip_objects: false,
            sample_limit: defaults.sample_limit,
            scroll_page_size: defaults.scroll_page_size,
        }
    }
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) if message == "help" => {
            print_usage();
            return;
        }
        Err(message) => {
            eprintln!("{message}");
            print_usage();
            std::process::exit(RETENTION_EXIT_ERROR);
        }
    };

    let options = RetentionReportOptions {
        probe_vectors: !args.skip_vectors,
        probe_objects: !args.skip_objects,
        scroll_page_size: args.scroll_page_size,
        sample_limit: args.sample_limit,
    };

    let database_url = match std::env::var("DATABASE_URL") {
        Ok(value) => value,
        Err(_) => exit_with("DATABASE_URL must be set."),
    };
    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(_) => exit_with("Could not connect to PostgreSQL."),
    };

    // Chỉ mở client cho store thực sự được probe — tránh fail vì config store bị tắt.
    let retrieval = if options.probe_vectors {
        match RetrievalClient::from_env() {
            Ok(client) => Some(client),
            Err(_) => exit_with(
                "Retrieval (Qdrant) configuration is incomplete; pass --skip-vectors to report objects only.",
            ),
        }
    } else {
        None
    };

    let storage = if options.probe_objects {
        match StorageConfig::from_env() {
            Ok(config) => Some(StorageClient::from_config(config).await),
            Err(_) => exit_with(
                "Object storage configuration is incomplete; pass --skip-objects to report vectors only.",
            ),
        }
    } else {
        None
    };

    let report =
        match run_retention_report(&pool, retrieval.as_ref(), storage.as_ref(), &options).await {
            Ok(report) => report,
            Err(error) => exit_with(&format!("Could not generate retention report: {error}")),
        };

    if args.json {
        match serde_json::to_string_pretty(&report) {
            Ok(rendered) => println!("{rendered}"),
            Err(error) => exit_with(&format!("Could not serialize retention report: {error}")),
        }
    } else {
        print!("{}", format_human_report(&report));
    }

    std::process::exit(retention_exit_code(&report));
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut parsed = Args::default();
    let mut iter = args.peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err("help".to_string()),
            "--json" => parsed.json = true,
            // Read-only là mặc định duy nhất; nhận --dry-run để khớp thói quen operator.
            "--dry-run" => {}
            "--skip-vectors" => parsed.skip_vectors = true,
            "--skip-objects" => parsed.skip_objects = true,
            "--sample-limit" => {
                parsed.sample_limit = parse_bounded("--sample-limit", iter.next(), 1, 10_000)?;
            }
            "--scroll-page-size" => {
                parsed.scroll_page_size =
                    parse_bounded("--scroll-page-size", iter.next(), 1, 1_000)?;
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if parsed.skip_vectors && parsed.skip_objects {
        return Err(
            "--skip-vectors and --skip-objects together would probe nothing; drop one".to_string(),
        );
    }

    Ok(parsed)
}

fn parse_bounded(
    flag: &str,
    value: Option<String>,
    min: usize,
    max: usize,
) -> Result<usize, String> {
    let raw = value.ok_or_else(|| format!("{flag} requires an integer value"))?;
    let parsed: usize = raw
        .parse()
        .map_err(|_| format!("{flag} must be an integer"))?;
    if !(min..=max).contains(&parsed) {
        return Err(format!("{flag} must be between {min} and {max}"));
    }
    Ok(parsed)
}

fn print_usage() {
    println!(
        "report-retention-residue (LIFE-007)\n\n\
Usage:\n  \
  cargo run --bin report-retention-residue -- [--json] [--sample-limit N]\n  \
  cargo run --bin report-retention-residue -- --skip-vectors    # objects only\n  \
  cargo run --bin report-retention-residue -- --skip-objects    # vectors only\n\n\
Shows which Qdrant vectors and object-storage objects still exist after their owning\n\
SQL row is gone, and whether anything is still on the hook to remove them.\n\n\
Options:\n  \
  --json                  Machine-readable JSON report.\n  \
  --sample-limit N        Max residue rows listed (1..10000, default 50). Counts stay complete.\n  \
  --scroll-page-size N    Qdrant scroll page size (1..1000, default 256).\n  \
  --skip-vectors          Do not scroll Qdrant (report marks vectors as not probed).\n  \
  --skip-objects          Do not list object storage (report marks objects as not probed).\n  \
  --dry-run               Accepted and ignored; this command is always read-only.\n  \
  --help, -h              Show this help.\n\n\
Residue classes:\n  \
  recovery_pending        An outbox row (PENDING/FAILED) still owes this delete — the worker will clear it.\n  \
  recovery_dead           The owing outbox row is DEAD; retries are exhausted and an operator must act.\n  \
  unrecovered             A delete was audited but no outbox row owes it — nothing will clean this up.\n  \
  unexplained             No owing outbox row and no matching delete event; provenance unknown.\n\n\
Exit codes:\n  \
  0  No residue needing operator action (recovery_pending alone still exits 0).\n  \
  1  Could not produce the report (config, PostgreSQL, Qdrant, or object storage error).\n  \
  2  recovery_dead, unrecovered, or unexplained residue found.\n\n\
Safety:\n  \
  Fully read-only: SELECT only, Qdrant scroll only, object list only. No delete, no\n  \
  outbox enqueue, no audit_events row. Use cleanup-qdrant-orphans or\n  \
  cleanup-storage-objects to remediate what this report finds.\n"
    );
}

fn exit_with(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(RETENTION_EXIT_ERROR)
}
