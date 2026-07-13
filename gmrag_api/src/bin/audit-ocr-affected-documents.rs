//! Operator OCR-004: audit tài liệu bị mock OCR / NEEDS_OCR; dry-run mặc định;
//! apply requeue chỉ khi production OCR capability mở.

use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event};
use gmrag_api::ingestion::ocr::OcrCapability;
use gmrag_api::ingestion::ocr_audit::{
    ObjectPresenceCheck, OcrAuditOptions, OcrAuditPersistKind, OcrAuditReport, default_capability,
    format_human_report, report_to_audit_metadata, run_ocr_affected_audit,
    should_persist_audit_event,
};
use gmrag_api::storage::{StorageClient, StorageConfig};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Debug, Default)]
struct Args {
    json: bool,
    apply: bool,
    limit: Option<i64>,
    sample_limit: Option<usize>,
    tenant_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
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
            std::process::exit(1);
        }
    };

    // Dry-run mặc định; --apply mới request mutation (vẫn bị refuse nếu OCR đóng).
    let options = OcrAuditOptions {
        dry_run: !args.apply,
        apply: args.apply,
        limit: args.limit.unwrap_or(50),
        sample_limit: args.sample_limit.unwrap_or(20),
        tenant_id: args.tenant_id,
        workspace_id: args.workspace_id,
        max_attempts: gmrag_api::ingestion::jobs::IngestionWorkerConfig::from_env().max_attempts,
    };

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    let capability = default_capability();
    let report = execute_audit(&pool, &options, &capability).await;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("serialize report")
        );
    } else {
        print!("{}", format_human_report(&report));
    }

    // Dry-run thuần: không INSERT audit_events (read-only). Chỉ --apply completed/refused.
    maybe_write_audit_event(&pool, &report).await;

    if report.apply_refused {
        std::process::exit(2);
    }
    if !report.errors.is_empty() {
        std::process::exit(1);
    }
}

async fn execute_audit(
    pool: &sqlx::PgPool,
    options: &OcrAuditOptions,
    capability: &OcrCapability,
) -> OcrAuditReport {
    // Chỉ mở StorageClient khi apply thực sự có thể chạy (capability + apply flag).
    if options.apply && capability.available {
        let storage_config = StorageConfig::from_env().expect("Failed to load storage config");
        let storage = StorageClient::from_config(storage_config).await;
        return run_ocr_affected_audit(
            pool,
            options,
            capability,
            ObjectPresenceCheck::Storage(&storage),
        )
        .await
        .expect("OCR audit apply failed");
    }

    run_ocr_affected_audit(pool, options, capability, ObjectPresenceCheck::Skip)
        .await
        .expect("OCR audit failed")
}

async fn maybe_write_audit_event(pool: &sqlx::PgPool, report: &OcrAuditReport) {
    let Some(kind) = should_persist_audit_event(report) else {
        return;
    };
    let event_type = match kind {
        OcrAuditPersistKind::ApplyCompleted => AuditEventType::OcrAffectedDocumentsApplyCompleted,
        OcrAuditPersistKind::ApplyRefused => AuditEventType::OcrAffectedDocumentsApplyRefused,
    };

    let mut event = AuditEventRecord::new(event_type)
        .with_target("ocr_audit", "ocr-004")
        .with_metadata(report_to_audit_metadata(report));
    if let Some(tenant_id) = report.tenant_id {
        event = event.with_tenant_id(tenant_id);
    }
    if let Some(workspace_id) = report.workspace_id {
        event = event.with_workspace_id(workspace_id);
    }

    if let Err(err) = insert_audit_event(pool, event).await {
        eprintln!("warning: failed to write audit_events row: {err}");
    }
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut parsed = Args::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err("help".to_string()),
            "--json" => parsed.json = true,
            "--apply" => parsed.apply = true,
            "--limit" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--limit requires a positive integer".to_string())?;
                let limit: i64 = value
                    .parse()
                    .map_err(|_| "--limit must be an integer".to_string())?;
                if !(1..=500).contains(&limit) {
                    return Err("--limit must be between 1 and 500".to_string());
                }
                parsed.limit = Some(limit);
            }
            "--sample-limit" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--sample-limit requires a positive integer".to_string())?;
                let sample_limit: usize = value
                    .parse()
                    .map_err(|_| "--sample-limit must be an integer".to_string())?;
                if sample_limit == 0 {
                    return Err("--sample-limit must be >= 1".to_string());
                }
                parsed.sample_limit = Some(sample_limit);
            }
            "--tenant-id" => parsed.tenant_id = Some(parse_uuid("--tenant-id", args.next())?),
            "--workspace-id" => {
                parsed.workspace_id = Some(parse_uuid("--workspace-id", args.next())?)
            }
            _ => return Err(format!("Unknown argument: {arg}")),
        }
    }
    Ok(parsed)
}

fn parse_uuid(flag: &str, value: Option<String>) -> Result<Uuid, String> {
    value
        .ok_or_else(|| format!("{flag} requires a UUID value"))?
        .parse()
        .map_err(|_| format!("{flag} must be a UUID"))
}

fn print_usage() {
    println!(
        "audit-ocr-affected-documents (OCR-004)\n\n\
Usage:\n  \
  cargo run --bin audit-ocr-affected-documents -- [--json] [--limit N] [--workspace-id UUID] [--tenant-id UUID]\n  \
  cargo run --bin audit-ocr-affected-documents -- --apply   # refused until production OCR is integrated\n\n\
Options:\n  \
  --json                 Machine-readable JSON report (no document/chunk text).\n  \
  --limit N              Max candidates (1..500, default 50). Stable order: created_at, document_id.\n  \
  --sample-limit N       Max sample rows in report (default 20).\n  \
  --tenant-id UUID       Limit to one tenant.\n  \
  --workspace-id UUID    Limit to one workspace.\n  \
  --apply                Request bounded requeue. Refused when OCR capability is closed.\n  \
  --help, -h             Show this help.\n\n\
Evidence categories (SQL-observed only):\n  \
  mock_ocr_chunk         document_chunks.original_text contains mock_ocr_text marker\n  \
  needs_ocr_terminal     documents.status=FAILED AND failure_code=NEEDS_OCR\n\n\
Safety:\n  \
  Dry-run by default is fully read-only: no SQL mutation (including no audit_events),\n  \
  no object storage writes, no Qdrant mutation. Report prints to stdout only.\n  \
  --apply may write metadata-only audit_events (refused or completed); never document/chunk text.\n  \
  Apply requeue is idempotent (one active job per document) and checks original object existence.\n  \
  Does not integrate Tesseract; does not claim real-corpus completion.\n"
    );
}
