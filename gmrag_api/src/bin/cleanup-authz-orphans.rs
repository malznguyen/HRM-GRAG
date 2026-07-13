use std::{fs::OpenOptions, io::Write, path::PathBuf};

use chrono::Utc;
use gmrag_api::{
    auth::authz::{AuthzClient, TupleKey, delete_tuples_fga_first},
    authz_orphan_report::{AuthzOrphanFinding, run_authz_orphan_report},
};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;

#[derive(Default)]
struct Args {
    delete: bool,
    yes: bool,
}

#[derive(Serialize)]
struct RecoveryFile<'a> {
    generated_at: String,
    tuples: Vec<&'a TupleKey>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            print_usage();
            std::process::exit(1);
        }
    };
    if args.delete && !args.yes {
        eprintln!("--delete requires --yes.");
        std::process::exit(1);
    }

    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| exit_with("DATABASE_URL must be set."));
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap_or_else(|_| exit_with("Could not connect to PostgreSQL."));
    let authz = AuthzClient::from_env()
        .unwrap_or_else(|_| exit_with("OpenFGA configuration is incomplete."));
    let report = run_authz_orphan_report(&pool, &authz)
        .await
        .unwrap_or_else(|error| {
            exit_with(&format!("Could not generate authz orphan report: {error}"))
        });

    print_summary(&report.findings, args.delete);
    if !args.delete {
        println!("DRY RUN — no OpenFGA tuple, SQL row, audit row, or outbox row was modified.");
        println!("To remove this exact class of orphan after review:");
        println!("  cargo run --locked --bin cleanup-authz-orphans -- --delete --yes");
        return;
    }

    let recovery_path = write_recovery_file(&report.findings)
        .unwrap_or_else(|error| exit_with(&format!("Could not write recovery file: {error}")));
    let tuples: Vec<TupleKey> = report
        .findings
        .iter()
        .map(|finding| finding.tuple.clone())
        .collect();
    if let Err(error) = delete_tuples_fga_first(&authz, &tuples).await {
        eprintln!(
            "OpenFGA cleanup stopped; SQL was not changed. Recovery file: {}",
            recovery_path.display()
        );
        exit_with(&format!("Could not remove orphan tuples: {error}"));
    }
    println!("deleted_tuples={}", tuples.len());
    println!("recovery_file={}", recovery_path.display());
}

fn print_summary(findings: &[AuthzOrphanFinding], deleting: bool) {
    let audit_matched = findings
        .iter()
        .filter(|finding| !finding.audit_matches.is_empty())
        .count();
    let without_audit = findings.len() - audit_matched;
    let mode = if deleting { "DELETE" } else { "DRY RUN" };
    println!("=== cleanup-authz-orphans ({mode}) ===");
    println!("orphan_tuples={}", findings.len());
    println!("audit_matched_tuples={audit_matched}");
    println!("without_audit_tuples={without_audit}");
    println!(
        "audit_matched_interpretation=corroborates a historical audited mutation but does not prove causality alone"
    );
    println!("without_audit_interpretation=suspected test or fixture residue; not proven");
}

fn write_recovery_file(
    findings: &[AuthzOrphanFinding],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
    let path = std::env::current_dir()?.join(format!("authz-orphan-recovery-{timestamp}.json"));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    let recovery = RecoveryFile {
        generated_at: Utc::now().to_rfc3339(),
        tuples: findings.iter().map(|finding| &finding.tuple).collect(),
    };
    let mut writer = std::io::BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, &recovery)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(path)
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--dry-run" => {}
            "--delete" => args.delete = true,
            "--yes" => args.yes = true,
            "--help" | "-h" => return Err("help".to_string()),
            _ => return Err(format!("Unknown argument: {arg}")),
        }
    }
    Ok(args)
}

fn print_usage() {
    println!(
        "cleanup-authz-orphans usage:\n  cargo run --locked --bin cleanup-authz-orphans -- --dry-run\n  cargo run --locked --bin cleanup-authz-orphans -- --delete --yes\n\nDefault is read-only. --delete requires --yes and writes authz-orphan-recovery-<timestamp>.json before OpenFGA mutation."
    );
}

fn exit_with(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1)
}
