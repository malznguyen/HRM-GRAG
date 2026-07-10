use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::retrieval::RetrievalClient;
use gmrag_api::retrieval::cleanup::{
    QdrantCleanupOptions, cleanup_qdrant_orphans, report_to_metadata,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Debug, Default)]
struct CleanupArgs {
    dry_run: bool,
    delete: bool,
    workspace_id: Option<Uuid>,
    tenant_id: Option<Uuid>,
    full_scan: bool,
    force: bool,
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
        Err(message) => {
            if message == "help" {
                print_usage();
                return;
            }
            eprintln!("{message}");
            print_usage();
            std::process::exit(1);
        }
    };

    // Dry-run mặc định an toàn; --delete mới mutate.
    let options = QdrantCleanupOptions {
        dry_run: !args.delete,
        delete: args.delete,
        workspace_id: args.workspace_id,
        tenant_id: args.tenant_id,
        full_scan: args.full_scan,
        scroll_page_size: 256,
        force: args.force,
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

    let retrieval = RetrievalClient::from_env().expect("Failed to load retrieval configuration");

    match cleanup_qdrant_orphans(&pool, &retrieval, &options).await {
        Ok(report) => {
            println!("mode={}", report.mode);
            println!("dry_run={}", report.dry_run);
            println!("candidates_from_outbox={}", report.candidates_from_outbox);
            println!("candidates_from_audit={}", report.candidates_from_audit);
            println!(
                "candidates_from_full_scan={}",
                report.candidates_from_full_scan
            );
            println!("unique_delete_targets={}", report.unique_delete_targets);
            println!("deletes_attempted={}", report.deletes_attempted);
            println!("deletes_succeeded={}", report.deletes_succeeded);
            println!("deletes_failed={}", report.deletes_failed);
            println!("outbox_requeued={}", report.outbox_requeued);

            if !report.sample_targets.is_empty() {
                println!("sample_targets:");
                for sample in &report.sample_targets {
                    println!("  {sample}");
                }
            }
            if !report.errors.is_empty() {
                println!("errors:");
                for err in &report.errors {
                    println!("  {err}");
                }
            }

            let event = if args.delete {
                AuditEventType::QdrantCleanupCompleted
            } else {
                AuditEventType::QdrantCleanupDryRun
            };

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(event).with_metadata(report_to_metadata(&report)),
            )
            .await;

            if report.deletes_failed > 0 {
                std::process::exit(2);
            }
        }
        Err(err) => {
            eprintln!("Qdrant orphan cleanup failed: {err}");

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::QdrantCleanupFailed).with_metadata(json!({
                    "delete": args.delete,
                    "workspace_id": args.workspace_id,
                    "tenant_id": args.tenant_id,
                    "full_scan": args.full_scan,
                    "error_code": sanitize_error_code(&err.to_string())
                })),
            )
            .await;

            std::process::exit(1);
        }
    }
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<CleanupArgs, String> {
    let mut parsed = CleanupArgs {
        dry_run: true,
        delete: false,
        workspace_id: None,
        tenant_id: None,
        full_scan: false,
        force: false,
    };

    let mut iter = args.peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err("help".to_string()),
            "--dry-run" => {
                parsed.dry_run = true;
                parsed.delete = false;
            }
            "--delete" => {
                parsed.delete = true;
                parsed.dry_run = false;
            }
            "--force" => parsed.force = true,
            "--full-scan" => parsed.full_scan = true,
            "--workspace-id" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--workspace-id requires a UUID value".to_string())?;
                parsed.workspace_id = Some(parse_uuid_arg("--workspace-id", &value)?);
            }
            "--tenant-id" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--tenant-id requires a UUID value".to_string())?;
                parsed.tenant_id = Some(parse_uuid_arg("--tenant-id", &value)?);
            }
            other if other.starts_with("--workspace-id=") => {
                let value = other.trim_start_matches("--workspace-id=");
                parsed.workspace_id = Some(parse_uuid_arg("--workspace-id", value)?);
            }
            other if other.starts_with("--tenant-id=") => {
                let value = other.trim_start_matches("--tenant-id=");
                parsed.tenant_id = Some(parse_uuid_arg("--tenant-id", value)?);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if parsed.workspace_id.is_some() && parsed.tenant_id.is_some() {
        return Err("use either --workspace-id or --tenant-id, not both".to_string());
    }

    Ok(parsed)
}

fn parse_uuid_arg(flag: &str, value: &str) -> Result<Uuid, String> {
    Uuid::parse_str(value).map_err(|_| format!("{flag} must be a valid UUID"))
}

fn print_usage() {
    eprintln!(
        "Usage: cleanup-qdrant-orphans [OPTIONS]

Priority cleanup of orphan Qdrant vectors after failed document/workspace deletes.

Options:
  --dry-run              Report only (default)
  --delete               Actually delete matching points
  --force                Allow --delete when workspace/tenant rows still exist in SQL
                         (default: refuse scoped live deletes to avoid wiping live vectors)
  --workspace-id <UUID>  Scoped workspace points (refuse --delete if workspace still live unless --force)
  --tenant-id <UUID>     Scoped tenant workspaces from SQL (empty list = hard error;
                         refuse --delete while workspaces still live unless --force)
  --full-scan            Optional: scroll entire Qdrant collection and compare to SQL
  -h, --help             Show this help

Examples:
  cargo run --bin cleanup-qdrant-orphans -- --dry-run
  cargo run --bin cleanup-qdrant-orphans -- --delete
  cargo run --bin cleanup-qdrant-orphans -- --workspace-id <uuid> --delete
  cargo run --bin cleanup-qdrant-orphans -- --workspace-id <uuid> --delete --force
  cargo run --bin cleanup-qdrant-orphans -- --full-scan --dry-run

Notes:
  - Tenant already cascaded (no workspaces in SQL): do NOT use --tenant-id; use
    outbox/audit mode, --workspace-id with captured ids, or --full-scan.
  - Prefer capturing workspace ids BEFORE SQL tenant cascade.
"
    );
}
