use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::storage::cleanup::{
    PrefixCleanupReport, StorageCleanupOptions, build_tenant_prefix, cleanup_prefix,
    resolve_workspace_prefix, scan_documents_and_orphans,
};
use gmrag_api::storage::{StorageClient, StorageConfig};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const DEFAULT_LOOP_INTERVAL_SECS: u64 = 3600;

#[derive(Debug, Default)]
struct CleanupArgs {
    dry_run: bool,
    delete: bool,
    delete_orphans: bool,
    loop_mode: bool,
    interval_secs: u64,
    workspace_id: Option<Uuid>,
    tenant_id: Option<Uuid>,
    mark_missing_documents_failed: bool,
}

#[derive(Debug, Clone)]
enum CleanupMode {
    Scan,
    WorkspacePrefix {
        workspace_id: Uuid,
        tenant_id_override: Option<Uuid>,
    },
    TenantPrefix {
        tenant_id: Uuid,
    },
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

    if let Err(message) = validate_args(&args) {
        eprintln!("{message}");
        print_usage();
        std::process::exit(1);
    }

    let mode = match resolve_mode(&args) {
        Ok(mode) => mode,
        Err(message) => {
            eprintln!("{message}");
            print_usage();
            std::process::exit(1);
        }
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

    let storage_config = StorageConfig::from_env().expect("Failed to load storage config");
    let storage = StorageClient::from_config(storage_config).await;

    if args.loop_mode {
        run_loop(&pool, &storage, &args, mode).await;
        return;
    }

    let operation_result = run_cleanup(&pool, &storage, &args, mode).await;

    match operation_result {
        Ok(metadata) => {
            let success_event = if args.delete {
                AuditEventType::StorageCleanupCompleted
            } else {
                AuditEventType::StorageCleanupDryRun
            };

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(success_event).with_metadata(metadata),
            )
            .await;
        }
        Err(err) => {
            eprintln!("Storage cleanup failed: {err}");

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::StorageCleanupFailed).with_metadata(json!({
                    "delete": args.delete,
                    "delete_orphans": args.delete_orphans,
                    "workspace_id": args.workspace_id,
                    "tenant_id": args.tenant_id,
                    "error_code": sanitize_error_code(&err.to_string())
                })),
            )
            .await;

            std::process::exit(1);
        }
    }
}

async fn run_loop(
    pool: &sqlx::PgPool,
    storage: &StorageClient,
    args: &CleanupArgs,
    mode: CleanupMode,
) {
    let interval = std::time::Duration::from_secs(args.interval_secs);

    loop {
        let result = run_cleanup(pool, storage, args, mode.clone()).await;
        match result {
            Ok(metadata) => {
                let _ = insert_audit_event(
                    pool,
                    AuditEventRecord::new(AuditEventType::StorageOrphanScanReport).with_metadata(
                        json!({
                            "success": true,
                            "interval_secs": args.interval_secs,
                            "report": metadata
                        }),
                    ),
                )
                .await;
            }
            Err(err) => {
                let _ = insert_audit_event(
                    pool,
                    AuditEventRecord::new(AuditEventType::StorageOrphanScanReport).with_metadata(
                        json!({
                            "success": false,
                            "interval_secs": args.interval_secs,
                            "error_code": sanitize_error_code(&err.to_string())
                        }),
                    ),
                )
                .await;

                eprintln!("Storage orphan scan failed: {err}");
                std::process::exit(1);
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Received shutdown signal; stopping storage-orphan-scan");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

async fn run_cleanup(
    pool: &sqlx::PgPool,
    storage: &StorageClient,
    args: &CleanupArgs,
    mode: CleanupMode,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    match mode {
        CleanupMode::Scan => {
            let options = StorageCleanupOptions {
                allow_delete: args.delete,
                delete_orphans: args.delete_orphans,
                mark_missing_documents_failed: args.mark_missing_documents_failed,
            };

            let report = scan_documents_and_orphans(pool, storage, options).await?;
            print_scan_report(&report, args);

            Ok(json!({
                "mode": "scan",
                "delete": args.delete,
                "delete_orphans": args.delete_orphans,
                "mark_missing_documents_failed": args.mark_missing_documents_failed,
                "checked_documents": report.checked_documents,
                "missing_document_objects": report.missing_document_objects.len(),
                "marked_failed_documents": report.marked_failed_documents,
                "listed_objects": report.listed_objects,
                "orphan_objects": report.orphan_object_keys.len(),
                "deleted_orphan_objects": report.deleted_orphan_objects
            }))
        }
        CleanupMode::WorkspacePrefix {
            workspace_id,
            tenant_id_override,
        } => {
            let prefix = resolve_workspace_prefix(pool, workspace_id, tenant_id_override).await?;
            let report = cleanup_prefix(storage, prefix.clone(), args.delete).await?;
            print_prefix_report("workspace", &report, args.delete);

            Ok(json!({
                "mode": "workspace_prefix",
                "workspace_id": workspace_id,
                "tenant_id_override": tenant_id_override,
                "prefix": prefix,
                "delete": args.delete,
                "listed_objects": report.listed_objects,
                "deleted_objects": report.deleted_objects
            }))
        }
        CleanupMode::TenantPrefix { tenant_id } => {
            let prefix = build_tenant_prefix(tenant_id);
            let report = cleanup_prefix(storage, prefix.clone(), args.delete).await?;
            print_prefix_report("tenant", &report, args.delete);

            Ok(json!({
                "mode": "tenant_prefix",
                "tenant_id": tenant_id,
                "prefix": prefix,
                "delete": args.delete,
                "listed_objects": report.listed_objects,
                "deleted_objects": report.deleted_objects
            }))
        }
    }
}

fn print_scan_report(
    report: &gmrag_api::storage::cleanup::StorageCleanupReport,
    args: &CleanupArgs,
) {
    println!(
        "Storage scan complete: checked_documents={}, missing_document_objects={}, listed_objects={}, orphan_objects={}, deleted_orphan_objects={}",
        report.checked_documents,
        report.missing_document_objects.len(),
        report.listed_objects,
        report.orphan_object_keys.len(),
        report.deleted_orphan_objects
    );

    if !report.missing_document_objects.is_empty() {
        println!("Missing document objects:");
        for item in report.missing_document_objects.iter().take(20) {
            println!(
                "  document_id={} workspace_id={} status={} object_key={}",
                item.document_id, item.workspace_id, item.status, item.object_key
            );
        }
    }

    if !report.orphan_object_keys.is_empty() {
        println!("Orphan storage objects:");
        for object_key in report.orphan_object_keys.iter().take(20) {
            println!("  object_key={object_key}");
        }
    }

    if !args.delete {
        println!("Dry-run mode: no storage objects were deleted.");
    }

    if args.mark_missing_documents_failed {
        println!(
            "Marked {} missing documents from PROCESSING to FAILED.",
            report.marked_failed_documents
        );
    }
}

fn print_prefix_report(scope: &str, report: &PrefixCleanupReport, delete_enabled: bool) {
    println!(
        "{} prefix scan complete: prefix={}, listed_objects={}, deleted_objects={}",
        scope, report.prefix, report.listed_objects, report.deleted_objects
    );

    for object_key in report.object_keys.iter().take(20) {
        println!("  object_key={object_key}");
    }

    if !delete_enabled {
        println!("Dry-run mode: no storage objects were deleted.");
    }
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<CleanupArgs, String> {
    let mut parsed = CleanupArgs {
        dry_run: true,
        interval_secs: DEFAULT_LOOP_INTERVAL_SECS,
        ..CleanupArgs::default()
    };

    let mut pending = args.peekable();

    while let Some(arg) = pending.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err("help".to_string()),
            "--dry-run" => {
                parsed.dry_run = true;
            }
            "--delete" => {
                parsed.delete = true;
                parsed.dry_run = false;
            }
            "--delete-orphans" => {
                parsed.delete_orphans = true;
            }
            "--loop" => {
                parsed.loop_mode = true;
            }
            "--interval-secs" => {
                let Some(value) = pending.next() else {
                    return Err("Missing value for --interval-secs".to_string());
                };
                parsed.interval_secs = parse_interval_secs(&value)?;
            }
            "--mark-missing-documents-failed" => {
                parsed.mark_missing_documents_failed = true;
            }
            "--workspace-id" => {
                let Some(value) = pending.next() else {
                    return Err("Missing value for --workspace-id".to_string());
                };
                parsed.workspace_id = Some(parse_uuid_arg("--workspace-id", &value)?);
            }
            "--tenant-id" => {
                let Some(value) = pending.next() else {
                    return Err("Missing value for --tenant-id".to_string());
                };
                parsed.tenant_id = Some(parse_uuid_arg("--tenant-id", &value)?);
            }
            _ if arg.starts_with("--workspace-id=") => {
                let value = arg.trim_start_matches("--workspace-id=");
                parsed.workspace_id = Some(parse_uuid_arg("--workspace-id", value)?);
            }
            _ if arg.starts_with("--tenant-id=") => {
                let value = arg.trim_start_matches("--tenant-id=");
                parsed.tenant_id = Some(parse_uuid_arg("--tenant-id", value)?);
            }
            _ if arg.starts_with("--interval-secs=") => {
                let value = arg.trim_start_matches("--interval-secs=");
                parsed.interval_secs = parse_interval_secs(value)?;
            }
            _ => {
                return Err(format!("Unknown argument: {arg}"));
            }
        }
    }

    Ok(parsed)
}

fn validate_args(args: &CleanupArgs) -> Result<(), String> {
    if !args.loop_mode {
        return Ok(());
    }

    let mut destructive_flags = Vec::new();
    if args.delete {
        destructive_flags.push("--delete");
    }
    if args.delete_orphans {
        destructive_flags.push("--delete-orphans");
    }
    if args.mark_missing_documents_failed {
        destructive_flags.push("--mark-missing-documents-failed");
    }

    if destructive_flags.is_empty() {
        return Ok(());
    }

    Err(format!(
        "--loop chỉ hỗ trợ dry-run/report; không được dùng cùng {}",
        destructive_flags.join(", ")
    ))
}

fn parse_interval_secs(value: &str) -> Result<u64, String> {
    let interval_secs = value
        .parse::<u64>()
        .map_err(|_| format!("Invalid value for --interval-secs: {value}"))?;

    if interval_secs == 0 {
        return Err("--interval-secs must be greater than zero".to_string());
    }

    Ok(interval_secs)
}

fn resolve_mode(args: &CleanupArgs) -> Result<CleanupMode, String> {
    if let Some(workspace_id) = args.workspace_id {
        return Ok(CleanupMode::WorkspacePrefix {
            workspace_id,
            tenant_id_override: args.tenant_id,
        });
    }

    if let Some(tenant_id) = args.tenant_id {
        return Ok(CleanupMode::TenantPrefix { tenant_id });
    }

    if args.delete && !args.delete_orphans {
        return Err(
            "--delete without --workspace-id/--tenant-id requires --delete-orphans".to_string(),
        );
    }

    Ok(CleanupMode::Scan)
}

fn parse_uuid_arg(flag: &str, value: &str) -> Result<Uuid, String> {
    Uuid::parse_str(value).map_err(|_| format!("Invalid UUID for {flag}: {value}"))
}

fn print_usage() {
    println!(
        "cleanup-storage-objects usage:
  cargo run --bin cleanup-storage-objects -- --dry-run
  cargo run --bin cleanup-storage-objects -- --delete-orphans --delete
  cargo run --bin cleanup-storage-objects -- --workspace-id <workspace_uuid> --delete
  cargo run --bin cleanup-storage-objects -- --tenant-id <tenant_uuid> --delete

options:
  --dry-run
  --delete
  --delete-orphans
  --loop
  --interval-secs <seconds>
  --workspace-id <uuid>
  --tenant-id <uuid>
  --mark-missing-documents-failed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loop_rejects_destructive_flags_before_startup() {
        let args = parse_args(["--loop", "--delete"].into_iter().map(String::from)).unwrap();

        let error = validate_args(&args).unwrap_err();

        assert!(error.contains("--loop"));
        assert!(error.contains("--delete"));
    }

    #[test]
    fn loop_accepts_dry_run_with_interval() {
        let args = parse_args(
            ["--loop", "--dry-run", "--interval-secs=7"]
                .into_iter()
                .map(String::from),
        )
        .unwrap();

        assert!(validate_args(&args).is_ok());
        assert_eq!(args.interval_secs, 7);
    }
}
