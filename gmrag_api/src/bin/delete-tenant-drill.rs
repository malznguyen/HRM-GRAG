//! Operator drill: xoá tenant SQL + enqueue cleanup outbox (LIFE-005).
//!
//! **Không** phải public API. Không scheduler. Không gọi S3/Qdrant trực tiếp.
//!
//! Mặc định dry-run (chỉ capture + in plan). Xoá thật cần `--delete`.
//!
//! `--delete` đã bị vô hiệu hoá: dùng `delete-tenant` để revoke OpenFGA trước SQL.

use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::storage::StorageConfig;
use gmrag_api::tenant_cleanup::{
    TenantCleanupError, TenantDeletePlan, capture_tenant_delete_plan,
    commit_tenant_delete_lifecycle,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Debug, Default)]
struct DrillArgs {
    dry_run: bool,
    delete: bool,
    tenant_id: Option<Uuid>,
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

    let Some(tenant_id) = args.tenant_id else {
        eprintln!("--tenant-id <uuid> is required");
        print_usage();
        std::process::exit(1);
    };

    if args.delete {
        eprintln!(
            "delete-tenant-drill --delete is disabled because it can leave OpenFGA orphan tuples. Use: cargo run --locked --bin delete-tenant -- --tenant-id {tenant_id} --delete --yes"
        );
        std::process::exit(1);
    }

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

    // Bucket chỉ từ runtime config tin cậy — không nhận từ CLI/request.
    let storage_config = StorageConfig::from_env().expect("Failed to load storage config");
    let storage_bucket = storage_config.bucket.clone();

    if args.delete {
        match commit_tenant_delete_lifecycle(&pool, tenant_id, &storage_bucket).await {
            Ok(result) => {
                print_plan(&result.plan, true);
                println!(
                    "Committed: qdrant_outbox_id={}, storage_outbox_id={}",
                    result.qdrant_outbox_id, result.storage_outbox_id
                );
                println!(
                    "Next (manual OPS): process-qdrant-outbox / process-storage-outbox — no unattended scheduler (OPS-003)."
                );

                let _ = insert_audit_event(
                    &pool,
                    AuditEventRecord::new(AuditEventType::TenantDeleteDrillCompleted)
                        .with_tenant_id(tenant_id)
                        .with_target("tenant", tenant_id.to_string())
                        .with_metadata(json!({
                            "tenant_name": result.plan.tenant_name,
                            "workspace_ids": result.plan.workspace_ids,
                            "workspace_count": result.plan.workspace_count(),
                            "workspace_list_empty": result.plan.has_empty_workspace_list(),
                            "storage_prefix": result.plan.storage_prefix,
                            "storage_bucket": result.plan.storage_bucket,
                            "qdrant_outbox_id": result.qdrant_outbox_id,
                            "storage_outbox_id": result.storage_outbox_id,
                            "public_api": false,
                            "workers_unattended": false
                        })),
                )
                .await;
            }
            Err(err) => {
                eprintln!("Tenant delete drill failed: {err}");
                let _ = insert_audit_event(
                    &pool,
                    AuditEventRecord::new(AuditEventType::TenantDeleteDrillFailed)
                        .with_tenant_id(tenant_id)
                        .with_target("tenant", tenant_id.to_string())
                        .with_metadata(json!({
                            "error_code": error_code(&err),
                            "error": sanitize_error_code(&err.to_string()),
                            "public_api": false
                        })),
                )
                .await;
                std::process::exit(exit_code(&err));
            }
        }
    } else {
        match capture_tenant_delete_plan(&pool, tenant_id, &storage_bucket).await {
            Ok(plan) => {
                print_plan(&plan, false);
                println!("Dry-run only — pass --delete to commit SQL cascade + outbox rows.");

                let _ = insert_audit_event(
                    &pool,
                    AuditEventRecord::new(AuditEventType::TenantDeleteDrillDryRun)
                        .with_tenant_id(tenant_id)
                        .with_target("tenant", tenant_id.to_string())
                        .with_metadata(json!({
                            "tenant_name": plan.tenant_name,
                            "workspace_ids": plan.workspace_ids,
                            "workspace_count": plan.workspace_count(),
                            "workspace_list_empty": plan.has_empty_workspace_list(),
                            "storage_prefix": plan.storage_prefix,
                            "storage_bucket": plan.storage_bucket,
                            "public_api": false,
                            "deleted": false
                        })),
                )
                .await;
            }
            Err(err) => {
                eprintln!("Tenant delete drill dry-run failed: {err}");
                let _ = insert_audit_event(
                    &pool,
                    AuditEventRecord::new(AuditEventType::TenantDeleteDrillFailed)
                        .with_tenant_id(tenant_id)
                        .with_target("tenant", tenant_id.to_string())
                        .with_metadata(json!({
                            "error_code": error_code(&err),
                            "error": sanitize_error_code(&err.to_string()),
                            "public_api": false,
                            "deleted": false
                        })),
                )
                .await;
                std::process::exit(exit_code(&err));
            }
        }
    }
}

fn print_plan(plan: &TenantDeletePlan, deleted: bool) {
    println!("Tenant delete plan (LIFE-005 operator drill — no public route):");
    println!("  tenant_id={}", plan.tenant_id);
    println!("  tenant_name={}", plan.tenant_name);
    println!("  workspace_count={}", plan.workspace_count());
    println!(
        "  workspace_list_empty={} (explicit; empty is valid for empty tenant)",
        plan.has_empty_workspace_list()
    );
    println!("  workspace_ids={:?}", plan.workspace_ids);
    println!("  storage_prefix={}", plan.storage_prefix);
    println!(
        "  storage_bucket={} (from runtime config only)",
        plan.storage_bucket
    );
    println!("  deleted={deleted}");
}

fn error_code(err: &TenantCleanupError) -> &'static str {
    match err {
        TenantCleanupError::Database(_) => "DATABASE",
        TenantCleanupError::TenantNotFound { .. } => "TENANT_NOT_FOUND",
        TenantCleanupError::EmptyBucket => "EMPTY_BUCKET",
    }
}

fn exit_code(err: &TenantCleanupError) -> i32 {
    match err {
        TenantCleanupError::TenantNotFound { .. } => 1,
        TenantCleanupError::EmptyBucket => 1,
        TenantCleanupError::Database(_) => 2,
    }
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<DrillArgs, String> {
    let mut result = DrillArgs {
        dry_run: true,
        delete: false,
        tenant_id: None,
    };

    let mut iter = args.peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" | "--help" => return Err("help".to_string()),
            "--dry-run" => {
                result.dry_run = true;
                result.delete = false;
            }
            "--delete" => {
                result.delete = true;
                result.dry_run = false;
            }
            "--tenant-id" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--tenant-id requires a UUID value".to_string())?;
                result.tenant_id = Some(
                    Uuid::parse_str(&value)
                        .map_err(|_| format!("invalid --tenant-id UUID: {value}"))?,
                );
            }
            other if other.starts_with("--tenant-id=") => {
                let value = other.trim_start_matches("--tenant-id=");
                result.tenant_id = Some(
                    Uuid::parse_str(value)
                        .map_err(|_| format!("invalid --tenant-id UUID: {value}"))?,
                );
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if result.delete && result.dry_run {
        // --delete wins when both set; dry_run default is flipped above.
    }

    let _ = result.dry_run;
    Ok(result)
}

fn print_usage() {
    eprintln!(
        "\
Usage:
  cargo run --bin delete-tenant-drill -- --tenant-id <uuid>           # dry-run (default)
  cargo run --bin delete-tenant-drill -- --tenant-id <uuid> --delete  # refused; use delete-tenant

Notes:
  - No public DELETE /tenants route. Operator/library drill only.
  - Bucket comes from S3_BUCKET runtime config (never CLI).
  - Captures workspace IDs before SQL cascade; enqueues qdrant delete_by_workspaces
    and storage delete_prefix (tenants/{{tenant_id}}/) in the same transaction.
  - Empty workspace list is explicit (valid for empty tenant); missing tenant is an error.
  - This drill never performs SQL deletion. Use delete-tenant for the FGA-first lifecycle."
    );
}
