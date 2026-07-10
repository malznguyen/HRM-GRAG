//! Operator command: dọn placeholder invite legacy (`invite_*`).
//!
//! Mặc định dry-run. Xoá thật chỉ khi có `--delete`.

use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::auth::authz::AuthzClient;
use gmrag_api::invite_cleanup::{
    InvitePlaceholderCleanupOptions, InvitePlaceholderCleanupReport, cleanup_invite_placeholders,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

#[derive(Debug, Default)]
struct CleanupArgs {
    dry_run: bool,
    delete: bool,
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

    let authz_client = AuthzClient::from_env().expect("Failed to initialize AuthzClient");

    let options = InvitePlaceholderCleanupOptions {
        allow_delete: args.delete,
    };

    match cleanup_invite_placeholders(&pool, &authz_client, options).await {
        Ok(report) => {
            print_report(&report, args.delete);

            let event_type = if args.delete {
                AuditEventType::InvitePlaceholderCleanupCompleted
            } else {
                AuditEventType::InvitePlaceholderCleanupDryRun
            };

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(event_type).with_metadata(report_metadata(&report)),
            )
            .await;

            if !report.errors.is_empty() {
                std::process::exit(2);
            }
        }
        Err(err) => {
            eprintln!("Invite placeholder cleanup failed: {err}");

            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::InvitePlaceholderCleanupFailed)
                    .with_metadata(json!({
                        "delete": args.delete,
                        "error_code": sanitize_error_code(&err.to_string())
                    })),
            )
            .await;

            std::process::exit(1);
        }
    }
}

fn print_report(report: &InvitePlaceholderCleanupReport, delete_enabled: bool) {
    let mode = if delete_enabled { "DELETE" } else { "DRY-RUN" };

    println!("=== cleanup-invite-placeholders ({mode}) ===");
    println!("placeholders_found={}", report.placeholders_found);
    println!("openfga_tuples_found={}", report.openfga_tuples_found);

    for snapshot in report.placeholders.iter().take(50) {
        // Không log email đầy đủ nếu không cần — chỉ id + counts (ops summary)
        println!(
            "  user_id={} email={} workspace_members={} tenant_members={} document_shares={} chat_sessions={} openfga_tuples={}",
            snapshot.user_id,
            snapshot.email,
            snapshot.workspace_member_count,
            snapshot.tenant_member_count,
            snapshot.document_share_count,
            snapshot.chat_session_count,
            snapshot.openfga_tuples.len()
        );
    }

    if report.placeholders.len() > 50 {
        println!(
            "  ... and {} more placeholders",
            report.placeholders.len() - 50
        );
    }

    if delete_enabled {
        println!("--- deletion summary ---");
        println!("openfga_tuples_deleted={}", report.openfga_tuples_deleted);
        println!("document_shares_deleted={}", report.document_shares_deleted);
        println!(
            "workspace_members_deleted={}",
            report.workspace_members_deleted
        );
        println!("tenant_members_deleted={}", report.tenant_members_deleted);
        println!("chat_sessions_deleted={}", report.chat_sessions_deleted);
        println!("users_deleted={}", report.users_deleted);
    } else {
        println!("Dry-run mode: no OpenFGA tuples or SQL rows were deleted.");
        println!("Re-run with --delete to perform cleanup.");
    }

    if !report.errors.is_empty() {
        println!("--- errors ({}) ---", report.errors.len());
        for error in &report.errors {
            println!("  {error}");
        }
    }

    println!(
        "Summary: found={}, deleted_users={}, errors={}",
        report.placeholders_found,
        if delete_enabled {
            report.users_deleted
        } else {
            0
        },
        report.errors.len()
    );
}

fn report_metadata(report: &InvitePlaceholderCleanupReport) -> serde_json::Value {
    // Metadata audit: counts only — không nhét email / secret
    json!({
        "delete": report.deleted,
        "placeholders_found": report.placeholders_found,
        "openfga_tuples_found": report.openfga_tuples_found,
        "openfga_tuples_deleted": report.openfga_tuples_deleted,
        "document_shares_deleted": report.document_shares_deleted,
        "workspace_members_deleted": report.workspace_members_deleted,
        "tenant_members_deleted": report.tenant_members_deleted,
        "chat_sessions_deleted": report.chat_sessions_deleted,
        "users_deleted": report.users_deleted,
        "error_count": report.errors.len()
    })
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<CleanupArgs, String> {
    // Mặc định dry-run an toàn: không xoá nếu thiếu --delete
    let mut parsed = CleanupArgs {
        dry_run: true,
        delete: false,
    };

    for arg in args {
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
            _ => return Err(format!("Unknown argument: {arg}")),
        }
    }

    let _ = parsed.dry_run;
    Ok(parsed)
}

fn print_usage() {
    println!(
        "cleanup-invite-placeholders usage:
  cargo run --bin cleanup-invite-placeholders -- --dry-run
  cargo run --bin cleanup-invite-placeholders -- --delete

Removes legacy invite_* placeholder users left after the invite flow was removed.

options:
  --dry-run   Report only (default). Does not delete anything.
  --delete    Actually delete OpenFGA tuples then SQL rows.
  --help, -h  Show this help.

Safety:
  - Default is dry-run; --delete is required for mutations.
  - Idempotent: safe to re-run.
  - Delete order: OpenFGA tuples → document_shares → workspace/tenant_members → users."
    );
}
