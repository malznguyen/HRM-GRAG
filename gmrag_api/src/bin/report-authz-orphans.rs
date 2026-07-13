use gmrag_api::{
    auth::authz::AuthzClient,
    authz_orphan_report::{AuthzOrphanReport, run_authz_orphan_report},
};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        return;
    }
    if args.iter().any(|arg| arg != "--dry-run") {
        eprintln!("report-authz-orphans is read-only and accepts only --dry-run.");
        print_usage();
        std::process::exit(1);
    }

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
    let authz = match AuthzClient::from_env() {
        Ok(client) => client,
        Err(_) => exit_with("OpenFGA configuration is incomplete."),
    };

    match run_authz_orphan_report(&pool, &authz).await {
        Ok(report) => print_report(&report),
        Err(error) => exit_with(&format!("Could not generate authz orphan report: {error}")),
    }
}

fn print_report(report: &AuthzOrphanReport) {
    println!("=== report-authz-orphans (DRY RUN) ===");
    println!("orphan_tuples={}", report.summary.orphan_tuples);
    println!(
        "orphan_tuples_with_audit={}",
        report.summary.orphan_tuples_with_audit
    );
    println!(
        "orphan_tuples_without_audit={}",
        report.summary.orphan_tuples_without_audit
    );
    for (resource_type, count) in &report.summary.by_resource_type {
        println!("resource_type={resource_type} count={count}");
    }

    let mut current_type = None;
    for finding in &report.findings {
        if current_type.as_deref() != Some(finding.resource_type.as_str()) {
            current_type = Some(finding.resource_type.clone());
            println!("--- {} ---", finding.resource_type);
        }
        println!(
            "resource_id={} relation={} user={} audit_matches={}",
            finding.resource_id,
            finding.tuple.relation,
            finding.tuple.user,
            finding.audit_matches.len()
        );
        for audit in &finding.audit_matches {
            println!(
                "  audit event_type={} created_at={} target_type={} target_id={}",
                audit.event_type,
                audit.created_at,
                audit.target_type.as_deref().unwrap_or("-"),
                audit.target_id.as_deref().unwrap_or("-")
            );
        }
        if finding.audit_matches.is_empty() {
            println!("  audit no_matching_audit");
        }
    }
    for limitation in &report.limitations {
        println!("limitation={limitation}");
    }
    println!("DRY RUN — no tuple, SQL row, audit row, or outbox row was modified.");
}

fn print_usage() {
    println!(
        "report-authz-orphans usage:\n  cargo run --bin report-authz-orphans -- --dry-run\n\nRead-only tuple-to-SQL orphan triage with audit_events correlation."
    );
}

fn exit_with(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1)
}
