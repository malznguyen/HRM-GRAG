use std::path::Path;

use gmrag_api::{
    auth::authz::AuthzClient,
    retrieval::RetrievalClient,
    storage::{StorageClient, StorageConfig},
    tenant_cleanup::{
        OperatorTenantDeleteError, OperatorTenantDeleteResult, TenantDeleteImpact,
        capture_operator_tenant_delete_impact, execute_operator_tenant_delete,
        find_tenants_by_exact_name, run_post_commit_cleanup,
    },
};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Debug)]
enum Target {
    TenantId(Uuid),
    Name(String),
}

#[derive(Debug)]
struct Arguments {
    target: Target,
    delete: bool,
    yes: bool,
    actor: String,
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
        Err(ParseError::Help) => {
            print_usage();
            return;
        }
        Err(ParseError::Message(message)) => exit_with(1, &message),
    };

    let database_url = match std::env::var("DATABASE_URL") {
        Ok(value) => value,
        Err(_) => exit_with(1, "DATABASE_URL must be set."),
    };
    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(_) => exit_with(1, "Could not connect to PostgreSQL."),
    };
    if sqlx::migrate!("./migrations").run(&pool).await.is_err() {
        exit_with(1, "Could not run migrations before tenant deletion.");
    }
    let storage_config = match StorageConfig::from_env() {
        Ok(config) => config,
        Err(_) => exit_with(1, "Could not load S3/MinIO configuration."),
    };
    let authz = match AuthzClient::from_env() {
        Ok(client) => client,
        Err(_) => exit_with(1, "OpenFGA configuration is incomplete."),
    };

    let tenant_id = match resolve_target(&pool, args.target).await {
        Ok(Some(tenant_id)) => tenant_id,
        Ok(None) => {
            println!("Tenant not found. Nothing was modified.");
            return;
        }
        Err(message) => exit_with(1, &message),
    };

    if !args.delete {
        match capture_operator_tenant_delete_impact(
            &pool,
            &authz,
            tenant_id,
            &storage_config.bucket,
        )
        .await
        {
            Ok(Some(impact)) => {
                print_impact_report(&impact);
                println!("DRY RUN — nothing was modified. Re-run with --delete to apply.");
                println!("--delete also requires --yes.");
            }
            Ok(None) => println!("Tenant not found. Nothing was modified."),
            Err(error) => exit_with(1, &format!("Could not inspect tenant: {error}")),
        }
        return;
    }

    if !args.yes {
        exit_with(
            1,
            "--delete requires --yes to confirm this destructive operation.",
        );
    }

    match execute_operator_tenant_delete(
        &pool,
        &authz,
        tenant_id,
        &storage_config.bucket,
        &args.actor,
        Path::new("."),
    )
    .await
    {
        Ok(OperatorTenantDeleteResult::NotFound) => {
            println!("Tenant not found. Nothing was modified.");
        }
        Ok(OperatorTenantDeleteResult::Deleted {
            impact,
            recovery_file,
            qdrant_outbox_id,
            storage_outbox_id,
        }) => {
            print_impact_report(&impact);
            println!("Committed tenant deletion.");
            println!("  recovery_file={}", recovery_file.display());
            println!("  qdrant_outbox_id={qdrant_outbox_id}");
            println!("  storage_outbox_id={storage_outbox_id}");

            let storage = StorageClient::from_config(storage_config).await;
            match RetrievalClient::from_env() {
                Ok(retrieval) => {
                    let cleanup = run_post_commit_cleanup(&impact, &storage, &retrieval).await;
                    println!(
                        "Post-commit cleanup: storage_succeeded={}, qdrant_succeeded={}. Durable outbox rows remain available for retry.",
                        cleanup.storage_succeeded, cleanup.qdrant_succeeded
                    );
                }
                Err(_) => {
                    eprintln!(
                        "Post-commit Qdrant cleanup was not attempted because Qdrant configuration is unavailable. The durable outbox row remains for process-qdrant-outbox."
                    );
                }
            }
        }
        Err(OperatorTenantDeleteError::SqlAfterOpenFga { recovery_file, .. }) => {
            eprintln!("DANGER: OpenFGA tuples were removed, but the SQL delete did not commit.");
            eprintln!("Recovery file: {}", recovery_file.display());
            eprintln!(
                "Re-run the same --delete --yes command to finish deletion, or restore tuples from the recovery file."
            );
            std::process::exit(3);
        }
        Err(error) => exit_with(1, &format!("Tenant deletion failed: {error}")),
    }
}

async fn resolve_target(pool: &sqlx::PgPool, target: Target) -> Result<Option<Uuid>, String> {
    match target {
        Target::TenantId(tenant_id) => Ok(Some(tenant_id)),
        Target::Name(name) => {
            let matches = find_tenants_by_exact_name(pool, &name)
                .await
                .map_err(|_| "Could not resolve tenant name from PostgreSQL.".to_string())?;
            match matches.len() {
                0 => Ok(None),
                1 => Ok(matches.first().map(|tenant| tenant.tenant_id)),
                _ => {
                    eprintln!("More than one tenant matches --name exactly; refusing to guess:");
                    for tenant in matches {
                        eprintln!(
                            "  tenant_id={} name={} created_at={}",
                            tenant.tenant_id, tenant.tenant_name, tenant.created_at
                        );
                    }
                    Err("Use --tenant-id to select exactly one tenant.".to_string())
                }
            }
        }
    }
}

fn print_impact_report(impact: &TenantDeleteImpact) {
    println!("Tenant deletion impact:");
    println!("  tenant_id={}", impact.plan.tenant_id);
    println!("  tenant_name={}", impact.plan.tenant_name);
    println!("  created_at={}", impact.created_at);
    println!("  owner_count={}", impact.owner_emails.len());
    for email in &impact.owner_emails {
        println!("    owner_email={email}");
    }
    println!("  workspace_count={}", impact.workspaces.len());
    for workspace in &impact.workspaces {
        println!(
            "    workspace_id={} name={} document_count={}",
            workspace.id, workspace.name, workspace.document_count
        );
    }
    println!("  document_count={}", impact.document_count);
    println!("  chunk_count={}", impact.chunk_count);
    println!("  graph_node_count={}", impact.graph_node_count);
    println!("  chat_session_count={}", impact.chat_session_count);
    println!("  storage_prefix={}", impact.plan.storage_prefix);
    println!("  storage_bucket={}", impact.plan.storage_bucket);
    println!("  openfga_tuple_count={}", impact.openfga_tuples.len());
    for (relation, count) in impact.openfga_tuples_by_relation() {
        println!("    openfga_relation={relation} count={count}");
    }
}

enum ParseError {
    Help,
    Message(String),
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Arguments, ParseError> {
    let mut tenant_id = None;
    let mut name = None;
    let mut delete = false;
    let mut dry_run = false;
    let mut yes = false;
    let mut actor = "operator-cli".to_string();
    let mut args = args.peekable();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err(ParseError::Help),
            "--tenant-id" => {
                let Some(value) = args.next() else {
                    return Err(ParseError::Message(
                        "--tenant-id requires a UUID.".to_string(),
                    ));
                };
                tenant_id = Some(parse_uuid(&value)?);
            }
            "--name" => {
                let Some(value) = args.next() else {
                    return Err(ParseError::Message("--name requires a value.".to_string()));
                };
                name = Some(parse_name(value)?);
            }
            "--actor" => {
                let Some(value) = args.next() else {
                    return Err(ParseError::Message("--actor requires an id.".to_string()));
                };
                actor = parse_actor(value)?;
            }
            "--dry-run" => dry_run = true,
            "--delete" => delete = true,
            "--yes" => yes = true,
            _ if arg.starts_with("--tenant-id=") => {
                tenant_id = Some(parse_uuid(arg.trim_start_matches("--tenant-id="))?);
            }
            _ if arg.starts_with("--name=") => {
                name = Some(parse_name(arg.trim_start_matches("--name=").to_string())?);
            }
            _ if arg.starts_with("--actor=") => {
                actor = parse_actor(arg.trim_start_matches("--actor=").to_string())?;
            }
            _ => return Err(ParseError::Message(format!("Unknown argument: {arg}"))),
        }
    }

    if tenant_id.is_some() == name.is_some() {
        return Err(ParseError::Message(
            "Provide exactly one of --tenant-id or --name.".to_string(),
        ));
    }
    if delete && dry_run {
        return Err(ParseError::Message(
            "--delete and --dry-run cannot be used together.".to_string(),
        ));
    }
    if yes && !delete {
        return Err(ParseError::Message("--yes requires --delete.".to_string()));
    }

    let target = match (tenant_id, name) {
        (Some(tenant_id), None) => Target::TenantId(tenant_id),
        (None, Some(name)) => Target::Name(name),
        _ => unreachable!(),
    };
    Ok(Arguments {
        target,
        delete,
        yes,
        actor,
    })
}

fn parse_uuid(value: &str) -> Result<Uuid, ParseError> {
    Uuid::parse_str(value)
        .map_err(|_| ParseError::Message(format!("Invalid --tenant-id UUID: {value}")))
}

fn parse_name(value: String) -> Result<String, ParseError> {
    if value.trim().is_empty() {
        return Err(ParseError::Message("--name cannot be empty.".to_string()));
    }
    Ok(value)
}

fn parse_actor(value: String) -> Result<String, ParseError> {
    if value.trim().is_empty() {
        return Err(ParseError::Message("--actor cannot be empty.".to_string()));
    }
    Ok(value)
}

fn print_usage() {
    println!(
        "delete-tenant usage:\n  cargo run --bin delete-tenant -- --tenant-id <uuid> --dry-run\n  cargo run --bin delete-tenant -- --name <exact_name> --dry-run\n  cargo run --bin delete-tenant -- --tenant-id <uuid> --delete --yes [--actor <id>]\n\nSafety:\n  --dry-run is the default and never mutates data.\n  --delete requires --yes.\n  --tenant-id and --name are mutually exclusive.\n  A duplicate exact --name is refused; use --tenant-id instead."
    );
}

fn exit_with(code: i32, message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(code)
}
