//! Operator command: backfill `graph_nodes.embedding` cho node legacy (`embedding IS NULL`).
//!
//! Mặc định dry-run. Ghi thật chỉ khi có `--apply`.

use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::ingestion::backfill_node_embeddings::{
    BackfillGraphNodeEmbeddingsOptions, BackfillGraphNodeEmbeddingsReport,
    backfill_graph_node_embeddings,
};
use gmrag_api::ingestion::embedding::log_embedding_config_on_startup;
use reqwest::Client;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Debug)]
struct BackfillArgs {
    apply: bool,
    workspace_id: Option<Uuid>,
    batch_size: usize,
}

impl Default for BackfillArgs {
    fn default() -> Self {
        Self {
            apply: false,
            workspace_id: None,
            batch_size: 50,
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

    log_embedding_config_on_startup();

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

    let client = Client::new();
    let options = BackfillGraphNodeEmbeddingsOptions {
        allow_apply: args.apply,
        workspace_id: args.workspace_id,
        batch_size: args.batch_size,
    };

    match backfill_graph_node_embeddings(&pool, &client, options).await {
        Ok(report) => {
            print_report(&report);

            // Audit chỉ khi apply thật — dry-run không ghi event (metadata counts only).
            if args.apply {
                let mut event =
                    AuditEventRecord::new(AuditEventType::GraphNodeEmbeddingBackfillCompleted)
                        .with_metadata(report_metadata(&report));
                if let Some(workspace_id) = args.workspace_id {
                    event = event.with_workspace_id(workspace_id);
                }
                let _ = insert_audit_event(&pool, event).await;
            }

            if !report.error_samples.is_empty() || report.error_count > 0 {
                std::process::exit(2);
            }
        }
        Err(err) => {
            eprintln!("Graph node embedding backfill failed: {err}");

            if args.apply {
                let mut event =
                    AuditEventRecord::new(AuditEventType::GraphNodeEmbeddingBackfillFailed)
                        .with_metadata(json!({
                            "apply": true,
                            "workspace_id": args.workspace_id,
                            "batch_size": args.batch_size,
                            "error_code": sanitize_error_code(&err.to_string())
                        }));
                if let Some(workspace_id) = args.workspace_id {
                    event = event.with_workspace_id(workspace_id);
                }
                let _ = insert_audit_event(&pool, event).await;
            }

            std::process::exit(1);
        }
    }
}

fn print_report(report: &BackfillGraphNodeEmbeddingsReport) {
    let mode = if report.applied { "APPLY" } else { "DRY-RUN" };

    println!("=== backfill-graph-node-embeddings ({mode}) ===");
    println!("batch_size={}", report.batch_size);
    if let Some(workspace_id) = report.workspace_filter {
        println!("workspace_id={workspace_id}");
    } else {
        println!("workspace_id=<all>");
    }
    println!("nodes_found={}", report.nodes_found);

    if !report.counts_by_workspace.is_empty() {
        println!("--- counts by workspace ---");
        for row in &report.counts_by_workspace {
            println!(
                "  workspace_id={} null_embeddings={}",
                row.workspace_id, row.null_count
            );
        }
    }

    if report.applied {
        println!("nodes_updated={}", report.nodes_updated);
        println!(
            "nodes_skipped_already_embedded={}",
            report.nodes_skipped_already_embedded
        );
        println!("error_count={}", report.error_count);
        if !report.error_samples.is_empty() {
            println!("--- error samples ---");
            for sample in &report.error_samples {
                println!("  {sample}");
            }
        }
    } else {
        println!("Dry-run mode: no embeddings were written.");
        println!("Re-run with --apply to write embeddings for NULL nodes.");
    }

    println!(
        "Summary: found={}, updated={}, errors={}",
        report.nodes_found,
        if report.applied {
            report.nodes_updated
        } else {
            0
        },
        report.error_count
    );
}

fn report_metadata(report: &BackfillGraphNodeEmbeddingsReport) -> serde_json::Value {
    // Metadata-only: counts / filter — không chứa entity_name hay description.
    json!({
        "apply": report.applied,
        "workspace_id": report.workspace_filter,
        "batch_size": report.batch_size,
        "nodes_found": report.nodes_found,
        "nodes_updated": report.nodes_updated,
        "nodes_skipped_already_embedded": report.nodes_skipped_already_embedded,
        "error_count": report.error_count,
        "workspace_count": report.counts_by_workspace.len(),
        "counts_by_workspace": report.counts_by_workspace.iter().map(|row| {
            json!({
                "workspace_id": row.workspace_id,
                "null_count": row.null_count
            })
        }).collect::<Vec<_>>()
    })
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<BackfillArgs, String> {
    let mut parsed = BackfillArgs::default();

    // Env override trước CLI (CLI thắng nếu truyền --batch-size).
    if let Ok(value) = std::env::var("GMRAG_GRAPH_NODE_EMBEDDING_BACKFILL_BATCH_SIZE") {
        parsed.batch_size = value
            .parse::<usize>()
            .map_err(|_| {
                format!(
                    "Invalid GMRAG_GRAPH_NODE_EMBEDDING_BACKFILL_BATCH_SIZE: {value}"
                )
            })?
            .max(1);
    }

    let mut pending = args.peekable();

    while let Some(arg) = pending.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err("help".to_string()),
            "--dry-run" => {
                parsed.apply = false;
            }
            "--apply" => {
                parsed.apply = true;
            }
            "--workspace-id" => {
                let Some(value) = pending.next() else {
                    return Err("Missing value for --workspace-id".to_string());
                };
                parsed.workspace_id = Some(parse_uuid_arg("--workspace-id", &value)?);
            }
            "--batch-size" => {
                let Some(value) = pending.next() else {
                    return Err("Missing value for --batch-size".to_string());
                };
                parsed.batch_size = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid --batch-size: {value}"))?
                    .max(1);
            }
            _ if arg.starts_with("--workspace-id=") => {
                let value = arg.trim_start_matches("--workspace-id=");
                parsed.workspace_id = Some(parse_uuid_arg("--workspace-id", value)?);
            }
            _ if arg.starts_with("--batch-size=") => {
                let value = arg.trim_start_matches("--batch-size=");
                parsed.batch_size = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid --batch-size: {value}"))?
                    .max(1);
            }
            _ => return Err(format!("Unknown argument: {arg}")),
        }
    }

    Ok(parsed)
}

fn parse_uuid_arg(flag: &str, value: &str) -> Result<Uuid, String> {
    Uuid::parse_str(value).map_err(|_| format!("Invalid UUID for {flag}: {value}"))
}

fn print_usage() {
    println!(
        "backfill-graph-node-embeddings usage:
  cargo run --bin backfill-graph-node-embeddings -- --dry-run
  cargo run --bin backfill-graph-node-embeddings -- --apply
  cargo run --bin backfill-graph-node-embeddings -- --apply --workspace-id <uuid>
  cargo run --bin backfill-graph-node-embeddings -- --apply --batch-size 50

Backfills graph_nodes.embedding for legacy rows where embedding IS NULL.
Uses the same ADR-21 Ollama model and node_text_for_embedding() text format
as the ingestion forward-path. Does not re-ingest documents.

options:
  --dry-run              Report only (default). Does not call Ollama or write.
  --apply                Embed NULL nodes and UPDATE graph_nodes.embedding.
  --workspace-id <uuid>  Limit to one workspace (default: all workspaces).
  --batch-size <n>       Nodes per embed batch (default: 50, or env
                         GMRAG_GRAPH_NODE_EMBEDDING_BACKFILL_BATCH_SIZE).
  --help, -h             Show this help.

Safety:
  - Default is dry-run; --apply is required for mutations.
  - Idempotent: only SELECT/UPDATE rows with embedding IS NULL.
  - Safe to re-run; already-embedded nodes are not overwritten.
  - Per-node embed errors are logged and skipped; the command continues.
  - Does not change retrieval SQL, HNSW index, or embedding model config."
    );
}
