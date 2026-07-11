use gmrag_api::ingestion::jobs::recover_legacy_processing_documents;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    if matches!(std::env::args().nth(1).as_deref(), Some("--help" | "-h")) {
        println!(
            "Usage: recover-stale-ingestion-jobs [--dry-run|--apply]\n\nInspects legacy PROCESSING documents without an active durable ingestion job."
        );
        return;
    }
    dotenvy::dotenv().ok();
    let apply = matches!(std::env::args().nth(1).as_deref(), Some("--apply"));
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"))
        .await
        .expect("Failed to connect to database");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM documents document WHERE status = 'PROCESSING' AND NOT EXISTS (SELECT 1 FROM ingestion_jobs job WHERE job.document_id = document.id AND job.status IN ('QUEUED', 'PROCESSING'))",
    )
    .fetch_one(&pool)
    .await
    .expect("Failed to inspect legacy documents");
    if !apply {
        println!("dry-run: {count} legacy PROCESSING document(s) have no active ingestion job");
        return;
    }
    let recovered = recover_legacy_processing_documents(&pool)
        .await
        .expect("Failed to recover legacy documents");
    println!("recovered: {recovered} legacy PROCESSING document(s) marked FAILED");
}
