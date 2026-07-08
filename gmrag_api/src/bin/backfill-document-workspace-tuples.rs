use gmrag_api::auth::authz::AuthzClient;
use gmrag_api::auth::document_acl::backfill_document_workspace_relations;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    let authz_client = AuthzClient::from_env().expect("Failed to initialize AuthzClient");
    let result = backfill_document_workspace_relations(&pool, &authz_client)
        .await
        .expect("Backfill failed");

    println!(
        "Backfill complete: total_documents={}, inserted_relations={}, existing_relations={}",
        result.total_documents,
        result.inserted_relations,
        result.existing_relations
    );
}
