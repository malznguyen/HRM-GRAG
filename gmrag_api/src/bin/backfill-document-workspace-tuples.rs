use gmrag_api::audit::{AuditEventRecord, AuditEventType, insert_audit_event, sanitize_error_code};
use gmrag_api::auth::authz::AuthzClient;
use gmrag_api::auth::document_acl::backfill_document_workspace_relations;
use serde_json::json;
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
    let _ = insert_audit_event(
        &pool,
        AuditEventRecord::new(AuditEventType::BackfillDocumentWorkspaceTuplesStarted),
    )
    .await;

    let result = match backfill_document_workspace_relations(&pool, &authz_client).await {
        Ok(result) => result,
        Err(err) => {
            let _ = insert_audit_event(
                &pool,
                AuditEventRecord::new(AuditEventType::BackfillDocumentWorkspaceTuplesFailed)
                    .with_metadata(json!({
                        "source": "backfill_document_workspace_tuples",
                        "error_code": sanitize_error_code(&err.to_string())
                    })),
            )
            .await;
            panic!("Backfill failed: {err}");
        }
    };

    let _ = insert_audit_event(
        &pool,
        AuditEventRecord::new(AuditEventType::BackfillDocumentWorkspaceTuplesCompleted)
            .with_metadata(json!({
                "total_documents": result.total_documents,
                "inserted_relations": result.inserted_relations,
                "existing_relations": result.existing_relations
            })),
    )
    .await;

    println!(
        "Backfill complete: total_documents={}, inserted_relations={}, existing_relations={}",
        result.total_documents, result.inserted_relations, result.existing_relations
    );
}
