//! OCR-004: controlled-fixture tests — audit classification, dry-run no mutation,
//! bounded selection, idempotent requeue, refuse apply when OCR unavailable.
//! Không dùng production corpus; skip nếu DATABASE_URL không sẵn.

mod support;

use gmrag_api::ingestion::jobs::{
    JOB_QUEUED, RequeueEligibility, enqueue_job_tx, requeue_document_for_reingest,
};
use gmrag_api::ingestion::ocr::{MOCK_OCR_MARKER, OcrCapability, production_ocr_capability};
use gmrag_api::ingestion::ocr_audit::{
    EVIDENCE_MOCK_OCR_CHUNK, EVIDENCE_NEEDS_OCR_TERMINAL, ObjectPresenceCheck, OcrAuditOptions,
    OcrAuditPersistKind, classify_evidence, format_human_report, merge_evidence_maps,
    report_to_audit_metadata, run_ocr_affected_audit, select_bounded_ids,
    should_persist_audit_event,
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::OnceLock;
use tokio::sync::Mutex;
use uuid::Uuid;

static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn test_lock() -> &'static Mutex<()> {
    TEST_LOCK.get_or_init(|| Mutex::new(()))
}

async fn pool_or_skip() -> Option<PgPool> {
    dotenvy::dotenv().ok();
    let database_url = support::database_url().ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .ok()?;
    sqlx::migrate!("./migrations").run(&pool).await.ok()?;
    Some(pool)
}

struct Fixture {
    tenant_id: Uuid,
    workspace_id: Uuid,
    user_id: String,
    mock_doc_id: Uuid,
    needs_ocr_doc_id: Uuid,
    clean_doc_id: Uuid,
}

async fn seed_fixture(pool: &PgPool) -> Fixture {
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let user_id = format!("ocr004-test-{}", Uuid::new_v4());
    let mock_doc_id = Uuid::new_v4();
    let needs_ocr_doc_id = Uuid::new_v4();
    let clean_doc_id = Uuid::new_v4();

    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&user_id)
        .bind(format!("{user_id}@test.local"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("ocr004-tenant-{tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind(format!("ocr004-ws-{workspace_id}"))
        .execute(pool)
        .await
        .unwrap();

    // COMPLETED + chunk chứa mock marker (evidence historical contamination).
    insert_document(
        pool,
        mock_doc_id,
        workspace_id,
        &user_id,
        "COMPLETED",
        "DONE",
        None,
        &format!("ocr004/mock/{mock_doc_id}.pdf"),
    )
    .await;
    sqlx::query(
        "INSERT INTO document_chunks (document_id, workspace_id, chunk_index, original_text) VALUES ($1, $2, 0, $3)",
    )
    .bind(mock_doc_id)
    .bind(workspace_id)
    .bind(format!("prefix {MOCK_OCR_MARKER} suffix"))
    .execute(pool)
    .await
    .unwrap();

    // Terminal NEEDS_OCR.
    insert_document(
        pool,
        needs_ocr_doc_id,
        workspace_id,
        &user_id,
        "FAILED",
        "FAILED",
        Some("NEEDS_OCR"),
        &format!("ocr004/needs-ocr/{needs_ocr_doc_id}.pdf"),
    )
    .await;

    // Clean COMPLETED — không phải candidate.
    insert_document(
        pool,
        clean_doc_id,
        workspace_id,
        &user_id,
        "COMPLETED",
        "DONE",
        None,
        &format!("ocr004/clean/{clean_doc_id}.pdf"),
    )
    .await;
    sqlx::query(
        "INSERT INTO document_chunks (document_id, workspace_id, chunk_index, original_text) VALUES ($1, $2, 0, $3)",
    )
    .bind(clean_doc_id)
    .bind(workspace_id)
    .bind("clean native text only")
    .execute(pool)
    .await
    .unwrap();

    Fixture {
        tenant_id,
        workspace_id,
        user_id,
        mock_doc_id,
        needs_ocr_doc_id,
        clean_doc_id,
    }
}

async fn insert_document(
    pool: &PgPool,
    document_id: Uuid,
    workspace_id: Uuid,
    user_id: &str,
    status: &str,
    stage: &str,
    failure_code: Option<&str>,
    object_key: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO documents (
            id, workspace_id, owner_id, filename, status, processing_stage,
            failure_code, object_key, bucket, uploaded_by
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'test', $3)
        "#,
    )
    .bind(document_id)
    .bind(workspace_id)
    .bind(user_id)
    .bind(format!("ocr004-{document_id}.pdf"))
    .bind(status)
    .bind(stage)
    .bind(failure_code)
    .bind(object_key)
    .execute(pool)
    .await
    .unwrap();
}

async fn cleanup(pool: &PgPool, fixture: &Fixture) {
    let _ = sqlx::query("DELETE FROM workspaces WHERE id = $1")
        .bind(fixture.workspace_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(fixture.tenant_id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&fixture.user_id)
        .execute(pool)
        .await;
}

fn open_capability_for_test() -> OcrCapability {
    OcrCapability {
        available: true,
        provider: Some("test-stub"),
        detail: "Controlled fixture capability for OCR-004 tests only",
    }
}

#[test]
fn unit_classify_evidence_and_merge() {
    assert_eq!(
        classify_evidence(true, true),
        vec![EVIDENCE_MOCK_OCR_CHUNK, EVIDENCE_NEEDS_OCR_TERMINAL]
    );
    let a = Uuid::nil();
    let map = merge_evidence_maps([a], [a]);
    assert_eq!(map[&a].len(), 2);
}

#[test]
fn unit_bounded_selection_stable() {
    let t = chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let id1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let id2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let out = select_bounded_ids(vec![(Some(t), id2), (Some(t), id1)], 1);
    assert_eq!(out, vec![id1]);
}

#[test]
fn unit_production_capability_closed() {
    let cap = production_ocr_capability();
    assert!(!cap.available);
}

#[test]
fn unit_cli_audit_persist_policy_dry_run_vs_apply() {
    // Mirror CLI: dry-run report → no audit_events; apply refused/completed → persist.
    let dry = gmrag_api::ingestion::ocr_audit::OcrAuditReport {
        mode: "dry_run".into(),
        dry_run: true,
        apply_requested: false,
        apply_executed: false,
        apply_refused: false,
        refusal_reason: None,
        ocr_provider_available: false,
        ocr_provider: None,
        ocr_capability_detail: "closed".into(),
        limit: 50,
        tenant_id: None,
        workspace_id: None,
        counts: Default::default(),
        samples: vec![],
        limitations: vec![],
        errors: vec![],
    };
    assert_eq!(should_persist_audit_event(&dry), None);

    let refused = gmrag_api::ingestion::ocr_audit::OcrAuditReport {
        mode: "apply_refused".into(),
        dry_run: false,
        apply_requested: true,
        apply_executed: false,
        apply_refused: true,
        refusal_reason: Some("OCR unavailable".into()),
        ocr_provider_available: false,
        ocr_provider: None,
        ocr_capability_detail: "closed".into(),
        limit: 50,
        tenant_id: None,
        workspace_id: None,
        counts: Default::default(),
        samples: vec![],
        limitations: vec![],
        errors: vec![],
    };
    assert_eq!(
        should_persist_audit_event(&refused),
        Some(OcrAuditPersistKind::ApplyRefused)
    );
}

#[tokio::test]
async fn dry_run_classifies_fixtures_without_mutation() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip ocr004 dry_run: DATABASE_URL unavailable");
        return;
    };
    let fixture = seed_fixture(&pool).await;

    let options = OcrAuditOptions {
        dry_run: true,
        apply: false,
        limit: 50,
        sample_limit: 20,
        tenant_id: Some(fixture.tenant_id),
        workspace_id: Some(fixture.workspace_id),
        max_attempts: 5,
    };

    let before_jobs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM ingestion_jobs j JOIN documents d ON d.id = j.document_id WHERE d.workspace_id = $1",
    )
    .bind(fixture.workspace_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    let report = run_ocr_affected_audit(
        &pool,
        &options,
        &production_ocr_capability(),
        ObjectPresenceCheck::Skip,
    )
    .await
    .expect("dry-run audit");

    assert_eq!(report.mode, "dry_run");
    assert!(report.dry_run);
    assert!(!report.apply_executed);
    assert!(!report.apply_refused);
    // CLI path: dry-run must not request audit_events INSERT.
    assert_eq!(should_persist_audit_event(&report), None);
    assert_eq!(report.counts.union_candidates, 2);
    assert_eq!(report.counts.selected_for_plan, 2);
    assert_eq!(report.counts.requeued, 0);
    assert!(report.counts.mock_ocr_chunk >= 1);
    assert!(report.counts.needs_ocr_terminal >= 1);

    let ids: Vec<Uuid> = report.samples.iter().map(|s| s.document_id).collect();
    assert!(ids.contains(&fixture.mock_doc_id));
    assert!(ids.contains(&fixture.needs_ocr_doc_id));
    assert!(!ids.contains(&fixture.clean_doc_id));

    let mock_sample = report
        .samples
        .iter()
        .find(|s| s.document_id == fixture.mock_doc_id)
        .unwrap();
    assert!(
        mock_sample
            .evidence_categories
            .iter()
            .any(|c| c == EVIDENCE_MOCK_OCR_CHUNK)
    );
    assert_eq!(mock_sample.plan_action, "plan_requeue");

    let needs_sample = report
        .samples
        .iter()
        .find(|s| s.document_id == fixture.needs_ocr_doc_id)
        .unwrap();
    assert!(
        needs_sample
            .evidence_categories
            .iter()
            .any(|c| c == EVIDENCE_NEEDS_OCR_TERMINAL)
    );

    // Không leak nội dung chunk/document. Tên marker có thể xuất hiện trong
    // limitations (mô tả heuristic), không phải body text của fixture.
    let json = serde_json::to_string(&report).unwrap();
    assert!(!json.contains("original_text"));
    assert!(!json.contains("clean native text"));
    assert!(!json.contains("prefix "));
    assert!(!json.contains(" suffix"));
    for sample in &report.samples {
        let sample_json = serde_json::to_string(sample).unwrap();
        assert!(
            !sample_json.contains(MOCK_OCR_MARKER),
            "sample rows must not embed chunk text/marker body"
        );
    }
    let human = format_human_report(&report);
    assert!(!human.contains("clean native text"));
    assert!(!human.contains("prefix "));
    let meta = report_to_audit_metadata(&report).to_string();
    assert!(!meta.contains("original_text"));
    assert!(!meta.contains("clean native text"));

    let after_jobs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM ingestion_jobs j JOIN documents d ON d.id = j.document_id WHERE d.workspace_id = $1",
    )
    .bind(fixture.workspace_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(before_jobs, after_jobs, "dry-run must not create jobs");

    let mock_status: String = sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
        .bind(fixture.mock_doc_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(mock_status, "COMPLETED");

    cleanup(&pool, &fixture).await;
}

#[tokio::test]
async fn apply_refused_when_ocr_unavailable() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip ocr004 refuse: DATABASE_URL unavailable");
        return;
    };
    let fixture = seed_fixture(&pool).await;

    let options = OcrAuditOptions {
        dry_run: false,
        apply: true,
        limit: 50,
        sample_limit: 10,
        tenant_id: Some(fixture.tenant_id),
        workspace_id: Some(fixture.workspace_id),
        max_attempts: 5,
    };

    let report = run_ocr_affected_audit(
        &pool,
        &options,
        &production_ocr_capability(),
        ObjectPresenceCheck::AlwaysPresent,
    )
    .await
    .expect("apply refused path");

    assert!(report.apply_refused);
    assert!(!report.apply_executed);
    assert_eq!(report.counts.requeued, 0);
    // CLI may write metadata-only audit for refused apply (not for dry-run).
    assert_eq!(
        should_persist_audit_event(&report),
        Some(OcrAuditPersistKind::ApplyRefused)
    );
    assert!(
        report
            .refusal_reason
            .as_deref()
            .unwrap_or("")
            .contains("unavailable")
    );

    let jobs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM ingestion_jobs j JOIN documents d ON d.id = j.document_id WHERE d.workspace_id = $1",
    )
    .bind(fixture.workspace_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(jobs, 0);

    let needs_status: (String, Option<String>) =
        sqlx::query_as("SELECT status, failure_code FROM documents WHERE id = $1")
            .bind(fixture.needs_ocr_doc_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(needs_status.0, "FAILED");
    assert_eq!(needs_status.1.as_deref(), Some("NEEDS_OCR"));

    cleanup(&pool, &fixture).await;
}

#[tokio::test]
async fn apply_with_open_capability_requeues_bounded_and_idempotent() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip ocr004 apply: DATABASE_URL unavailable");
        return;
    };
    let fixture = seed_fixture(&pool).await;

    let options = OcrAuditOptions {
        dry_run: false,
        apply: true,
        limit: 1,
        sample_limit: 5,
        tenant_id: Some(fixture.tenant_id),
        workspace_id: Some(fixture.workspace_id),
        max_attempts: 5,
    };

    let first = run_ocr_affected_audit(
        &pool,
        &options,
        &open_capability_for_test(),
        ObjectPresenceCheck::AlwaysPresent,
    )
    .await
    .expect("first apply");

    assert!(first.apply_executed);
    assert!(!first.apply_refused);
    assert_eq!(first.counts.selected_for_plan, 1);
    assert_eq!(first.counts.requeued, 1);

    let jobs_after_first: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM ingestion_jobs j JOIN documents d ON d.id = j.document_id WHERE d.workspace_id = $1 AND j.status = $2",
    )
    .bind(fixture.workspace_id)
    .bind(JOB_QUEUED)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(jobs_after_first, 1);

    // Idempotent: active job → no second requeue for same doc; limit=1 still only one candidate slot.
    let second = run_ocr_affected_audit(
        &pool,
        &options,
        &open_capability_for_test(),
        ObjectPresenceCheck::AlwaysPresent,
    )
    .await
    .expect("second apply");
    assert_eq!(second.counts.requeued, 0);
    assert!(
        second.counts.skipped_active_job >= 1 || second.counts.already_not_requeued >= 1,
        "second pass must not double-enqueue"
    );

    let jobs_after_second: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM ingestion_jobs j JOIN documents d ON d.id = j.document_id WHERE d.workspace_id = $1 AND j.status = $2",
    )
    .bind(fixture.workspace_id)
    .bind(JOB_QUEUED)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(jobs_after_second, 1, "idempotent: still one active job");

    cleanup(&pool, &fixture).await;
}

#[tokio::test]
async fn requeue_helper_failed_or_completed_and_duplicate_prevention() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip ocr004 requeue helper: DATABASE_URL unavailable");
        return;
    };
    let fixture = seed_fixture(&pool).await;

    let first = requeue_document_for_reingest(
        &pool,
        fixture.needs_ocr_doc_id,
        fixture.workspace_id,
        5,
        RequeueEligibility::FailedOrCompleted,
    )
    .await
    .unwrap();
    assert!(first);

    let second = requeue_document_for_reingest(
        &pool,
        fixture.needs_ocr_doc_id,
        fixture.workspace_id,
        5,
        RequeueEligibility::FailedOrCompleted,
    )
    .await
    .unwrap();
    assert!(!second, "active job must block duplicate enqueue");

    let mock_ok = requeue_document_for_reingest(
        &pool,
        fixture.mock_doc_id,
        fixture.workspace_id,
        5,
        RequeueEligibility::FailedOrCompleted,
    )
    .await
    .unwrap();
    assert!(mock_ok, "COMPLETED mock-contaminated doc is requeueable");

    // FailedOnly must not requeue COMPLETED after already requeued path — use clean COMPLETED.
    let failed_only = requeue_document_for_reingest(
        &pool,
        fixture.clean_doc_id,
        fixture.workspace_id,
        5,
        RequeueEligibility::FailedOnly,
    )
    .await
    .unwrap();
    assert!(!failed_only, "FailedOnly must not touch COMPLETED");

    let active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM ingestion_jobs WHERE document_id = ANY($1::uuid[]) AND status IN ('QUEUED', 'PROCESSING')",
    )
    .bind(&[fixture.needs_ocr_doc_id, fixture.mock_doc_id])
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(active, 2);

    cleanup(&pool, &fixture).await;
}

#[tokio::test]
async fn active_job_skips_requeue_in_plan() {
    let _guard = test_lock().lock().await;
    let Some(pool) = pool_or_skip().await else {
        eprintln!("skip ocr004 active job: DATABASE_URL unavailable");
        return;
    };
    let fixture = seed_fixture(&pool).await;

    let mut tx = pool.begin().await.unwrap();
    enqueue_job_tx(&mut tx, fixture.needs_ocr_doc_id, fixture.workspace_id, 5)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let options = OcrAuditOptions {
        dry_run: true,
        apply: false,
        limit: 50,
        sample_limit: 10,
        tenant_id: Some(fixture.tenant_id),
        workspace_id: Some(fixture.workspace_id),
        max_attempts: 5,
    };
    let report = run_ocr_affected_audit(
        &pool,
        &options,
        &production_ocr_capability(),
        ObjectPresenceCheck::Skip,
    )
    .await
    .unwrap();

    let needs = report
        .samples
        .iter()
        .find(|s| s.document_id == fixture.needs_ocr_doc_id)
        .unwrap();
    assert!(needs.has_active_job);
    assert_eq!(needs.plan_action, "skip_active_job");
    assert!(report.counts.skipped_active_job >= 1);

    cleanup(&pool, &fixture).await;
}
