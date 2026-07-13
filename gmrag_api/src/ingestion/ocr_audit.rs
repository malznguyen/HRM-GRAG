//! OCR-004: audit + dry-run/report + guarded reingest planning cho corpus bị mock OCR / NEEDS_OCR.
//!
//! Không claim đã reprocess production corpus. Apply chỉ khi OCR capability mở và `--apply`.

use serde::Serialize;
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use super::jobs::{IngestionWorkerConfig, RequeueEligibility, requeue_document_for_reingest};
use super::ocr::{MOCK_OCR_MARKER, OcrCapability, production_ocr_capability};
use crate::storage::StorageClient;

/// Category bằng chứng SQL — không suy diễn lịch sử ngoài dữ liệu quan sát được.
pub const EVIDENCE_MOCK_OCR_CHUNK: &str = "mock_ocr_chunk";
pub const EVIDENCE_NEEDS_OCR_TERMINAL: &str = "needs_ocr_terminal";

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 500;
const DEFAULT_SAMPLE_LIMIT: usize = 20;

#[derive(Debug, Clone)]
pub struct OcrAuditOptions {
    /// Mặc định true: chỉ đọc SQL, không mutate.
    pub dry_run: bool,
    /// Explicit apply; bị refuse khi OCR unavailable.
    pub apply: bool,
    pub limit: i64,
    pub sample_limit: usize,
    pub tenant_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    pub max_attempts: i32,
}

impl Default for OcrAuditOptions {
    fn default() -> Self {
        Self {
            dry_run: true,
            apply: false,
            limit: DEFAULT_LIMIT,
            sample_limit: DEFAULT_SAMPLE_LIMIT,
            tenant_id: None,
            workspace_id: None,
            max_attempts: IngestionWorkerConfig::from_env().max_attempts,
        }
    }
}

/// Cách kiểm tra object storage trước apply.
pub enum ObjectPresenceCheck<'a> {
    /// Operator path: head object qua StorageClient.
    Storage(&'a StorageClient),
    /// Controlled fixture test only — không dùng CLI.
    AlwaysPresent,
    /// Dry-run: không gọi storage.
    Skip,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OcrAuditSample {
    pub document_id: Uuid,
    pub workspace_id: Uuid,
    pub tenant_id: Option<Uuid>,
    pub status: String,
    pub processing_stage: String,
    pub failure_code: Option<String>,
    pub evidence_categories: Vec<String>,
    pub has_active_job: bool,
    pub plan_action: String,
    /// Metadata storage only — không log content.
    pub has_object_key: bool,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct OcrAuditCounts {
    pub mock_ocr_chunk: usize,
    pub needs_ocr_terminal: usize,
    pub union_candidates: usize,
    pub selected_for_plan: usize,
    pub skipped_active_job: usize,
    pub skipped_object_missing: usize,
    pub skipped_not_requeueable: usize,
    pub requeued: usize,
    pub requeue_failed: usize,
    pub already_not_requeued: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OcrAuditReport {
    pub mode: String,
    pub dry_run: bool,
    pub apply_requested: bool,
    pub apply_executed: bool,
    pub apply_refused: bool,
    pub refusal_reason: Option<String>,
    pub ocr_provider_available: bool,
    pub ocr_provider: Option<String>,
    pub ocr_capability_detail: String,
    pub limit: i64,
    pub tenant_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    pub counts: OcrAuditCounts,
    pub samples: Vec<OcrAuditSample>,
    pub limitations: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug)]
pub enum OcrAuditError {
    Database(sqlx::Error),
    Storage(String),
    InvalidArgs(String),
}

impl std::fmt::Display for OcrAuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OcrAuditError::Database(err) => write!(f, "database error: {err}"),
            OcrAuditError::Storage(msg) => write!(f, "storage error: {msg}"),
            OcrAuditError::InvalidArgs(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for OcrAuditError {}

impl From<sqlx::Error> for OcrAuditError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct CandidateRow {
    document_id: Uuid,
    workspace_id: Uuid,
    tenant_id: Option<Uuid>,
    status: String,
    processing_stage: String,
    failure_code: Option<String>,
    object_key: String,
    has_mock_chunk: bool,
    is_needs_ocr_terminal: bool,
    has_active_job: bool,
    /// Chỉ phục vụ ORDER BY ổn định trong SQL; không đưa vào report.
    #[allow(dead_code)]
    created_at: Option<chrono::NaiveDateTime>,
}

/// Chạy audit (luôn) + apply requeue chỉ khi `apply` và OCR capability mở.
pub async fn run_ocr_affected_audit(
    pool: &PgPool,
    options: &OcrAuditOptions,
    capability: &OcrCapability,
    object_check: ObjectPresenceCheck<'_>,
) -> Result<OcrAuditReport, OcrAuditError> {
    validate_options(options)?;

    let mut report = base_report(options, capability);
    let rows = load_candidates(pool, options).await?;
    report.counts.union_candidates = rows.len();

    let mut selected: Vec<CandidateRow> = rows;
    // Stable order đã có trong SQL; clamp limit lần nữa cho an toàn.
    if selected.len() > options.limit as usize {
        selected.truncate(options.limit as usize);
    }
    report.counts.selected_for_plan = selected.len();

    for row in &selected {
        if row.has_mock_chunk {
            report.counts.mock_ocr_chunk += 1;
        }
        if row.is_needs_ocr_terminal {
            report.counts.needs_ocr_terminal += 1;
        }
    }

    // Apply gate: refuse trước khi bất kỳ mutation nào.
    if options.apply {
        if !capability.available {
            report.apply_refused = true;
            report.refusal_reason = Some(format!(
                "Apply refused: production OCR unavailable ({})",
                capability.detail
            ));
            report.mode = "apply_refused".to_string();
        } else if options.dry_run {
            report.apply_refused = true;
            report.refusal_reason = Some(
                "Apply refused: dry_run is still true; set dry_run=false with apply".to_string(),
            );
            report.mode = "apply_refused".to_string();
        } else {
            report.mode = "apply".to_string();
            report.apply_executed = true;
        }
    }

    let mut samples = Vec::new();
    for row in &selected {
        let evidence = evidence_categories(row.has_mock_chunk, row.is_needs_ocr_terminal);
        let plan_action = plan_action_for(row, report.apply_executed);

        if row.has_active_job {
            report.counts.skipped_active_job += 1;
        } else if !is_requeueable_status(&row.status) {
            report.counts.skipped_not_requeueable += 1;
        }

        let mut sample = OcrAuditSample {
            document_id: row.document_id,
            workspace_id: row.workspace_id,
            tenant_id: row.tenant_id,
            status: row.status.clone(),
            processing_stage: row.processing_stage.clone(),
            failure_code: row.failure_code.clone(),
            evidence_categories: evidence,
            has_active_job: row.has_active_job,
            plan_action: plan_action.clone(),
            has_object_key: !row.object_key.is_empty(),
        };

        if report.apply_executed
            && plan_action == "requeue"
            && !row.has_active_job
            && is_requeueable_status(&row.status)
        {
            match check_object_present(&object_check, &row.object_key).await {
                Ok(true) => match requeue_document_for_reingest(
                    pool,
                    row.document_id,
                    row.workspace_id,
                    options.max_attempts,
                    RequeueEligibility::FailedOrCompleted,
                )
                .await
                {
                    Ok(true) => {
                        report.counts.requeued += 1;
                        sample.plan_action = "requeued".to_string();
                    }
                    Ok(false) => {
                        report.counts.already_not_requeued += 1;
                        sample.plan_action = "not_requeued_race_or_ineligible".to_string();
                    }
                    Err(err) => {
                        report.counts.requeue_failed += 1;
                        sample.plan_action = "requeue_error".to_string();
                        report
                            .errors
                            .push(format!("requeue {}: {err}", row.document_id));
                    }
                },
                Ok(false) => {
                    report.counts.skipped_object_missing += 1;
                    sample.plan_action = "skip_object_missing".to_string();
                }
                Err(err) => {
                    report.counts.requeue_failed += 1;
                    sample.plan_action = "object_check_error".to_string();
                    report
                        .errors
                        .push(format!("object check {}: {err}", row.document_id));
                }
            }
        }

        if samples.len() < options.sample_limit {
            samples.push(sample);
        }
    }

    report.samples = samples;
    Ok(report)
}

/// Capability mặc định production (đóng) — CLI dùng hàm này; test có thể inject capability mở.
pub fn default_capability() -> OcrCapability {
    production_ocr_capability()
}

pub fn format_human_report(report: &OcrAuditReport) -> String {
    let mut out = String::new();
    out.push_str("OCR-004 affected-documents audit\n");
    out.push_str(&format!("mode={}\n", report.mode));
    out.push_str(&format!("dry_run={}\n", report.dry_run));
    out.push_str(&format!("apply_requested={}\n", report.apply_requested));
    out.push_str(&format!("apply_executed={}\n", report.apply_executed));
    out.push_str(&format!("apply_refused={}\n", report.apply_refused));
    if let Some(reason) = &report.refusal_reason {
        out.push_str(&format!("refusal_reason={reason}\n"));
    }
    out.push_str(&format!(
        "ocr_provider_available={}\n",
        report.ocr_provider_available
    ));
    if let Some(provider) = &report.ocr_provider {
        out.push_str(&format!("ocr_provider={provider}\n"));
    }
    out.push_str(&format!(
        "ocr_capability_detail={}\n",
        report.ocr_capability_detail
    ));
    out.push_str(&format!("limit={}\n", report.limit));
    if let Some(tenant_id) = report.tenant_id {
        out.push_str(&format!("tenant_id={tenant_id}\n"));
    }
    if let Some(workspace_id) = report.workspace_id {
        out.push_str(&format!("workspace_id={workspace_id}\n"));
    }
    out.push_str("counts:\n");
    out.push_str(&format!(
        "  mock_ocr_chunk={}\n",
        report.counts.mock_ocr_chunk
    ));
    out.push_str(&format!(
        "  needs_ocr_terminal={}\n",
        report.counts.needs_ocr_terminal
    ));
    out.push_str(&format!(
        "  union_candidates={}\n",
        report.counts.union_candidates
    ));
    out.push_str(&format!(
        "  selected_for_plan={}\n",
        report.counts.selected_for_plan
    ));
    out.push_str(&format!(
        "  skipped_active_job={}\n",
        report.counts.skipped_active_job
    ));
    out.push_str(&format!(
        "  skipped_object_missing={}\n",
        report.counts.skipped_object_missing
    ));
    out.push_str(&format!(
        "  skipped_not_requeueable={}\n",
        report.counts.skipped_not_requeueable
    ));
    out.push_str(&format!("  requeued={}\n", report.counts.requeued));
    out.push_str(&format!(
        "  requeue_failed={}\n",
        report.counts.requeue_failed
    ));
    out.push_str(&format!(
        "  already_not_requeued={}\n",
        report.counts.already_not_requeued
    ));
    if !report.samples.is_empty() {
        out.push_str("samples (metadata only; no chunk/document text):\n");
        for sample in &report.samples {
            out.push_str(&format!(
                "  document_id={} workspace_id={} status={} failure_code={:?} evidence={:?} action={} active_job={}\n",
                sample.document_id,
                sample.workspace_id,
                sample.status,
                sample.failure_code,
                sample.evidence_categories,
                sample.plan_action,
                sample.has_active_job
            ));
        }
    }
    if !report.limitations.is_empty() {
        out.push_str("limitations:\n");
        for item in &report.limitations {
            out.push_str(&format!("  - {item}\n"));
        }
    }
    if !report.errors.is_empty() {
        out.push_str("errors:\n");
        for err in &report.errors {
            out.push_str(&format!("  - {err}\n"));
        }
    }
    out
}

/// Phân loại evidence từ cờ quan sát (dùng cho unit test deterministic).
pub fn classify_evidence(has_mock_chunk: bool, is_needs_ocr_terminal: bool) -> Vec<&'static str> {
    evidence_categories(has_mock_chunk, is_needs_ocr_terminal)
        .into_iter()
        .map(|s| match s.as_str() {
            EVIDENCE_MOCK_OCR_CHUNK => EVIDENCE_MOCK_OCR_CHUNK,
            EVIDENCE_NEEDS_OCR_TERMINAL => EVIDENCE_NEEDS_OCR_TERMINAL,
            other => panic!("unexpected evidence category: {other}"),
        })
        .collect()
}

/// Chọn tối đa `limit` id theo thứ tự ổn định (created_at, document_id).
pub fn select_bounded_ids(
    mut items: Vec<(Option<chrono::NaiveDateTime>, Uuid)>,
    limit: usize,
) -> Vec<Uuid> {
    items.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    items.into_iter().take(limit).map(|(_, id)| id).collect()
}

fn validate_options(options: &OcrAuditOptions) -> Result<(), OcrAuditError> {
    if options.limit < 1 || options.limit > MAX_LIMIT {
        return Err(OcrAuditError::InvalidArgs(format!(
            "limit must be between 1 and {MAX_LIMIT}"
        )));
    }
    if options.sample_limit == 0 {
        return Err(OcrAuditError::InvalidArgs(
            "sample_limit must be >= 1".to_string(),
        ));
    }
    if options.apply && options.dry_run {
        // Cho phép: run vẫn refuse apply; không lỗi parse.
    }
    Ok(())
}

fn base_report(options: &OcrAuditOptions, capability: &OcrCapability) -> OcrAuditReport {
    OcrAuditReport {
        mode: if options.apply {
            "apply_pending".to_string()
        } else {
            "dry_run".to_string()
        },
        dry_run: options.dry_run,
        apply_requested: options.apply,
        apply_executed: false,
        apply_refused: false,
        refusal_reason: None,
        ocr_provider_available: capability.available,
        ocr_provider: capability.provider.map(str::to_string),
        ocr_capability_detail: capability.detail.to_string(),
        limit: options.limit,
        tenant_id: options.tenant_id,
        workspace_id: options.workspace_id,
        counts: OcrAuditCounts::default(),
        samples: Vec::new(),
        limitations: vec![
            "Evidence is SQL-observed only: chunk marker mock_ocr_text and/or failure_code=NEEDS_OCR."
                .to_string(),
            "This tool does not prove historical mock injection for rows without those markers."
                .to_string(),
            "No production corpus is claimed; reports only reflect the connected database."
                .to_string(),
            "Production OCR integration is out of scope for OCR-004; apply is gated closed until capability is true."
                .to_string(),
            "Report samples contain IDs/metadata only — never chunk or document text."
                .to_string(),
        ],
        errors: Vec::new(),
    }
}

fn evidence_categories(has_mock_chunk: bool, is_needs_ocr_terminal: bool) -> Vec<String> {
    let mut cats = Vec::new();
    if has_mock_chunk {
        cats.push(EVIDENCE_MOCK_OCR_CHUNK.to_string());
    }
    if is_needs_ocr_terminal {
        cats.push(EVIDENCE_NEEDS_OCR_TERMINAL.to_string());
    }
    cats
}

fn is_requeueable_status(status: &str) -> bool {
    status == "FAILED" || status == "COMPLETED"
}

fn plan_action_for(row: &CandidateRow, apply_executed: bool) -> String {
    if row.has_active_job {
        return "skip_active_job".to_string();
    }
    if !is_requeueable_status(&row.status) {
        return "skip_not_requeueable".to_string();
    }
    if apply_executed {
        "requeue".to_string()
    } else {
        "plan_requeue".to_string()
    }
}

async fn check_object_present(
    mode: &ObjectPresenceCheck<'_>,
    object_key: &str,
) -> Result<bool, OcrAuditError> {
    match mode {
        ObjectPresenceCheck::Storage(storage) => storage
            .object_exists(object_key)
            .await
            .map_err(|err| OcrAuditError::Storage(err.to_string())),
        ObjectPresenceCheck::AlwaysPresent => Ok(true),
        ObjectPresenceCheck::Skip => Err(OcrAuditError::InvalidArgs(
            "object presence check is required for apply".to_string(),
        )),
    }
}

async fn load_candidates(
    pool: &PgPool,
    options: &OcrAuditOptions,
) -> Result<Vec<CandidateRow>, sqlx::Error> {
    // Union bằng chứng: (1) chunk chứa marker mock, (2) terminal NEEDS_OCR.
    // Không trả original_text. ORDER BY ổn định; LIMIT bound.
    let rows = sqlx::query_as::<_, CandidateRow>(
        r#"
        WITH mock_docs AS (
            SELECT DISTINCT c.document_id
            FROM document_chunks c
            WHERE position($1 in c.original_text) > 0
        ),
        affected AS (
            SELECT
                d.id AS document_id,
                d.workspace_id,
                w.tenant_id,
                d.status,
                d.processing_stage,
                d.failure_code,
                d.object_key,
                (md.document_id IS NOT NULL) AS has_mock_chunk,
                (d.failure_code = 'NEEDS_OCR' AND d.status = 'FAILED') AS is_needs_ocr_terminal,
                EXISTS (
                    SELECT 1
                    FROM ingestion_jobs j
                    WHERE j.document_id = d.id
                      AND j.status IN ('QUEUED', 'PROCESSING')
                ) AS has_active_job,
                d.created_at
            FROM documents d
            INNER JOIN workspaces w ON w.id = d.workspace_id
            LEFT JOIN mock_docs md ON md.document_id = d.id
            WHERE (
                md.document_id IS NOT NULL
                OR (d.failure_code = 'NEEDS_OCR' AND d.status = 'FAILED')
            )
              AND ($2::uuid IS NULL OR d.workspace_id = $2)
              AND ($3::uuid IS NULL OR w.tenant_id = $3)
        )
        SELECT *
        FROM affected
        ORDER BY created_at ASC NULLS LAST, document_id ASC
        LIMIT $4
        "#,
    )
    .bind(MOCK_OCR_MARKER)
    .bind(options.workspace_id)
    .bind(options.tenant_id)
    .bind(options.limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Loại sự kiện audit được phép ghi khi operator path có mutation intent.
/// Dry-run thuần không map sang biến thể nào (không INSERT `audit_events`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OcrAuditPersistKind {
    ApplyCompleted,
    ApplyRefused,
}

/// Dry-run thuần → `None` (read-only). Chỉ `--apply` (completed hoặc refused) mới persist.
pub fn should_persist_audit_event(report: &OcrAuditReport) -> Option<OcrAuditPersistKind> {
    if report.apply_executed {
        Some(OcrAuditPersistKind::ApplyCompleted)
    } else if report.apply_refused {
        Some(OcrAuditPersistKind::ApplyRefused)
    } else {
        None
    }
}

/// Metadata-only JSON cho audit_events — không nhét sample text.
pub fn report_to_audit_metadata(report: &OcrAuditReport) -> serde_json::Value {
    serde_json::json!({
        "mode": report.mode,
        "dry_run": report.dry_run,
        "apply_requested": report.apply_requested,
        "apply_executed": report.apply_executed,
        "apply_refused": report.apply_refused,
        "refusal_reason": report.refusal_reason,
        "ocr_provider_available": report.ocr_provider_available,
        "counts": {
            "mock_ocr_chunk": report.counts.mock_ocr_chunk,
            "needs_ocr_terminal": report.counts.needs_ocr_terminal,
            "union_candidates": report.counts.union_candidates,
            "selected_for_plan": report.counts.selected_for_plan,
            "skipped_active_job": report.counts.skipped_active_job,
            "skipped_object_missing": report.counts.skipped_object_missing,
            "skipped_not_requeueable": report.counts.skipped_not_requeueable,
            "requeued": report.counts.requeued,
            "requeue_failed": report.counts.requeue_failed,
            "already_not_requeued": report.counts.already_not_requeued,
        },
        "sample_document_ids": report.samples.iter().map(|s| s.document_id).collect::<Vec<_>>(),
        "limitations_count": report.limitations.len(),
        "errors_count": report.errors.len(),
    })
}

/// Gộp evidence map theo document (helper pure cho test).
pub fn merge_evidence_maps(
    mock_ids: impl IntoIterator<Item = Uuid>,
    needs_ocr_ids: impl IntoIterator<Item = Uuid>,
) -> BTreeMap<Uuid, BTreeSet<&'static str>> {
    let mut map: BTreeMap<Uuid, BTreeSet<&'static str>> = BTreeMap::new();
    for id in mock_ids {
        map.entry(id).or_default().insert(EVIDENCE_MOCK_OCR_CHUNK);
    }
    for id in needs_ocr_ids {
        map.entry(id)
            .or_default()
            .insert(EVIDENCE_NEEDS_OCR_TERMINAL);
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn classify_evidence_categories_are_explicit() {
        assert_eq!(classify_evidence(false, false), Vec::<&str>::new());
        assert_eq!(
            classify_evidence(true, false),
            vec![EVIDENCE_MOCK_OCR_CHUNK]
        );
        assert_eq!(
            classify_evidence(false, true),
            vec![EVIDENCE_NEEDS_OCR_TERMINAL]
        );
        assert_eq!(
            classify_evidence(true, true),
            vec![EVIDENCE_MOCK_OCR_CHUNK, EVIDENCE_NEEDS_OCR_TERMINAL]
        );
    }

    #[test]
    fn merge_evidence_maps_unions_categories() {
        let a = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let b = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        let c = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
        let map = merge_evidence_maps([a, b], [b, c]);
        assert_eq!(map.len(), 3);
        assert!(map[&a].contains(EVIDENCE_MOCK_OCR_CHUNK));
        assert!(map[&b].contains(EVIDENCE_MOCK_OCR_CHUNK));
        assert!(map[&b].contains(EVIDENCE_NEEDS_OCR_TERMINAL));
        assert!(map[&c].contains(EVIDENCE_NEEDS_OCR_TERMINAL));
    }

    #[test]
    fn select_bounded_ids_stable_order_and_limit() {
        let t1 = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let t2 = NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let id_early = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let id_late = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
        let id_same_day_low = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let id_same_day_high = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let selected = select_bounded_ids(
            vec![
                (Some(t2), id_late),
                (Some(t1), id_same_day_high),
                (Some(t1), id_same_day_low),
                (Some(t1), id_early),
            ],
            2,
        );
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0], id_same_day_low);
        assert_eq!(selected[1], id_same_day_high);
    }

    #[test]
    fn report_json_has_no_text_fields() {
        let report = OcrAuditReport {
            mode: "dry_run".into(),
            dry_run: true,
            apply_requested: false,
            apply_executed: false,
            apply_refused: false,
            refusal_reason: None,
            ocr_provider_available: false,
            ocr_provider: None,
            ocr_capability_detail: "closed".into(),
            limit: 10,
            tenant_id: None,
            workspace_id: None,
            counts: OcrAuditCounts {
                mock_ocr_chunk: 1,
                ..OcrAuditCounts::default()
            },
            samples: vec![OcrAuditSample {
                document_id: Uuid::nil(),
                workspace_id: Uuid::nil(),
                tenant_id: None,
                status: "COMPLETED".into(),
                processing_stage: "DONE".into(),
                failure_code: None,
                evidence_categories: vec![EVIDENCE_MOCK_OCR_CHUNK.into()],
                has_active_job: false,
                plan_action: "plan_requeue".into(),
                has_object_key: true,
            }],
            limitations: vec!["x".into()],
            errors: vec![],
        };
        let json = serde_json::to_string(&report).unwrap();
        // Category name mock_ocr_chunk is metadata; marker mock_ocr_text must not appear.
        assert!(!json.contains(MOCK_OCR_MARKER));
        assert!(!json.contains("original_text"));
        assert!(!json.contains("chunk_text"));
        let meta = report_to_audit_metadata(&report);
        let meta_s = meta.to_string();
        assert!(!meta_s.contains("original_text"));
        assert!(!meta_s.contains(MOCK_OCR_MARKER));
        assert!(meta_s.contains("sample_document_ids"));
    }

    #[test]
    fn default_options_are_dry_run() {
        let opts = OcrAuditOptions::default();
        assert!(opts.dry_run);
        assert!(!opts.apply);
        assert!(opts.limit > 0 && opts.limit <= MAX_LIMIT);
    }

    #[test]
    fn capability_closed_implies_apply_refused_reason_shape() {
        let cap = default_capability();
        assert!(!cap.available);
        let reason = format!("Apply refused: production OCR unavailable ({})", cap.detail);
        assert!(reason.contains("not integrated") || reason.contains("None"));
    }

    #[test]
    fn dry_run_report_must_not_persist_audit_event() {
        let report = OcrAuditReport {
            mode: "dry_run".into(),
            dry_run: true,
            apply_requested: false,
            apply_executed: false,
            apply_refused: false,
            refusal_reason: None,
            ocr_provider_available: false,
            ocr_provider: None,
            ocr_capability_detail: "closed".into(),
            limit: 10,
            tenant_id: None,
            workspace_id: None,
            counts: OcrAuditCounts::default(),
            samples: Vec::new(),
            limitations: Vec::new(),
            errors: Vec::new(),
        };
        assert_eq!(should_persist_audit_event(&report), None);
    }

    #[test]
    fn apply_refused_and_completed_persist_metadata_only_audit() {
        let refused = OcrAuditReport {
            mode: "apply_refused".into(),
            dry_run: false,
            apply_requested: true,
            apply_executed: false,
            apply_refused: true,
            refusal_reason: Some("OCR unavailable".into()),
            ocr_provider_available: false,
            ocr_provider: None,
            ocr_capability_detail: "closed".into(),
            limit: 10,
            tenant_id: None,
            workspace_id: None,
            counts: OcrAuditCounts::default(),
            samples: Vec::new(),
            limitations: Vec::new(),
            errors: Vec::new(),
        };
        assert_eq!(
            should_persist_audit_event(&refused),
            Some(OcrAuditPersistKind::ApplyRefused)
        );

        let completed = OcrAuditReport {
            mode: "apply".into(),
            dry_run: false,
            apply_requested: true,
            apply_executed: true,
            apply_refused: false,
            refusal_reason: None,
            ocr_provider_available: true,
            ocr_provider: Some("test".into()),
            ocr_capability_detail: "open".into(),
            limit: 10,
            tenant_id: None,
            workspace_id: None,
            counts: OcrAuditCounts {
                requeued: 1,
                ..OcrAuditCounts::default()
            },
            samples: Vec::new(),
            limitations: Vec::new(),
            errors: Vec::new(),
        };
        assert_eq!(
            should_persist_audit_event(&completed),
            Some(OcrAuditPersistKind::ApplyCompleted)
        );

        let meta = report_to_audit_metadata(&refused).to_string();
        assert!(!meta.contains("original_text"));
        assert!(!meta.contains(MOCK_OCR_MARKER));
    }
}
