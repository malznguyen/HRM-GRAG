//! LIFE-007: retention/audit report — object/vector nào còn sót lại sau delete.
//!
//! Read-only hoàn toàn: chỉ SELECT PostgreSQL, scroll Qdrant và list object storage.
//! Không xoá point/object, không enqueue outbox, không ghi `audit_events`.
//!
//! Khác LIFE-006 (`cleanup-qdrant-orphans` / `cleanup-storage-objects`): LIFE-006 đi tìm
//! target để dọn, còn báo cáo này trả lời câu hỏi retention "đã xoá rồi thì dữ liệu
//! thật sự biến mất chưa" — và quan trọng nhất là tách residue **còn worker sẽ dọn**
//! khỏi residue **không còn ai chịu trách nhiệm**.

use std::collections::{BTreeMap, HashSet};

use chrono::NaiveDateTime;
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::outbox::{STATUS_DEAD, STATUS_FAILED, STATUS_PENDING};
use crate::retrieval::RetrievalClient;
use crate::retrieval::outbox::QdrantOutboxEventType;
use crate::storage::StorageClient;
use crate::storage::outbox::StorageOutboxEventType;

/// Outbox row (PENDING/FAILED) vẫn còn nợ việc xoá — worker sẽ tự dọn.
pub const CLASS_RECOVERY_PENDING: &str = "recovery_pending";
/// Outbox row DEAD — hết retry, cần operator can thiệp.
pub const CLASS_RECOVERY_DEAD: &str = "recovery_dead";
/// Có delete event nhưng không còn outbox row nào nợ → residue im lặng, không ai dọn.
pub const CLASS_UNRECOVERED: &str = "unrecovered";
/// Không outbox row, cũng không delete event tương ứng → residue không rõ nguồn gốc.
pub const CLASS_UNEXPLAINED: &str = "unexplained";

const DEFAULT_SAMPLE_LIMIT: usize = 50;
const DEFAULT_SCROLL_PAGE_SIZE: usize = 256;

const DELETE_EVENT_TYPES: [&str; 3] = ["document_deleted", "workspace_deleted", "tenant_deleted"];

#[derive(Debug, Clone)]
pub struct RetentionReportOptions {
    /// Scroll toàn collection Qdrant (đắt trên collection lớn).
    pub probe_vectors: bool,
    /// List toàn bucket object storage.
    pub probe_objects: bool,
    pub scroll_page_size: usize,
    /// Trần số dòng residue liệt kê trong report (counts vẫn là tổng đầy đủ).
    pub sample_limit: usize,
}

impl Default for RetentionReportOptions {
    fn default() -> Self {
        Self {
            probe_vectors: true,
            probe_objects: true,
            scroll_page_size: DEFAULT_SCROLL_PAGE_SIZE,
            sample_limit: DEFAULT_SAMPLE_LIMIT,
        }
    }
}

/// Point Qdrant quan sát được khi scroll (payload only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScannedVector {
    pub workspace_id: Uuid,
    pub document_id: Uuid,
}

/// Việc xoá vector mà outbox còn nợ, kèm status của row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwedVectorTarget {
    Document {
        workspace_id: Uuid,
        document_id: Uuid,
        status: String,
    },
    Workspace {
        workspace_id: Uuid,
        status: String,
    },
    Workspaces {
        workspace_ids: Vec<Uuid>,
        status: String,
    },
}

/// Việc xoá object mà outbox còn nợ, kèm status của row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwedObjectTarget {
    Object { object_key: String, status: String },
    Prefix { prefix: String, status: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteEventRow {
    pub event_type: String,
    pub created_at: NaiveDateTime,
    pub tenant_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    pub document_id: Option<Uuid>,
}

/// Delete event gần nhất khớp với residue — bằng chứng "đã từng xoá", không phải nhân quả.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DeleteEventRef {
    pub event_type: String,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct VectorResidue {
    pub workspace_id: Uuid,
    pub document_id: Uuid,
    /// Workspace còn trong SQL (chỉ document bị xoá) hay đã biến mất cả workspace.
    pub workspace_live: bool,
    pub class: String,
    pub owed_outbox_status: Option<String>,
    pub delete_event: Option<DeleteEventRef>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ObjectResidue {
    pub object_key: String,
    pub tenant_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    pub document_id: Option<Uuid>,
    pub class: String,
    pub owed_outbox_status: Option<String>,
    pub delete_event: Option<DeleteEventRef>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RetentionCounts {
    pub vector_residue: usize,
    pub object_residue: usize,
    pub recovery_pending: usize,
    pub recovery_dead: usize,
    pub unrecovered: usize,
    pub unexplained: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RetentionReport {
    pub vectors_probed: bool,
    pub objects_probed: bool,
    pub scanned_vector_points: usize,
    pub scanned_object_keys: usize,
    pub counts: RetentionCounts,
    pub by_class: BTreeMap<String, usize>,
    pub vector_residue: Vec<VectorResidue>,
    pub object_residue: Vec<ObjectResidue>,
    /// True khi `sample_limit` đã cắt danh sách — counts vẫn đầy đủ, không im lặng.
    pub vector_residue_truncated: bool,
    pub object_residue_truncated: bool,
    pub limitations: Vec<String>,
}

/// Input đã thu thập xong — tách khỏi I/O để `build_retention_report` thuần và test được.
#[derive(Debug, Clone, Default)]
pub struct RetentionInputs {
    pub live_workspaces: HashSet<Uuid>,
    pub live_documents: HashSet<(Uuid, Uuid)>,
    pub live_object_keys: HashSet<String>,
    /// `None` = không probe (khác với `Some(vec![])` = probe và sạch).
    pub scanned_vectors: Option<Vec<ScannedVector>>,
    pub scanned_object_keys: Option<Vec<String>>,
    pub owed_vectors: Vec<OwedVectorTarget>,
    pub owed_objects: Vec<OwedObjectTarget>,
    pub delete_events: Vec<DeleteEventRow>,
}

#[derive(Debug)]
pub enum RetentionReportError {
    Database(sqlx::Error),
    Retrieval(crate::retrieval::RetrievalError),
    Storage(crate::storage::StorageError),
}

impl std::fmt::Display for RetentionReportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::Retrieval(error) => write!(formatter, "qdrant error: {error}"),
            Self::Storage(error) => write!(formatter, "object storage error: {error}"),
        }
    }
}

impl std::error::Error for RetentionReportError {}

impl From<sqlx::Error> for RetentionReportError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

impl From<crate::retrieval::RetrievalError> for RetentionReportError {
    fn from(value: crate::retrieval::RetrievalError) -> Self {
        Self::Retrieval(value)
    }
}

impl From<crate::storage::StorageError> for RetentionReportError {
    fn from(value: crate::storage::StorageError) -> Self {
        Self::Storage(value)
    }
}

/// Thu thập inventory + probe rồi build report. Read-only ở mọi nhánh.
///
/// `retrieval`/`storage` là `None` khi operator tắt probe tương ứng — report ghi rõ
/// `vectors_probed=false` / `objects_probed=false` thay vì báo sạch.
pub async fn run_retention_report(
    pool: &PgPool,
    retrieval: Option<&RetrievalClient>,
    storage: Option<&StorageClient>,
    options: &RetentionReportOptions,
) -> Result<RetentionReport, RetentionReportError> {
    let mut inputs = RetentionInputs {
        live_workspaces: load_live_workspaces(pool).await?,
        live_documents: load_live_documents(pool).await?,
        live_object_keys: load_live_object_keys(pool).await?,
        owed_vectors: load_owed_vectors(pool).await?,
        owed_objects: load_owed_objects(pool).await?,
        delete_events: load_delete_events(pool).await?,
        ..RetentionInputs::default()
    };

    if options.probe_vectors
        && let Some(retrieval) = retrieval
    {
        inputs.scanned_vectors = Some(scan_vectors(retrieval, options.scroll_page_size).await?);
    }

    if options.probe_objects
        && let Some(storage) = storage
    {
        inputs.scanned_object_keys = Some(scan_object_keys(storage).await?);
    }

    Ok(build_retention_report(&inputs, options.sample_limit))
}

/// Core thuần: phân loại residue từ input đã quan sát. Không I/O, deterministic.
pub fn build_retention_report(inputs: &RetentionInputs, sample_limit: usize) -> RetentionReport {
    let mut vector_residue = collect_vector_residue(inputs);
    let mut object_residue = collect_object_residue(inputs);

    vector_residue.sort_by(|left, right| {
        (left.workspace_id, left.document_id).cmp(&(right.workspace_id, right.document_id))
    });
    object_residue.sort_by(|left, right| left.object_key.cmp(&right.object_key));

    let mut counts = RetentionCounts {
        vector_residue: vector_residue.len(),
        object_residue: object_residue.len(),
        ..RetentionCounts::default()
    };
    let mut by_class: BTreeMap<String, usize> = BTreeMap::new();
    for class in vector_residue
        .iter()
        .map(|residue| residue.class.as_str())
        .chain(object_residue.iter().map(|residue| residue.class.as_str()))
    {
        *by_class.entry(class.to_string()).or_default() += 1;
        match class {
            CLASS_RECOVERY_PENDING => counts.recovery_pending += 1,
            CLASS_RECOVERY_DEAD => counts.recovery_dead += 1,
            CLASS_UNRECOVERED => counts.unrecovered += 1,
            CLASS_UNEXPLAINED => counts.unexplained += 1,
            _ => {}
        }
    }

    let vector_residue_truncated = vector_residue.len() > sample_limit;
    let object_residue_truncated = object_residue.len() > sample_limit;
    vector_residue.truncate(sample_limit);
    object_residue.truncate(sample_limit);

    RetentionReport {
        vectors_probed: inputs.scanned_vectors.is_some(),
        objects_probed: inputs.scanned_object_keys.is_some(),
        scanned_vector_points: inputs
            .scanned_vectors
            .as_ref()
            .map_or(0, |points| points.len()),
        scanned_object_keys: inputs
            .scanned_object_keys
            .as_ref()
            .map_or(0, |keys| keys.len()),
        counts,
        by_class,
        vector_residue,
        object_residue,
        vector_residue_truncated,
        object_residue_truncated,
        limitations: limitations(inputs),
    }
}

fn limitations(inputs: &RetentionInputs) -> Vec<String> {
    let mut limitations = vec![
        "Residue is judged against the SQL inventory read in this run. A delete committing \
         mid-scan can appear as residue on this pass and be clear on the next."
            .to_string(),
        "A matching delete event proves the resource ID appeared in an audited delete; it does \
         not by itself prove that delete caused this residue."
            .to_string(),
        format!(
            "`{CLASS_UNEXPLAINED}` means no owing outbox row and no delete event was found — \
             treat it as unknown provenance, not proof of an unaudited delete."
        ),
    ];
    if inputs.scanned_vectors.is_none() {
        limitations
            .push("Vectors were not probed: vector residue is unknown, not zero.".to_string());
    }
    if inputs.scanned_object_keys.is_none() {
        limitations
            .push("Objects were not probed: object residue is unknown, not zero.".to_string());
    }
    limitations
}

fn collect_vector_residue(inputs: &RetentionInputs) -> Vec<VectorResidue> {
    let Some(points) = inputs.scanned_vectors.as_ref() else {
        return Vec::new();
    };

    let mut seen: HashSet<(Uuid, Uuid)> = HashSet::new();
    let mut residue = Vec::new();
    for point in points {
        // Một document có nhiều point; chỉ báo cáo ở mức (workspace, document).
        if !seen.insert((point.workspace_id, point.document_id)) {
            continue;
        }
        let workspace_live = inputs.live_workspaces.contains(&point.workspace_id);
        let document_live = inputs
            .live_documents
            .contains(&(point.workspace_id, point.document_id));
        if workspace_live && document_live {
            continue;
        }

        let owed_outbox_status =
            owed_status_for_vector(&inputs.owed_vectors, point.workspace_id, point.document_id);
        let delete_event = latest_delete_event(
            &inputs.delete_events,
            None,
            Some(point.workspace_id),
            Some(point.document_id),
        );
        residue.push(VectorResidue {
            workspace_id: point.workspace_id,
            document_id: point.document_id,
            workspace_live,
            class: classify(owed_outbox_status.as_deref(), delete_event.is_some()),
            owed_outbox_status,
            delete_event,
        });
    }
    residue
}

fn collect_object_residue(inputs: &RetentionInputs) -> Vec<ObjectResidue> {
    let Some(keys) = inputs.scanned_object_keys.as_ref() else {
        return Vec::new();
    };

    let mut seen: HashSet<&str> = HashSet::new();
    let mut residue = Vec::new();
    for object_key in keys {
        if !seen.insert(object_key.as_str()) {
            continue;
        }
        // Object đang được một document sống trỏ tới → dữ liệu live, không phải residue.
        if inputs.live_object_keys.contains(object_key) {
            continue;
        }

        let (tenant_id, workspace_id, document_id) = parse_object_key(object_key);
        let owed_outbox_status = owed_status_for_object(&inputs.owed_objects, object_key);
        let delete_event =
            latest_delete_event(&inputs.delete_events, tenant_id, workspace_id, document_id);
        residue.push(ObjectResidue {
            object_key: object_key.clone(),
            tenant_id,
            workspace_id,
            document_id,
            class: classify(owed_outbox_status.as_deref(), delete_event.is_some()),
            owed_outbox_status,
            delete_event,
        });
    }
    residue
}

/// Phân loại residue theo "còn ai nợ việc dọn không".
pub fn classify(owed_outbox_status: Option<&str>, has_delete_event: bool) -> String {
    match owed_outbox_status {
        Some(STATUS_DEAD) => CLASS_RECOVERY_DEAD,
        Some(_) => CLASS_RECOVERY_PENDING,
        None if has_delete_event => CLASS_UNRECOVERED,
        None => CLASS_UNEXPLAINED,
    }
    .to_string()
}

/// Recovery đang chạy (PENDING/FAILED) thắng DEAD: vẫn còn đường tự khỏi.
fn pick_owed_status(statuses: Vec<String>) -> Option<String> {
    for wanted in [STATUS_PENDING, STATUS_FAILED, STATUS_DEAD] {
        if let Some(found) = statuses.iter().find(|status| status.as_str() == wanted) {
            return Some(found.clone());
        }
    }
    statuses.into_iter().min()
}

fn owed_status_for_vector(
    owed: &[OwedVectorTarget],
    workspace_id: Uuid,
    document_id: Uuid,
) -> Option<String> {
    let statuses = owed
        .iter()
        .filter_map(|target| match target {
            OwedVectorTarget::Document {
                workspace_id: owed_workspace,
                document_id: owed_document,
                status,
            } => (*owed_workspace == workspace_id && *owed_document == document_id)
                .then(|| status.clone()),
            OwedVectorTarget::Workspace {
                workspace_id: owed_workspace,
                status,
            } => (*owed_workspace == workspace_id).then(|| status.clone()),
            OwedVectorTarget::Workspaces {
                workspace_ids,
                status,
            } => workspace_ids
                .contains(&workspace_id)
                .then(|| status.clone()),
        })
        .collect();
    pick_owed_status(statuses)
}

fn owed_status_for_object(owed: &[OwedObjectTarget], object_key: &str) -> Option<String> {
    let statuses = owed
        .iter()
        .filter_map(|target| match target {
            OwedObjectTarget::Object {
                object_key: owed_key,
                status,
            } => (owed_key == object_key).then(|| status.clone()),
            OwedObjectTarget::Prefix { prefix, status } => object_key
                .starts_with(prefix.as_str())
                .then(|| status.clone()),
        })
        .collect();
    pick_owed_status(statuses)
}

fn latest_delete_event(
    events: &[DeleteEventRow],
    tenant_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
    document_id: Option<Uuid>,
) -> Option<DeleteEventRef> {
    events
        .iter()
        .filter(|event| event_covers(event, tenant_id, workspace_id, document_id))
        .max_by_key(|event| event.created_at)
        .map(|event| DeleteEventRef {
            event_type: event.event_type.clone(),
            created_at: event.created_at,
        })
}

fn event_covers(
    event: &DeleteEventRow,
    tenant_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
    document_id: Option<Uuid>,
) -> bool {
    match event.event_type.as_str() {
        "document_deleted" => document_id.is_some() && event.document_id == document_id,
        "workspace_deleted" => workspace_id.is_some() && event.workspace_id == workspace_id,
        "tenant_deleted" => tenant_id.is_some() && event.tenant_id == tenant_id,
        _ => false,
    }
}

/// Tách `tenants/{t}/workspaces/{w}/documents/{d}/...` thành id — segment lạ trả `None`.
pub fn parse_object_key(object_key: &str) -> (Option<Uuid>, Option<Uuid>, Option<Uuid>) {
    let segments: Vec<&str> = object_key.split('/').collect();
    let mut tenant_id = None;
    let mut workspace_id = None;
    let mut document_id = None;
    for window in segments.windows(2) {
        let value = Uuid::parse_str(window[1]).ok();
        match window[0] {
            "tenants" => tenant_id = value,
            "workspaces" => workspace_id = value,
            "documents" => document_id = value,
            _ => {}
        }
    }
    (tenant_id, workspace_id, document_id)
}

async fn load_live_workspaces(pool: &PgPool) -> Result<HashSet<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT id FROM workspaces")
        .fetch_all(pool)
        .await
        .map(|ids: Vec<Uuid>| ids.into_iter().collect())
}

async fn load_live_documents(pool: &PgPool) -> Result<HashSet<(Uuid, Uuid)>, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT workspace_id, id
        FROM documents
        WHERE workspace_id IS NOT NULL
        "#,
    )
    .fetch_all(pool)
    .await
    .map(|rows: Vec<(Uuid, Uuid)>| rows.into_iter().collect())
}

async fn load_live_object_keys(pool: &PgPool) -> Result<HashSet<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT object_key FROM documents")
        .fetch_all(pool)
        .await
        .map(|keys: Vec<String>| keys.into_iter().collect())
}

async fn load_owed_vectors(pool: &PgPool) -> Result<Vec<OwedVectorTarget>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT event_type, payload, status
        FROM qdrant_outbox
        WHERE status <> 'PROCESSED'
        ORDER BY created_at ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut owed = Vec::new();
    for row in rows {
        let event_type: String = row.get("event_type");
        let payload: Value = row.get("payload");
        let status: String = row.get("status");
        let Some(parsed) = QdrantOutboxEventType::parse(&event_type) else {
            continue;
        };
        match parsed {
            QdrantOutboxEventType::DeleteByDocument => {
                if let (Some(workspace_id), Some(document_id)) = (
                    parse_uuid_field(&payload, "workspace_id"),
                    parse_uuid_field(&payload, "document_id"),
                ) {
                    owed.push(OwedVectorTarget::Document {
                        workspace_id,
                        document_id,
                        status,
                    });
                }
            }
            QdrantOutboxEventType::DeleteByWorkspace => {
                if let Some(workspace_id) = parse_uuid_field(&payload, "workspace_id") {
                    owed.push(OwedVectorTarget::Workspace {
                        workspace_id,
                        status,
                    });
                }
            }
            QdrantOutboxEventType::DeleteByWorkspaces => {
                let workspace_ids = payload
                    .get("workspace_ids")
                    .and_then(|value| value.as_array())
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(|value| {
                                value.as_str().and_then(|raw| Uuid::parse_str(raw).ok())
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                owed.push(OwedVectorTarget::Workspaces {
                    workspace_ids,
                    status,
                });
            }
        }
    }
    Ok(owed)
}

async fn load_owed_objects(pool: &PgPool) -> Result<Vec<OwedObjectTarget>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT event_type, payload, status
        FROM storage_outbox
        WHERE status <> 'PROCESSED'
        ORDER BY created_at ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut owed = Vec::new();
    for row in rows {
        let event_type: String = row.get("event_type");
        let payload: Value = row.get("payload");
        let status: String = row.get("status");
        let Some(parsed) = StorageOutboxEventType::parse(&event_type) else {
            continue;
        };
        match parsed {
            StorageOutboxEventType::DeleteObject => {
                if let Some(object_key) = payload
                    .get("object_key")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
                {
                    owed.push(OwedObjectTarget::Object { object_key, status });
                }
            }
            StorageOutboxEventType::DeletePrefix => {
                if let Some(prefix) = payload
                    .get("prefix")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
                {
                    owed.push(OwedObjectTarget::Prefix { prefix, status });
                }
            }
        }
    }
    Ok(owed)
}

async fn load_delete_events(pool: &PgPool) -> Result<Vec<DeleteEventRow>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT event_type, created_at, tenant_id, workspace_id, document_id
        FROM audit_events
        WHERE event_type = ANY($1::text[])
        ORDER BY created_at ASC
        "#,
    )
    .bind(DELETE_EVENT_TYPES.map(str::to_string).to_vec())
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| DeleteEventRow {
            event_type: row.get("event_type"),
            created_at: row.get("created_at"),
            tenant_id: row.get("tenant_id"),
            workspace_id: row.get("workspace_id"),
            document_id: row.get("document_id"),
        })
        .collect())
}

async fn scan_vectors(
    retrieval: &RetrievalClient,
    page_size: usize,
) -> Result<Vec<ScannedVector>, RetentionReportError> {
    let mut scanned = Vec::new();
    let mut offset: Option<Value> = None;
    loop {
        let page = retrieval
            .scroll_points_page(page_size, offset.clone())
            .await?;
        if page.points.is_empty() {
            break;
        }
        for point in &page.points {
            scanned.push(ScannedVector {
                workspace_id: point.workspace_id,
                document_id: point.document_id,
            });
        }
        match page.next_offset {
            Some(next) => offset = Some(next),
            None => break,
        }
    }
    Ok(scanned)
}

async fn scan_object_keys(storage: &StorageClient) -> Result<Vec<String>, RetentionReportError> {
    let objects = storage.list_objects(None).await?;
    Ok(objects.into_iter().map(|object| object.key).collect())
}

fn parse_uuid_field(payload: &Value, key: &str) -> Option<Uuid> {
    let value = payload.get(key)?;
    if let Some(raw) = value.as_str() {
        return Uuid::parse_str(raw).ok();
    }
    serde_json::from_value(value.clone()).ok()
}

/// Exit code operator: 0 sạch, 2 cần can thiệp, 1 lỗi (bin quyết định nhánh 1).
pub const RETENTION_EXIT_CLEAR: i32 = 0;
pub const RETENTION_EXIT_ERROR: i32 = 1;
pub const RETENTION_EXIT_ACTION_REQUIRED: i32 = 2;

/// `recovery_pending` không cần operator (worker sẽ dọn); DEAD/unrecovered/unexplained thì cần.
pub fn retention_exit_code(report: &RetentionReport) -> i32 {
    let needs_action =
        report.counts.recovery_dead + report.counts.unrecovered + report.counts.unexplained;
    if needs_action > 0 {
        RETENTION_EXIT_ACTION_REQUIRED
    } else {
        RETENTION_EXIT_CLEAR
    }
}

pub fn format_human_report(report: &RetentionReport) -> String {
    let mut out = String::new();
    out.push_str("LIFE-007 retention residue report (READ-ONLY)\n");
    out.push_str(&format!("vectors_probed={}\n", report.vectors_probed));
    out.push_str(&format!("objects_probed={}\n", report.objects_probed));
    out.push_str(&format!(
        "scanned_vector_points={}\n",
        report.scanned_vector_points
    ));
    out.push_str(&format!(
        "scanned_object_keys={}\n",
        report.scanned_object_keys
    ));
    out.push_str("counts:\n");
    out.push_str(&format!(
        "  vector_residue={}\n",
        report.counts.vector_residue
    ));
    out.push_str(&format!(
        "  object_residue={}\n",
        report.counts.object_residue
    ));
    out.push_str(&format!(
        "  recovery_pending={}\n",
        report.counts.recovery_pending
    ));
    out.push_str(&format!(
        "  recovery_dead={}\n",
        report.counts.recovery_dead
    ));
    out.push_str(&format!("  unrecovered={}\n", report.counts.unrecovered));
    out.push_str(&format!("  unexplained={}\n", report.counts.unexplained));

    if !report.vector_residue.is_empty() {
        out.push_str("vector_residue:\n");
        for residue in &report.vector_residue {
            out.push_str(&format!(
                "  workspace_id={} document_id={} workspace_live={} class={} owed={} delete_event={}\n",
                residue.workspace_id,
                residue.document_id,
                residue.workspace_live,
                residue.class,
                residue.owed_outbox_status.as_deref().unwrap_or("-"),
                describe_event(residue.delete_event.as_ref()),
            ));
        }
    }
    if report.vector_residue_truncated {
        out.push_str("  (vector_residue list truncated by sample limit; counts are complete)\n");
    }

    if !report.object_residue.is_empty() {
        out.push_str("object_residue:\n");
        for residue in &report.object_residue {
            out.push_str(&format!(
                "  object_key={} class={} owed={} delete_event={}\n",
                residue.object_key,
                residue.class,
                residue.owed_outbox_status.as_deref().unwrap_or("-"),
                describe_event(residue.delete_event.as_ref()),
            ));
        }
    }
    if report.object_residue_truncated {
        out.push_str("  (object_residue list truncated by sample limit; counts are complete)\n");
    }

    if !report.limitations.is_empty() {
        out.push_str("limitations:\n");
        for limitation in &report.limitations {
            out.push_str(&format!("  - {limitation}\n"));
        }
    }
    out.push_str(
        "READ ONLY — no vector, object, SQL row, outbox row, or audit row was modified.\n",
    );
    out
}

fn describe_event(event: Option<&DeleteEventRef>) -> String {
    event.map_or_else(
        || "-".to_string(),
        |event| format!("{}@{}", event.event_type, event.created_at),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp(day: u32) -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2026, 7, day)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
    }

    #[test]
    fn object_key_parser_extracts_canonical_ids() {
        let tenant_id = Uuid::new_v4();
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();
        let key = format!(
            "tenants/{tenant_id}/workspaces/{workspace_id}/documents/{document_id}/original.pdf"
        );

        assert_eq!(
            parse_object_key(&key),
            (Some(tenant_id), Some(workspace_id), Some(document_id))
        );
        assert_eq!(parse_object_key("loose/object.bin"), (None, None, None));
    }

    #[test]
    fn classification_separates_owed_recovery_from_silent_residue() {
        assert_eq!(classify(Some(STATUS_PENDING), true), CLASS_RECOVERY_PENDING);
        assert_eq!(classify(Some(STATUS_FAILED), false), CLASS_RECOVERY_PENDING);
        assert_eq!(classify(Some(STATUS_DEAD), true), CLASS_RECOVERY_DEAD);
        assert_eq!(classify(None, true), CLASS_UNRECOVERED);
        assert_eq!(classify(None, false), CLASS_UNEXPLAINED);
    }

    #[test]
    fn active_recovery_status_wins_over_dead() {
        let statuses = vec![STATUS_DEAD.to_string(), STATUS_FAILED.to_string()];
        assert_eq!(pick_owed_status(statuses).as_deref(), Some(STATUS_FAILED));
    }

    #[test]
    fn live_vectors_and_live_objects_are_not_residue() {
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();
        let object_key = format!("tenants/t/workspaces/{workspace_id}/{document_id}");
        let inputs = RetentionInputs {
            live_workspaces: HashSet::from([workspace_id]),
            live_documents: HashSet::from([(workspace_id, document_id)]),
            live_object_keys: HashSet::from([object_key.clone()]),
            scanned_vectors: Some(vec![ScannedVector {
                workspace_id,
                document_id,
            }]),
            scanned_object_keys: Some(vec![object_key]),
            ..RetentionInputs::default()
        };

        let report = build_retention_report(&inputs, 50);

        assert_eq!(report.counts.vector_residue, 0);
        assert_eq!(report.counts.object_residue, 0);
        assert_eq!(report.scanned_vector_points, 1);
        assert_eq!(retention_exit_code(&report), RETENTION_EXIT_CLEAR);
    }

    #[test]
    fn workspace_scoped_owed_row_covers_its_document_vectors() {
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();
        let inputs = RetentionInputs {
            scanned_vectors: Some(vec![ScannedVector {
                workspace_id,
                document_id,
            }]),
            owed_vectors: vec![OwedVectorTarget::Workspace {
                workspace_id,
                status: STATUS_PENDING.to_string(),
            }],
            delete_events: vec![DeleteEventRow {
                event_type: "workspace_deleted".to_string(),
                created_at: timestamp(1),
                tenant_id: None,
                workspace_id: Some(workspace_id),
                document_id: None,
            }],
            ..RetentionInputs::default()
        };

        let report = build_retention_report(&inputs, 50);

        assert_eq!(report.counts.vector_residue, 1);
        assert_eq!(report.counts.recovery_pending, 1);
        assert_eq!(report.vector_residue[0].class, CLASS_RECOVERY_PENDING);
        assert!(!report.vector_residue[0].workspace_live);
        assert_eq!(retention_exit_code(&report), RETENTION_EXIT_CLEAR);
    }

    #[test]
    fn prefix_owed_row_covers_nested_object_keys() {
        let tenant_id = Uuid::new_v4();
        let key = format!("tenants/{tenant_id}/workspaces/w/documents/d/original.pdf");
        let inputs = RetentionInputs {
            scanned_object_keys: Some(vec![key.clone()]),
            owed_objects: vec![OwedObjectTarget::Prefix {
                prefix: format!("tenants/{tenant_id}/"),
                status: STATUS_DEAD.to_string(),
            }],
            delete_events: vec![DeleteEventRow {
                event_type: "tenant_deleted".to_string(),
                created_at: timestamp(2),
                tenant_id: Some(tenant_id),
                workspace_id: None,
                document_id: None,
            }],
            ..RetentionInputs::default()
        };

        let report = build_retention_report(&inputs, 50);

        assert_eq!(report.counts.object_residue, 1);
        assert_eq!(report.counts.recovery_dead, 1);
        assert_eq!(report.object_residue[0].class, CLASS_RECOVERY_DEAD);
        assert_eq!(report.object_residue[0].tenant_id, Some(tenant_id));
        assert_eq!(retention_exit_code(&report), RETENTION_EXIT_ACTION_REQUIRED);
    }

    #[test]
    fn deleted_document_without_owed_row_is_unrecovered() {
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();
        let inputs = RetentionInputs {
            live_workspaces: HashSet::from([workspace_id]),
            scanned_vectors: Some(vec![ScannedVector {
                workspace_id,
                document_id,
            }]),
            delete_events: vec![DeleteEventRow {
                event_type: "document_deleted".to_string(),
                created_at: timestamp(3),
                tenant_id: None,
                workspace_id: Some(workspace_id),
                document_id: Some(document_id),
            }],
            ..RetentionInputs::default()
        };

        let report = build_retention_report(&inputs, 50);

        assert_eq!(report.counts.unrecovered, 1);
        assert!(report.vector_residue[0].workspace_live);
        assert_eq!(
            report.vector_residue[0]
                .delete_event
                .as_ref()
                .map(|event| event.event_type.as_str()),
            Some("document_deleted")
        );
    }

    #[test]
    fn residue_without_owed_row_or_delete_event_is_unexplained() {
        let inputs = RetentionInputs {
            scanned_object_keys: Some(vec!["stray/object.bin".to_string()]),
            ..RetentionInputs::default()
        };

        let report = build_retention_report(&inputs, 50);

        assert_eq!(report.counts.unexplained, 1);
        assert_eq!(report.object_residue[0].class, CLASS_UNEXPLAINED);
        assert_eq!(retention_exit_code(&report), RETENTION_EXIT_ACTION_REQUIRED);
    }

    #[test]
    fn unprobed_stores_report_unknown_rather_than_clean() {
        let report = build_retention_report(&RetentionInputs::default(), 50);

        assert!(!report.vectors_probed);
        assert!(!report.objects_probed);
        assert!(
            report
                .limitations
                .iter()
                .any(|limitation| limitation.contains("Vectors were not probed"))
        );
        assert!(
            report
                .limitations
                .iter()
                .any(|limitation| limitation.contains("Objects were not probed"))
        );
    }

    #[test]
    fn sample_limit_truncates_list_but_keeps_full_counts() {
        let keys: Vec<String> = (0..5).map(|index| format!("stray/{index}.bin")).collect();
        let inputs = RetentionInputs {
            scanned_object_keys: Some(keys),
            ..RetentionInputs::default()
        };

        let report = build_retention_report(&inputs, 2);

        assert_eq!(report.counts.object_residue, 5);
        assert_eq!(report.object_residue.len(), 2);
        assert!(report.object_residue_truncated);
        assert_eq!(report.by_class[CLASS_UNEXPLAINED], 5);
    }

    #[test]
    fn multiple_points_of_one_document_collapse_to_one_finding() {
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();
        let inputs = RetentionInputs {
            scanned_vectors: Some(vec![
                ScannedVector {
                    workspace_id,
                    document_id,
                },
                ScannedVector {
                    workspace_id,
                    document_id,
                },
            ]),
            ..RetentionInputs::default()
        };

        let report = build_retention_report(&inputs, 50);

        assert_eq!(report.scanned_vector_points, 2);
        assert_eq!(report.counts.vector_residue, 1);
    }
}
