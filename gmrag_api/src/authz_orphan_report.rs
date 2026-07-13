use std::collections::{BTreeMap, HashSet};

use chrono::NaiveDateTime;
use serde::Serialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::auth::authz::{AuthzClient, AuthzError, TupleKey};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AuthzOrphanAuditMatch {
    pub event_type: String,
    pub created_at: NaiveDateTime,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AuthzOrphanFinding {
    pub resource_type: String,
    pub resource_id: Uuid,
    pub tuple: TupleKey,
    pub audit_matches: Vec<AuthzOrphanAuditMatch>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct AuthzOrphanSummary {
    pub orphan_tuples: usize,
    pub orphan_tuples_with_audit: usize,
    pub orphan_tuples_without_audit: usize,
    pub by_resource_type: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct AuthzOrphanReport {
    pub summary: AuthzOrphanSummary,
    pub findings: Vec<AuthzOrphanFinding>,
    pub limitations: Vec<String>,
}

#[derive(Debug)]
pub enum AuthzOrphanReportError {
    Database(sqlx::Error),
    OpenFga(AuthzError),
}

impl std::fmt::Display for AuthzOrphanReportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(_) => write!(formatter, "could not read PostgreSQL resource inventory"),
            Self::OpenFga(_) => write!(formatter, "could not read OpenFGA tuple inventory"),
        }
    }
}

impl std::error::Error for AuthzOrphanReportError {}

impl From<sqlx::Error> for AuthzOrphanReportError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

impl From<AuthzError> for AuthzOrphanReportError {
    fn from(error: AuthzError) -> Self {
        Self::OpenFga(error)
    }
}

/// Báo cáo tuple OpenFGA trỏ tới tenant/workspace/document không còn trong SQL.
/// Chỉ đọc OpenFGA và PostgreSQL; không enqueue, ghi audit, hoặc xoá tuple.
pub async fn run_authz_orphan_report(
    pool: &PgPool,
    authz: &AuthzClient,
) -> Result<AuthzOrphanReport, AuthzOrphanReportError> {
    let inventory = load_resource_inventory(pool).await?;
    let audit_index = load_audit_index(pool).await?;
    let tuples = authz.list_all_tuples().await?;
    Ok(build_report(tuples, &inventory, &audit_index))
}

struct ResourceInventory {
    tenant_ids: HashSet<Uuid>,
    workspace_ids: HashSet<Uuid>,
    document_ids: HashSet<Uuid>,
}

#[derive(Debug, Clone)]
struct AuditRow {
    event_type: String,
    created_at: NaiveDateTime,
    tenant_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
    document_id: Option<Uuid>,
    target_type: Option<String>,
    target_id: Option<String>,
}

async fn load_resource_inventory(pool: &PgPool) -> Result<ResourceInventory, sqlx::Error> {
    let tenant_ids = load_uuid_set(pool, "SELECT id FROM tenants").await?;
    let workspace_ids = load_uuid_set(pool, "SELECT id FROM workspaces").await?;
    let document_ids = load_uuid_set(pool, "SELECT id FROM documents").await?;
    Ok(ResourceInventory {
        tenant_ids,
        workspace_ids,
        document_ids,
    })
}

async fn load_uuid_set(pool: &PgPool, query: &'static str) -> Result<HashSet<Uuid>, sqlx::Error> {
    sqlx::query_scalar(query)
        .fetch_all(pool)
        .await
        .map(|ids: Vec<Uuid>| ids.into_iter().collect())
}

async fn load_audit_index(pool: &PgPool) -> Result<Vec<AuditRow>, sqlx::Error> {
    sqlx::query(
        r#"
        SELECT event_type, created_at, tenant_id, workspace_id, document_id, target_type, target_id
        FROM audit_events
        WHERE tenant_id IS NOT NULL
           OR workspace_id IS NOT NULL
           OR document_id IS NOT NULL
           OR (target_type IS NOT NULL AND target_id IS NOT NULL)
        ORDER BY created_at ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| AuditRow {
                event_type: row.get("event_type"),
                created_at: row.get("created_at"),
                tenant_id: row.get("tenant_id"),
                workspace_id: row.get("workspace_id"),
                document_id: row.get("document_id"),
                target_type: row.get("target_type"),
                target_id: row.get("target_id"),
            })
            .collect()
    })
}

fn build_report(
    tuples: Vec<TupleKey>,
    inventory: &ResourceInventory,
    audit_rows: &[AuditRow],
) -> AuthzOrphanReport {
    let mut findings = Vec::new();
    for tuple in tuples {
        let Some((resource_type, resource_id)) = parse_resource_object(&tuple.object) else {
            continue;
        };
        if resource_exists(&resource_type, resource_id, inventory) {
            continue;
        }
        let audit_matches = audit_rows
            .iter()
            .filter(|audit| audit_matches_resource(audit, &resource_type, resource_id))
            .map(|audit| AuthzOrphanAuditMatch {
                event_type: audit.event_type.clone(),
                created_at: audit.created_at,
                target_type: audit.target_type.clone(),
                target_id: audit.target_id.clone(),
            })
            .collect();
        findings.push(AuthzOrphanFinding {
            resource_type,
            resource_id,
            tuple,
            audit_matches,
        });
    }
    findings.sort_by(|left, right| {
        (
            &left.resource_type,
            left.resource_id,
            &left.tuple.relation,
            &left.tuple.user,
        )
            .cmp(&(
                &right.resource_type,
                right.resource_id,
                &right.tuple.relation,
                &right.tuple.user,
            ))
    });

    let mut summary = AuthzOrphanSummary {
        orphan_tuples: findings.len(),
        ..AuthzOrphanSummary::default()
    };
    for finding in &findings {
        *summary
            .by_resource_type
            .entry(finding.resource_type.clone())
            .or_default() += 1;
        if finding.audit_matches.is_empty() {
            summary.orphan_tuples_without_audit += 1;
        } else {
            summary.orphan_tuples_with_audit += 1;
        }
    }

    AuthzOrphanReport {
        summary,
        findings,
        limitations: vec![
            "Audit correlation uses audit_events tenant_id/workspace_id/document_id or an exact target_type plus target_id match. It does not infer causality from arbitrary JSON metadata.".to_string(),
            "A matching audit row shows that the resource ID appeared in an audited operation; it does not by itself prove that operation caused the orphan tuple.".to_string(),
        ],
    }
}

fn parse_resource_object(object: &str) -> Option<(String, Uuid)> {
    let (resource_type, raw_id) = object.split_once(':')?;
    let resource_type = match resource_type {
        "tenant" | "workspace" | "document" => resource_type,
        _ => return None,
    };
    Uuid::parse_str(raw_id)
        .ok()
        .map(|resource_id| (resource_type.to_string(), resource_id))
}

fn resource_exists(resource_type: &str, resource_id: Uuid, inventory: &ResourceInventory) -> bool {
    match resource_type {
        "tenant" => inventory.tenant_ids.contains(&resource_id),
        "workspace" => inventory.workspace_ids.contains(&resource_id),
        "document" => inventory.document_ids.contains(&resource_id),
        _ => false,
    }
}

fn audit_matches_resource(audit: &AuditRow, resource_type: &str, resource_id: Uuid) -> bool {
    let resource_id_text = resource_id.to_string();
    match resource_type {
        "tenant" if audit.tenant_id == Some(resource_id) => true,
        "workspace" if audit.workspace_id == Some(resource_id) => true,
        "document" if audit.document_id == Some(resource_id) => true,
        _ => {
            audit.target_type.as_deref() == Some(resource_type)
                && audit.target_id.as_deref() == Some(resource_id_text.as_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inventory() -> ResourceInventory {
        ResourceInventory {
            tenant_ids: HashSet::new(),
            workspace_ids: HashSet::new(),
            document_ids: HashSet::new(),
        }
    }

    #[test]
    fn report_groups_missing_resources_and_correlates_exact_audit_columns() {
        let workspace_id = Uuid::new_v4();
        let document_id = Uuid::new_v4();
        let tuples = vec![
            TupleKey {
                user: "tenant:test".to_string(),
                relation: "tenant".to_string(),
                object: format!("workspace:{workspace_id}"),
            },
            TupleKey {
                user: "workspace:test".to_string(),
                relation: "workspace".to_string(),
                object: format!("document:{document_id}"),
            },
            TupleKey {
                user: "user:admin".to_string(),
                relation: "admin".to_string(),
                object: "platform:system".to_string(),
            },
        ];
        let audits = vec![AuditRow {
            event_type: "workspace_deleted".to_string(),
            created_at: NaiveDateTime::default(),
            tenant_id: None,
            workspace_id: Some(workspace_id),
            document_id: None,
            target_type: Some("workspace".to_string()),
            target_id: Some(workspace_id.to_string()),
        }];

        let report = build_report(tuples, &inventory(), &audits);

        assert_eq!(report.summary.orphan_tuples, 2);
        assert_eq!(report.summary.orphan_tuples_with_audit, 1);
        assert_eq!(report.summary.orphan_tuples_without_audit, 1);
        assert_eq!(report.summary.by_resource_type["workspace"], 1);
        assert_eq!(report.summary.by_resource_type["document"], 1);
    }
}
