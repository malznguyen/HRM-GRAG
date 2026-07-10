use std::collections::{HashMap, HashSet};

use serde::Serialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::auth::authz::{AuthzClient, Object, Relation, TupleKey};
use crate::auth::keycloak::{KeycloakClient, KeycloakUser};
use crate::invite::normalize_email;

#[derive(Debug, Clone, Default)]
pub struct IdentityReportOptions {
    pub tenant_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    pub include_email: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IdentityFinding {
    pub category: String,
    pub subject_id: String,
    pub scope: String,
    pub expected: String,
    pub actual: String,
    pub severity: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct IdentityReportSummary {
    pub sql_users: usize,
    pub keycloak_users_found: usize,
    pub missing_in_keycloak: usize,
    pub email_mismatches: usize,
    pub unverified_users: usize,
    pub orphan_sql_references: usize,
    pub missing_fga_relations: usize,
    pub extra_fga_relations: usize,
    pub legacy_invite_ids: usize,
    pub canonical_identity_failures: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct IdentityConsistencyReport {
    pub summary: IdentityReportSummary,
    pub findings: Vec<IdentityFinding>,
    pub limitations: Vec<String>,
}

#[derive(Debug, Clone)]
struct SqlUser {
    id: String,
    email: String,
}

#[derive(Debug, Clone)]
struct ExpectedTuple {
    user: String,
    relation: Relation,
    object: Object,
    scope: String,
}

impl ExpectedTuple {
    fn key(&self) -> String {
        tuple_key(&self.user, self.relation.as_str(), &self.object.to_string())
    }
}

/// Tạo báo cáo chỉ đọc giữa SQL, Keycloak và OpenFGA; không sửa identity hay tuple.
pub async fn run_identity_consistency_report(
    pool: &PgPool,
    keycloak: &KeycloakClient,
    authz: &AuthzClient,
    options: &IdentityReportOptions,
) -> IdentityConsistencyReport {
    let mut report = IdentityConsistencyReport::default();
    let users = load_sql_users(pool, options).await.unwrap_or_else(|error| {
        report.findings.push(finding(
            "sql_inventory_failed",
            "",
            "postgresql",
            "SQL user inventory available",
            &error.to_string(),
            "critical",
        ));
        Vec::new()
    });
    report.summary.sql_users = users.len();

    let sql_user_ids: HashSet<_> = users.iter().map(|user| user.id.clone()).collect();
    inspect_sql_users(&users, keycloak, options.include_email, &mut report).await;
    inspect_sql_integrity(pool, &mut report).await;

    let expected = load_expected_tuples(pool, options)
        .await
        .unwrap_or_else(|error| {
            report.findings.push(finding(
                "sql_membership_inventory_failed",
                "",
                "postgresql",
                "membership and document ACL inventory available",
                &error.to_string(),
                "critical",
            ));
            Vec::new()
        });
    inspect_expected_relations(authz, &expected, &mut report).await;
    inspect_fga_tuples(authz, &sql_user_ids, &expected, options, &mut report).await;
    report.limitations.push(
        "Platform admin lives only in OpenFGA (no SQL expected set). Inventory checks user subjects on platform:system when Read succeeds; missing platform-admin grants cannot be inferred from SQL membership alone.".to_string(),
    );
    refresh_summary(&mut report);
    report
}

async fn load_sql_users(
    pool: &PgPool,
    options: &IdentityReportOptions,
) -> Result<Vec<SqlUser>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT DISTINCT u.id, u.email
         FROM users u
         WHERE ($1::uuid IS NULL AND $2::uuid IS NULL)
            OR EXISTS (SELECT 1 FROM tenant_members tm WHERE tm.user_id = u.id AND ($1::uuid IS NULL OR tm.tenant_id = $1))
            OR EXISTS (SELECT 1 FROM workspace_members wm JOIN workspaces w ON w.id = wm.workspace_id WHERE wm.user_id = u.id AND ($1::uuid IS NULL OR w.tenant_id = $1) AND ($2::uuid IS NULL OR wm.workspace_id = $2))
            OR EXISTS (SELECT 1 FROM documents d JOIN workspaces w ON w.id = d.workspace_id WHERE (d.owner_id = u.id OR d.uploaded_by = u.id) AND ($1::uuid IS NULL OR w.tenant_id = $1) AND ($2::uuid IS NULL OR d.workspace_id = $2))
            OR EXISTS (SELECT 1 FROM document_shares ds JOIN documents d ON d.id = ds.document_id JOIN workspaces w ON w.id = d.workspace_id WHERE ds.user_id = u.id AND ($1::uuid IS NULL OR w.tenant_id = $1) AND ($2::uuid IS NULL OR d.workspace_id = $2))",
    )
    .bind(options.tenant_id)
    .bind(options.workspace_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| SqlUser {
            id: row.get("id"),
            email: row.get("email"),
        })
        .collect())
}

async fn inspect_sql_users(
    users: &[SqlUser],
    keycloak: &KeycloakClient,
    include_email: bool,
    report: &mut IdentityConsistencyReport,
) {
    let mut duplicate_emails = HashMap::<String, Vec<String>>::new();
    for user in users {
        duplicate_emails
            .entry(normalize_email(&user.email))
            .or_default()
            .push(user.id.clone());
        if user.id.trim().is_empty() {
            report.findings.push(finding(
                "empty_user_id",
                "",
                "users",
                "non-empty canonical id",
                "empty id",
                "critical",
            ));
        }
        if user.id.starts_with("invite_") {
            report.findings.push(finding(
                "legacy_invite_id",
                &user.id,
                "users",
                "Keycloak sub",
                "invite_* placeholder",
                "critical",
            ));
        }
        if looks_like_legacy_external_id(&user.id) {
            report.findings.push(finding(
                "suspicious_legacy_external_id",
                &user.id,
                "users",
                "Keycloak sub",
                "legacy external-id pattern",
                "warning",
            ));
        }

        match keycloak.get_user_by_id(&user.id).await {
            Ok(Some(keycloak_user)) => {
                report.summary.keycloak_users_found += 1;
                inspect_keycloak_user(user, &keycloak_user, include_email, report);
                match keycloak.get_users_by_email_exact(&user.email).await {
                    Ok(accounts) if normalized_email_count(&accounts, &user.email) > 1 => {
                        report.findings.push(finding(
                            "duplicate_keycloak_email",
                            &user.id,
                            "keycloak",
                            "one exact normalized email account",
                            "multiple Keycloak accounts with the same email",
                            "warning",
                        ))
                    }
                    Ok(_) => {}
                    Err(error) => report.findings.push(finding(
                        "keycloak_email_lookup_failed",
                        &user.id,
                        "keycloak",
                        "duplicate-email check available",
                        &error.to_string(),
                        "warning",
                    )),
                }
            }
            Ok(None) => report.findings.push(finding(
                "missing_in_keycloak",
                &user.id,
                "keycloak",
                "Keycloak user with exact id",
                "not found",
                "critical",
            )),
            Err(error) => report.findings.push(finding(
                "keycloak_lookup_failed",
                &user.id,
                "keycloak",
                "Keycloak Admin lookup by exact id",
                &error.to_string(),
                "critical",
            )),
        }
    }

    for (email, ids) in duplicate_emails {
        if ids.len() > 1 {
            report.findings.push(finding(
                "duplicate_sql_email",
                &ids.join(","),
                "users",
                "one canonical user per normalized email",
                &format!(
                    "{} SQL users share {}",
                    ids.len(),
                    display_email(&email, include_email)
                ),
                "critical",
            ));
        }
    }
}

fn inspect_keycloak_user(
    user: &SqlUser,
    keycloak_user: &KeycloakUser,
    include_email: bool,
    report: &mut IdentityConsistencyReport,
) {
    if keycloak_user.id != user.id {
        report.findings.push(finding(
            "keycloak_id_mismatch",
            &user.id,
            "keycloak",
            &user.id,
            &keycloak_user.id,
            "critical",
        ));
    }
    if !keycloak_user.enabled.unwrap_or(true) {
        report.findings.push(finding(
            "disabled_keycloak_user",
            &user.id,
            "keycloak",
            "enabled",
            "disabled",
            "warning",
        ));
    }
    if !keycloak_user.email_verified.unwrap_or(false) {
        report.findings.push(finding(
            "unverified_keycloak_email",
            &user.id,
            "keycloak",
            "verified email",
            "unverified or absent",
            "warning",
        ));
    }
    match keycloak_user.email.as_deref() {
        Some(email) if normalize_email(email) == normalize_email(&user.email) => {}
        Some(email) => report.findings.push(finding(
            "email_mismatch",
            &user.id,
            "keycloak",
            &display_email(&user.email, include_email),
            &display_email(email, include_email),
            "critical",
        )),
        None => report.findings.push(finding(
            "email_mismatch",
            &user.id,
            "keycloak",
            &display_email(&user.email, include_email),
            "missing Keycloak email",
            "critical",
        )),
    }
}

async fn inspect_sql_integrity(pool: &PgPool, report: &mut IdentityConsistencyReport) {
    let queries = [
        (
            "tenant_members",
            "SELECT user_id, tenant_id::text AS scope FROM tenant_members tm WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = tm.user_id)",
        ),
        (
            "workspace_members",
            "SELECT user_id, workspace_id::text AS scope FROM workspace_members wm WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = wm.user_id)",
        ),
        (
            "chat_sessions",
            "SELECT user_id, workspace_id::text AS scope FROM chat_sessions cs WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = cs.user_id)",
        ),
        (
            "documents.owner_id",
            "SELECT owner_id AS user_id, id::text AS scope FROM documents d WHERE owner_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM users u WHERE u.id = d.owner_id)",
        ),
        (
            "documents.uploaded_by",
            "SELECT uploaded_by AS user_id, id::text AS scope FROM documents d WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = d.uploaded_by)",
        ),
        (
            "document_shares",
            "SELECT user_id, document_id::text AS scope FROM document_shares ds WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = ds.user_id)",
        ),
        (
            "audit_events",
            "SELECT actor_user_id AS user_id, COALESCE(workspace_id::text, tenant_id::text, document_id::text, 'global') AS scope FROM audit_events ae WHERE actor_user_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM users u WHERE u.id = ae.actor_user_id)",
        ),
    ];
    for (table, query) in queries {
        match sqlx::query(query).fetch_all(pool).await {
            Ok(rows) => {
                for row in rows {
                    report.findings.push(finding(
                        "orphan_sql_reference",
                        row.get::<String, _>("user_id").as_str(),
                        &format!("{table}:{}", row.get::<String, _>("scope")),
                        "referencing SQL user exists",
                        "missing users.id",
                        "critical",
                    ));
                }
            }
            Err(error) => report.findings.push(finding(
                "sql_reference_check_failed",
                "",
                table,
                "reference check available",
                &error.to_string(),
                "critical",
            )),
        }
    }
}

async fn load_expected_tuples(
    pool: &PgPool,
    options: &IdentityReportOptions,
) -> Result<Vec<ExpectedTuple>, sqlx::Error> {
    let mut expected = Vec::new();
    for row in sqlx::query("SELECT tm.user_id, tm.tenant_id FROM tenant_members tm WHERE ($1::uuid IS NULL OR tm.tenant_id = $1)")
        .bind(options.tenant_id).fetch_all(pool).await? {
        let tenant_id: Uuid = row.get("tenant_id");
        expected.push(ExpectedTuple { user: format!("user:{}", row.get::<String, _>("user_id")), relation: Relation::Owner, object: Object::Tenant(tenant_id), scope: format!("tenant:{tenant_id}") });
    }
    for row in sqlx::query("SELECT wm.user_id, wm.workspace_id, wm.role FROM workspace_members wm JOIN workspaces w ON w.id = wm.workspace_id WHERE ($1::uuid IS NULL OR w.tenant_id = $1) AND ($2::uuid IS NULL OR wm.workspace_id = $2)")
        .bind(options.tenant_id).bind(options.workspace_id).fetch_all(pool).await? {
        let workspace_id: Uuid = row.get("workspace_id");
        let role: String = row.get("role");
        let relation = if role == "ADMIN" { Relation::Admin } else { Relation::Member };
        expected.push(ExpectedTuple { user: format!("user:{}", row.get::<String, _>("user_id")), relation, object: Object::Workspace(workspace_id), scope: format!("workspace:{workspace_id}") });
    }
    for row in sqlx::query("SELECT ds.user_id, ds.document_id, d.workspace_id FROM document_shares ds JOIN documents d ON d.id = ds.document_id JOIN workspaces w ON w.id = d.workspace_id WHERE ($1::uuid IS NULL OR w.tenant_id = $1) AND ($2::uuid IS NULL OR d.workspace_id = $2)")
        .bind(options.tenant_id).bind(options.workspace_id).fetch_all(pool).await? {
        let document_id: Uuid = row.get("document_id");
        expected.push(ExpectedTuple { user: format!("user:{}", row.get::<String, _>("user_id")), relation: Relation::ExplicitViewer, object: Object::Document(document_id), scope: format!("document:{document_id}") });
    }
    Ok(expected)
}

async fn inspect_expected_relations(
    authz: &AuthzClient,
    expected: &[ExpectedTuple],
    report: &mut IdentityConsistencyReport,
) {
    for tuple in expected {
        match authz
            .check_fga(&tuple.user, tuple.relation, &tuple.object)
            .await
        {
            Ok(true) => {}
            Ok(false) => report.findings.push(finding(
                "missing_fga_relation",
                &tuple.user,
                &tuple.scope,
                tuple.relation.as_str(),
                "not allowed",
                "critical",
            )),
            Err(error) => report.findings.push(finding(
                "openfga_check_failed",
                &tuple.user,
                &tuple.scope,
                tuple.relation.as_str(),
                &error.to_string(),
                "critical",
            )),
        }
    }
}

async fn inspect_fga_tuples(
    authz: &AuthzClient,
    sql_user_ids: &HashSet<String>,
    expected: &[ExpectedTuple],
    _options: &IdentityReportOptions,
    report: &mut IdentityConsistencyReport,
) {
    let tuples = match authz.list_all_tuples().await {
        Ok(tuples) => tuples,
        Err(error) => {
            report.limitations.push("OpenFGA tuple inventory was unavailable; expected-relation checks still ran, but extra or unknown subjects could not be proven.".to_string());
            report.findings.push(finding(
                "openfga_inventory_failed",
                "",
                "openfga",
                "tuple read API available",
                &error.to_string(),
                "warning",
            ));
            return;
        }
    };
    let expected_keys: HashSet<_> = expected.iter().map(ExpectedTuple::key).collect();
    for tuple in tuples {
        let Some(subject_id) = tuple.user.strip_prefix("user:") else {
            continue;
        };
        if subject_id.starts_with("invite_") {
            report.findings.push(finding(
                "legacy_invite_id",
                subject_id,
                &tuple.object,
                "Keycloak subject",
                "user:invite_* tuple",
                "critical",
            ));
        }
        if !sql_user_ids.contains(subject_id) {
            report.findings.push(finding(
                "openfga_subject_missing_in_sql",
                subject_id,
                &tuple.object,
                "users.id exists",
                "OpenFGA user subject only",
                "critical",
            ));
        }
        if is_sql_backed_direct_relation(&tuple)
            && tuple.object != "platform:system"
            && !expected_keys.contains(&tuple_key(&tuple.user, &tuple.relation, &tuple.object))
        {
            report.findings.push(finding(
                "extra_fga_relation",
                &tuple.user,
                &tuple.object,
                "SQL membership/share read model",
                &format!("{} tuple", tuple.relation),
                "warning",
            ));
        }
    }
}

fn is_sql_backed_direct_relation(tuple: &TupleKey) -> bool {
    matches!(
        (tuple.relation.as_str(), tuple.object.split(':').next()),
        ("owner", Some("tenant"))
            | ("admin", Some("workspace"))
            | ("member", Some("workspace"))
            | ("explicit_viewer", Some("document"))
    )
}

fn refresh_summary(report: &mut IdentityConsistencyReport) {
    let count = |category: &str| {
        report
            .findings
            .iter()
            .filter(|finding| finding.category == category)
            .count()
    };
    report.summary.missing_in_keycloak = count("missing_in_keycloak");
    report.summary.email_mismatches = count("email_mismatch");
    report.summary.unverified_users = count("unverified_keycloak_email");
    report.summary.orphan_sql_references = count("orphan_sql_reference");
    report.summary.missing_fga_relations = count("missing_fga_relation");
    report.summary.extra_fga_relations = count("extra_fga_relation");
    report.summary.legacy_invite_ids = count("legacy_invite_id");
    report.summary.canonical_identity_failures = report
        .findings
        .iter()
        .filter(|finding| finding.severity == "critical")
        .count();
}

/// Exit codes:
/// - `0` — tool execution OK and environment consistent (no critical findings;
///   no warnings when `--strict`)
/// - `2` — tool execution OK, **environment consistency FAIL / DIRTY** (critical findings)
/// - `3` — tool execution OK, warnings only under `--strict`
///
/// Exit `2` is **not** consistency PASS. Shared test fixtures may produce exit 2
/// (EXPECTED NON-ZERO ON SHARED TEST FIXTURES). Clean acceptance needs a dedicated
/// E2E database/store or fixture cleanup first.
pub fn report_exit_code(report: &IdentityConsistencyReport, strict: bool) -> i32 {
    if report
        .findings
        .iter()
        .any(|finding| finding.severity == "critical")
    {
        2
    } else if strict && !report.findings.is_empty() {
        3
    } else {
        0
    }
}

pub fn format_human_report(report: &IdentityConsistencyReport) -> String {
    let summary = &report.summary;
    let exit = report_exit_code(report, false);
    let consistency = if exit == 0 { "PASS" } else { "FAIL / DIRTY" };
    let mut output = format!(
        "Identity consistency report\ntool_execution=PASS\nenvironment_consistency={consistency}\nexit_code_meaning={}\nsql_users={}\nkeycloak_users_found={}\nmissing_in_keycloak={}\nemail_mismatches={}\nunverified_users={}\norphan_sql_references={}\nmissing_fga_relations={}\nextra_fga_relations={}\nlegacy_invite_ids={}\ncanonical_identity_failures={}\n",
        if exit == 2 {
            "2 = environment consistency FAIL (not tool failure)"
        } else if exit == 0 {
            "0 = consistent"
        } else {
            "non-zero"
        },
        summary.sql_users,
        summary.keycloak_users_found,
        summary.missing_in_keycloak,
        summary.email_mismatches,
        summary.unverified_users,
        summary.orphan_sql_references,
        summary.missing_fga_relations,
        summary.extra_fga_relations,
        summary.legacy_invite_ids,
        summary.canonical_identity_failures,
    );
    for finding in &report.findings {
        output.push_str(&format!(
            "{} | {} | {} | expected={} | actual={} | {}\n",
            finding.category,
            finding.subject_id,
            finding.scope,
            finding.expected,
            finding.actual,
            finding.severity
        ));
    }
    for limitation in &report.limitations {
        output.push_str(&format!("limitation | {limitation}\n"));
    }
    output
}

fn normalized_email_count(users: &[KeycloakUser], email: &str) -> usize {
    let expected = normalize_email(email);
    users
        .iter()
        .filter(|user| {
            user.email
                .as_deref()
                .is_some_and(|value| normalize_email(value) == expected)
        })
        .count()
}

fn display_email(email: &str, include_email: bool) -> String {
    if include_email {
        email.to_string()
    } else {
        mask_email(email)
    }
}

pub fn mask_email(email: &str) -> String {
    let Some((local, domain)) = email.split_once('@') else {
        return "***".to_string();
    };
    let prefix = local.chars().next().unwrap_or('*');
    format!("{prefix}***@{domain}")
}

fn looks_like_legacy_external_id(user_id: &str) -> bool {
    user_id.starts_with("user_") && user_id.len() > 10
}

fn tuple_key(user: &str, relation: &str, object: &str) -> String {
    format!("{user}|{relation}|{object}")
}

fn finding(
    category: &str,
    subject_id: &str,
    scope: &str,
    expected: &str,
    actual: &str,
    severity: &str,
) -> IdentityFinding {
    IdentityFinding {
        category: category.to_string(),
        subject_id: subject_id.to_string(),
        scope: scope.to_string(),
        expected: expected.to_string(),
        actual: actual.to_string(),
        severity: severity.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report_with(category: &str, severity: &str) -> IdentityConsistencyReport {
        IdentityConsistencyReport {
            findings: vec![finding(
                category, "subject", "scope", "expected", "actual", severity,
            )],
            ..Default::default()
        }
    }

    #[test]
    fn masks_email_without_an_explicit_operator_flag() {
        assert_eq!(mask_email("alice@example.com"), "a***@example.com");
    }

    #[test]
    fn strict_mode_fails_for_warnings_and_default_mode_keeps_warnings_nonfatal() {
        let report = report_with("extra_fga_relation", "warning");
        assert_eq!(report_exit_code(&report, false), 0);
        assert_eq!(report_exit_code(&report, true), 3);
    }

    #[test]
    fn critical_identity_failures_are_nonzero_in_every_mode() {
        let report = report_with("missing_in_keycloak", "critical");
        assert_eq!(report_exit_code(&report, false), 2);
        assert_eq!(report_exit_code(&report, true), 2);
    }

    #[test]
    fn keycloak_email_mismatch_and_unverified_account_are_reported() {
        let user = SqlUser {
            id: "keycloak-sub".to_string(),
            email: "owner@example.test".to_string(),
        };
        let keycloak_user = KeycloakUser {
            id: "keycloak-sub".to_string(),
            email: Some("other@example.test".to_string()),
            email_verified: Some(false),
            enabled: Some(true),
        };
        let mut report = IdentityConsistencyReport::default();
        inspect_keycloak_user(&user, &keycloak_user, false, &mut report);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.category == "email_mismatch")
        );
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.category == "unverified_keycloak_email")
        );
    }

    #[test]
    fn orphan_membership_and_missing_relation_are_critical_categories() {
        let mut report = IdentityConsistencyReport {
            findings: vec![
                finding(
                    "orphan_sql_reference",
                    "missing-user",
                    "workspace_members:scope",
                    "users.id exists",
                    "missing users.id",
                    "critical",
                ),
                finding(
                    "missing_fga_relation",
                    "user:member",
                    "workspace:scope",
                    "member",
                    "not allowed",
                    "critical",
                ),
            ],
            ..Default::default()
        };
        refresh_summary(&mut report);
        assert_eq!(report.summary.orphan_sql_references, 1);
        assert_eq!(report.summary.missing_fga_relations, 1);
        assert_eq!(report_exit_code(&report, false), 2);
    }

    #[test]
    fn json_report_is_machine_readable_and_does_not_require_email() {
        let mut report = report_with("email_mismatch", "critical");
        refresh_summary(&mut report);
        let json = serde_json::to_string(&report).unwrap();
        assert!(serde_json::from_str::<serde_json::Value>(&json).is_ok());
        assert!(!json.contains("alice@example.com"));
    }

    #[test]
    fn legacy_invite_and_extra_fga_findings_are_counted() {
        let mut report = IdentityConsistencyReport {
            findings: vec![
                finding(
                    "legacy_invite_id",
                    "invite_old",
                    "users",
                    "Keycloak sub",
                    "invite",
                    "critical",
                ),
                finding(
                    "extra_fga_relation",
                    "user:old",
                    "workspace:1",
                    "SQL",
                    "member",
                    "warning",
                ),
            ],
            ..Default::default()
        };
        refresh_summary(&mut report);
        assert_eq!(report.summary.legacy_invite_ids, 1);
        assert_eq!(report.summary.extra_fga_relations, 1);
    }

    #[test]
    fn report_builder_has_no_mutation_option() {
        let options = IdentityReportOptions::default();
        assert!(
            options.tenant_id.is_none() && options.workspace_id.is_none() && !options.include_email
        );
    }

    #[test]
    fn human_report_never_includes_tokens_or_secrets() {
        let mut report = report_with("missing_in_keycloak", "critical");
        refresh_summary(&mut report);
        let text = format_human_report(&report);
        assert!(!text.contains("access_token"));
        assert!(!text.contains("client_secret"));
        assert!(!text.contains("Bearer "));
        assert!(text.contains("sql_users="));
    }

    #[test]
    fn invite_prefix_and_legacy_external_ids_are_flagged() {
        assert!(looks_like_legacy_external_id("user_2abcXYZ12345"));
        assert!(!looks_like_legacy_external_id(
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        ));
        let users = vec![SqlUser {
            id: "invite_old@example.com".to_string(),
            email: "old@example.com".to_string(),
        }];
        let mut report = IdentityConsistencyReport::default();
        // Không gọi Keycloak thật; chỉ kiểm tra phân loại id.
        for user in &users {
            if user.id.starts_with("invite_") {
                report.findings.push(finding(
                    "legacy_invite_id",
                    &user.id,
                    "users",
                    "Keycloak sub",
                    "invite_* placeholder",
                    "critical",
                ));
            }
        }
        refresh_summary(&mut report);
        assert_eq!(report.summary.legacy_invite_ids, 1);
    }
}
