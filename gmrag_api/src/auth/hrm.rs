use crate::auth::authz::{AuthzClient, AuthzError, Object, Relation, TupleKey};
use crate::auth::jwt::JwtClaims;
use crate::api_error::ApiError;
use crate::invite::normalize_email;
use crate::state::AppState;
use axum::{
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
};
use sqlx::PgPool;
use std::fmt;
use uuid::Uuid;

pub const HRM_EMPLOYEE_ROLE: &str = "EMPLOYEE";
pub const HRM_MANAGER_ROLE: &str = "MANAGER";
pub const HRM_ADMIN_ROLE: &str = "ADMIN";
pub const HRM_HR_ROLE: &str = "HR";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HrmConfig {
    pub tenant_id: Uuid,
    pub workspace_id: Uuid,
}

#[derive(Debug, PartialEq, Eq)]
pub struct HrmConfigError {
    pub key: &'static str,
    pub invalid: bool,
}

impl HrmConfig {
    pub fn from_env() -> Result<Option<Self>, HrmConfigError> {
        let enabled = match std::env::var("HRM_MODE") {
            Ok(value) if value.trim() == "true" => true,
            Ok(value) if value.trim() == "false" => false,
            Ok(_) => {
                return Err(HrmConfigError {
                    key: "HRM_MODE",
                    invalid: true,
                });
            }
            Err(std::env::VarError::NotPresent) => false,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(HrmConfigError {
                    key: "HRM_MODE",
                    invalid: true,
                });
            }
        };

        if !enabled {
            return Ok(None);
        }

        Ok(Some(Self {
            tenant_id: required_uuid("HRM_TENANT_ID")?,
            workspace_id: required_uuid("HRM_WORKSPACE_ID")?,
        }))
    }

    pub fn role_relation(role: Option<&str>) -> Result<Relation, HrmProvisionError> {
        match role {
            Some(HRM_MANAGER_ROLE | HRM_EMPLOYEE_ROLE) => Ok(Relation::Member),
            Some(HRM_ADMIN_ROLE | HRM_HR_ROLE) => Ok(Relation::Admin),
            _ => Err(HrmProvisionError::InvalidRole),
        }
    }
}

#[derive(Debug)]
pub enum HrmProvisionError {
    InvalidRole,
    Database(sqlx::Error),
    Authz(AuthzError),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TupleSyncPlan {
    pub writes: Vec<TupleKey>,
    pub deletes: Vec<TupleKey>,
}

impl fmt::Display for HrmProvisionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRole => formatter.write_str("HRM token role is not supported"),
            Self::Database(error) => write!(formatter, "HRM user upsert failed: {error}"),
            Self::Authz(error) => write!(formatter, "HRM OpenFGA sync failed: {error}"),
        }
    }
}

impl std::error::Error for HrmProvisionError {}

/// Chuỗi duy nhất được chấp nhận thay cho UUID ở vị trí `{workspace_id}`.
///
/// Cố tình chỉ nhận đúng dạng chữ thường: một alias phân biệt hoa thường là một
/// alias, còn `hrm`/`HRM`/`Hrm` cùng chạy là ba cách viết cho một thứ và sẽ trôi
/// vào log, cache key, tài liệu ở ba dạng khác nhau.
pub const HRM_WORKSPACE_ALIAS: &str = "hrm";

/// Đổi alias `hrm` ở vị trí `{workspace_id}` thành `HRM_WORKSPACE_ID`.
///
/// Trả `None` khi path không chứa alias — caller giữ nguyên URI thay vì dựng lại
/// chuỗi, nên UUID thật đi qua đây không bị đụng vào.
///
/// Chỉ đổi segment đứng **ngay sau** `workspaces`, nên `hrm` ở vị trí khác
/// (tên file, session id, `/tenants/hrm/...`) không bị ảnh hưởng.
pub fn resolve_workspace_alias_path(path: &str, config: &HrmConfig) -> Option<String> {
    let workspace_id = config.workspace_id.to_string();
    let mut segments: Vec<&str> = path.split('/').collect();
    let mut resolved = false;

    for index in 1..segments.len() {
        if segments[index - 1] == "workspaces" && segments[index] == HRM_WORKSPACE_ALIAS {
            segments[index] = workspace_id.as_str();
            resolved = true;
        }
    }

    resolved.then(|| segments.join("/"))
}

pub fn ensure_scope(path: &str, config: &HrmConfig) -> Result<(), HrmScopeError> {
    let segments: Vec<&str> = path.split('/').filter(|segment| !segment.is_empty()).collect();
    for pair in segments.windows(2) {
        let Some(candidate) = Uuid::parse_str(pair[1]).ok() else {
            continue;
        };
        match pair[0] {
            "tenants" if candidate != config.tenant_id => {
                return Err(HrmScopeError::Tenant(candidate));
            }
            "workspaces" if candidate != config.workspace_id => {
                return Err(HrmScopeError::Workspace(candidate));
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub enum HrmScopeError {
    Tenant(Uuid),
    Workspace(Uuid),
}

pub struct HrmChatPermission;

pub struct HrmDocumentUploadPermission;

pub fn has_chatbot_permission(claims: &JwtClaims) -> bool {
    claims
        .permissions
        .iter()
        .any(|permission| permission == "CHATBOT_USE")
}

impl FromRequestParts<AppState> for HrmChatPermission {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if !state.jwt.hrm_mode() {
            return Ok(Self);
        }

        let allowed = parts
            .extensions
            .get::<JwtClaims>()
            .is_some_and(has_chatbot_permission);
        if allowed {
            Ok(Self)
        } else {
            Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "CHATBOT_PERMISSION_REQUIRED",
                "CHATBOT_USE permission is required",
            ))
        }
    }
}

pub fn has_document_upload_permission(claims: &JwtClaims) -> bool {
    claims
        .permissions
        .iter()
        .any(|permission| permission == "CHATBOT_UPLOAD_DOCUMENT")
}

fn require_document_upload_permission(
    hrm_mode: bool,
    claims: Option<&JwtClaims>,
) -> Result<(), ApiError> {
    if !hrm_mode || claims.is_some_and(has_document_upload_permission) {
        return Ok(());
    }

    Err(ApiError::new(
        StatusCode::FORBIDDEN,
        "CHATBOT_UPLOAD_PERMISSION_REQUIRED",
        "CHATBOT_UPLOAD_DOCUMENT permission is required",
    ))
}

impl FromRequestParts<AppState> for HrmDocumentUploadPermission {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        require_document_upload_permission(
            state.jwt.hrm_mode(),
            parts.extensions.get::<JwtClaims>(),
        )?;
        Ok(Self)
    }
}

pub async fn provision_identity(
    pool: &PgPool,
    authz: &AuthzClient,
    config: &HrmConfig,
    claims: &JwtClaims,
) -> Result<(), HrmProvisionError> {
    let desired_relation = HrmConfig::role_relation(claims.role.as_deref())?;
    upsert_hrm_user(pool, &claims.sub, claims.email.as_deref())
        .await
        .map_err(HrmProvisionError::Database)?;

    let other_relation = match desired_relation {
        Relation::Member => Relation::Admin,
        Relation::Admin => Relation::Member,
        _ => unreachable!("HRM role relation is restricted to member/admin"),
    };
    let user = format!("user:{}", claims.sub);
    let object = Object::Workspace(config.workspace_id);
    let desired_tuple = tuple_key(&user, desired_relation, &object);
    let other_tuple = tuple_key(&user, other_relation, &object);

    let tuples = authz
        .list_all_tuples()
        .await
        .map_err(HrmProvisionError::Authz)?;
    let desired_present = tuples.iter().any(|tuple| tuple == &desired_tuple);
    let other_present = tuples.iter().any(|tuple| tuple == &other_tuple);

    let plan = plan_tuple_sync(
        &user,
        &object,
        desired_relation,
        other_relation,
        desired_present,
        other_present,
    );
    if !plan.writes.is_empty() || !plan.deletes.is_empty() {
        authz
            .write_tuples(plan.writes, plan.deletes)
            .await
            .map_err(HrmProvisionError::Authz)?;
    }

    Ok(())
}

pub fn plan_tuple_sync(
    user: &str,
    object: &Object,
    desired_relation: Relation,
    other_relation: Relation,
    desired_present: bool,
    other_present: bool,
) -> TupleSyncPlan {
    TupleSyncPlan {
        writes: (!desired_present)
            .then_some(vec![tuple_key(user, desired_relation, object)])
            .unwrap_or_default(),
        deletes: other_present
            .then_some(vec![tuple_key(user, other_relation, object)])
            .unwrap_or_default(),
    }
}

fn tuple_key(user: &str, relation: Relation, object: &Object) -> TupleKey {
    TupleKey {
        user: user.to_string(),
        relation: relation.as_str().to_string(),
        object: object.to_string(),
    }
}

async fn upsert_hrm_user(
    pool: &PgPool,
    user_id: &str,
    email: Option<&str>,
) -> Result<(), sqlx::Error> {
    if let Some(email) = email.map(normalize_email).filter(|email| !email.is_empty()) {
        sqlx::query(
            r#"
            INSERT INTO users (id, email)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
            "#,
        )
        .bind(user_id)
        .bind(email)
        .execute(pool)
        .await?;
    } else {
        let fallback_email = format!("{user_id}@hrm.local");
        sqlx::query(
            r#"
            INSERT INTO users (id, email)
            VALUES ($1, $2)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(user_id)
        .bind(fallback_email)
        .execute(pool)
        .await?;
    }
    Ok(())
}

fn required_uuid(name: &'static str) -> Result<Uuid, HrmConfigError> {
    let value = std::env::var(name).map_err(|_| HrmConfigError {
        key: name,
        invalid: false,
    })?;
    Uuid::parse_str(value.trim()).map_err(|_| HrmConfigError {
        key: name,
        invalid: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims_with_permissions(permissions: &[&str]) -> JwtClaims {
        JwtClaims {
            sub: "test-user".to_string(),
            email: None,
            email_verified: false,
            role: None,
            permissions: permissions
                .iter()
                .map(|permission| (*permission).to_string())
                .collect(),
        }
    }

    #[test]
    fn document_upload_permission_requires_exact_permission_string() {
        let exact = claims_with_permissions(&["CHATBOT_UPLOAD_DOCUMENT"]);
        assert!(has_document_upload_permission(&exact));
        assert!(require_document_upload_permission(true, Some(&exact)).is_ok());

        for permissions in [
            Vec::<&str>::new(),
            vec!["CHATBOT_UPLOAD"],
            vec!["chatbot_upload_document"],
        ] {
            let error = require_document_upload_permission(
                true,
                Some(&claims_with_permissions(&permissions)),
            )
            .expect_err("near or missing permissions must be rejected");
            assert_eq!(error.status, StatusCode::FORBIDDEN);
            assert_eq!(error.code, "CHATBOT_UPLOAD_PERMISSION_REQUIRED");
        }
    }

    #[test]
    fn document_upload_permission_is_noop_outside_hrm_mode() {
        assert!(require_document_upload_permission(false, None).is_ok());
        assert!(
            require_document_upload_permission(
                false,
                Some(&claims_with_permissions(&["CHATBOT_UPLOAD"]))
            )
            .is_ok()
        );
    }

    #[test]
    fn role_mapping_is_fail_closed_and_exact() {
        for (role, relation) in [
            (HRM_ADMIN_ROLE, Relation::Admin),
            (HRM_HR_ROLE, Relation::Admin),
            // Phase 9 contract: managers consume the corpus as members; only ADMIN/HR manage it.
            (HRM_MANAGER_ROLE, Relation::Member),
            (HRM_EMPLOYEE_ROLE, Relation::Member),
        ] {
            assert_eq!(HrmConfig::role_relation(Some(role)).unwrap(), relation);
        }
        assert!(matches!(
            HrmConfig::role_relation(None),
            Err(HrmProvisionError::InvalidRole)
        ));
        assert!(matches!(
            HrmConfig::role_relation(Some("SUPERVISOR")),
            Err(HrmProvisionError::InvalidRole)
        ));
        assert!(matches!(
            HrmConfig::role_relation(Some("")),
            Err(HrmProvisionError::InvalidRole)
        ));
    }

    #[test]
    fn manager_maps_to_member_and_never_admin() {
        let relation = HrmConfig::role_relation(Some(HRM_MANAGER_ROLE)).unwrap();

        // Phase 9 contract: MANAGER can read/chat as a member but cannot manage the corpus.
        assert_eq!(relation, Relation::Member);
        assert_ne!(relation, Relation::Admin);
    }

    #[test]
    fn scope_rejects_foreign_tenant_or_workspace() {
        let config = HrmConfig {
            tenant_id: Uuid::parse_str("a47ab6d6-bf77-4c8c-a22d-a4f1997eb18d").unwrap(),
            workspace_id: Uuid::parse_str("fa76881f-6367-4b80-a89e-a3e01206a806").unwrap(),
        };

        assert!(ensure_scope("/workspaces/fa76881f-6367-4b80-a89e-a3e01206a806/chat", &config).is_ok());
        assert!(matches!(
            ensure_scope("/workspaces/00000000-0000-0000-0000-000000000001/chat", &config),
            Err(HrmScopeError::Workspace(_))
        ));
        assert!(matches!(
            ensure_scope("/tenants/00000000-0000-0000-0000-000000000001/workspaces", &config),
            Err(HrmScopeError::Tenant(_))
        ));
    }

    fn hrm_test_config() -> HrmConfig {
        HrmConfig {
            tenant_id: Uuid::parse_str("a47ab6d6-bf77-4c8c-a22d-a4f1997eb18d").unwrap(),
            workspace_id: Uuid::parse_str("fa76881f-6367-4b80-a89e-a3e01206a806").unwrap(),
        }
    }

    /// Alias phải chạy trên **mọi** route workspace, không chỉ vài route được nhớ tới:
    /// alias hoạt động chỗ này mà không hoạt động chỗ kia là thứ khó debug nhất.
    #[test]
    fn workspace_alias_resolves_on_every_workspace_route() {
        let config = hrm_test_config();
        let workspace_id = config.workspace_id.to_string();

        for suffix in [
            "",
            "/documents",
            "/documents/upload",
            "/documents/2f1d4e3c-0000-4000-8000-000000000001",
            "/documents/2f1d4e3c-0000-4000-8000-000000000001/retry",
            "/documents/2f1d4e3c-0000-4000-8000-000000000001/preview",
            "/chat",
            "/chat/history",
            "/chat/sessions",
            "/chat/sessions/2f1d4e3c-0000-4000-8000-000000000002/messages",
            "/chunks/2f1d4e3c-0000-4000-8000-000000000003",
            "/citations/resolve",
            "/graph",
            "/members",
        ] {
            assert_eq!(
                resolve_workspace_alias_path(&format!("/workspaces/hrm{suffix}"), &config),
                Some(format!("/workspaces/{workspace_id}{suffix}")),
                "alias must resolve for /workspaces/hrm{suffix}"
            );
        }
    }

    /// UUID thật không được đụng tới — trả `None` để caller giữ nguyên URI gốc.
    #[test]
    fn real_uuid_paths_are_left_untouched() {
        let config = hrm_test_config();

        for path in [
            "/workspaces/fa76881f-6367-4b80-a89e-a3e01206a806/chat",
            "/workspaces/00000000-0000-0000-0000-000000000001/documents",
            "/health",
            "/tenants/a47ab6d6-bf77-4c8c-a22d-a4f1997eb18d/workspaces",
        ] {
            assert_eq!(resolve_workspace_alias_path(path, &config), None);
        }
    }

    /// Alias chỉ nhận đúng chuỗi thường `hrm`, và chỉ ở vị trí ngay sau `workspaces`.
    #[test]
    fn alias_is_case_sensitive_and_positional() {
        let config = hrm_test_config();

        for path in [
            "/workspaces/HRM/chat",
            "/workspaces/Hrm/chat",
            "/workspaces/hrm-workspace/chat",
            "/workspaces/hrmx/chat",
            // `hrm` ở vị trí khác không phải alias workspace.
            "/tenants/hrm/workspaces",
            "/workspaces/fa76881f-6367-4b80-a89e-a3e01206a806/chat/sessions/hrm",
        ] {
            assert_eq!(
                resolve_workspace_alias_path(path, &config),
                None,
                "must not resolve {path}"
            );
        }
    }

    /// Sau khi resolve, `ensure_scope` phải thấy đúng workspace đã ghim — alias không
    /// được trở thành đường vòng qua kiểm tra scope.
    #[test]
    fn resolved_alias_satisfies_scope_check() {
        let config = hrm_test_config();
        let resolved =
            resolve_workspace_alias_path("/workspaces/hrm/documents/upload", &config).unwrap();

        assert!(ensure_scope(&resolved, &config).is_ok());
        assert!(resolved.contains(&config.workspace_id.to_string()));
    }

    #[test]
    fn chat_permission_requires_exact_permission_string() {
        let mut claims = JwtClaims {
            sub: "employee-1".to_string(),
            email: None,
            email_verified: false,
            role: Some(HRM_EMPLOYEE_ROLE.to_string()),
            permissions: vec!["DOCUMENT_READ".to_string()],
        };
        assert!(!has_chatbot_permission(&claims));

        claims.permissions.push("CHATBOT_USE".to_string());
        assert!(has_chatbot_permission(&claims));
    }

    #[test]
    fn tuple_plan_is_idempotent_and_updates_role_relation() {
        let workspace_id = Uuid::parse_str("fa76881f-6367-4b80-a89e-a3e01206a806").unwrap();
        let object = Object::Workspace(workspace_id);

        let employee_plan = plan_tuple_sync(
            "user:employee-1",
            &object,
            Relation::Member,
            Relation::Admin,
            false,
            false,
        );
        assert_eq!(employee_plan.writes[0].relation, "member");
        assert!(employee_plan.deletes.is_empty());

        let manager_plan = plan_tuple_sync(
            "user:manager-1",
            &object,
            Relation::Member,
            Relation::Admin,
            false,
            false,
        );
        // Phase 9 contract: initial MANAGER provisioning writes member, never admin.
        assert_eq!(manager_plan.writes[0].relation, "member");

        let repeated_plan = plan_tuple_sync(
            "user:employee-1",
            &object,
            Relation::Member,
            Relation::Admin,
            true,
            false,
        );
        assert_eq!(repeated_plan, TupleSyncPlan::default());

        let hr_relation = HrmConfig::role_relation(Some(HRM_HR_ROLE)).unwrap();
        let role_change_plan = plan_tuple_sync(
            "user:employee-1",
            &object,
            hr_relation,
            Relation::Member,
            false,
            true,
        );
        assert_eq!(role_change_plan.writes[0].relation, "admin");
        assert_eq!(role_change_plan.deletes[0].relation, "member");
    }

    #[test]
    fn manager_to_hr_promotes_the_single_tuple() {
        let object = Object::Workspace(
            Uuid::parse_str("fa76881f-6367-4b80-a89e-a3e01206a806").unwrap(),
        );
        let desired_relation = HrmConfig::role_relation(Some(HRM_HR_ROLE)).unwrap();
        let plan = plan_tuple_sync(
            "user:role-change-1",
            &object,
            desired_relation,
            Relation::Member,
            false,
            true,
        );

        assert_eq!(plan.writes.len(), 1);
        assert_eq!(plan.writes[0].relation, "admin");
        assert_eq!(plan.deletes.len(), 1);
        assert_eq!(plan.deletes[0].relation, "member");

        let settled = plan_tuple_sync(
            "user:role-change-1",
            &object,
            desired_relation,
            Relation::Member,
            true,
            false,
        );
        assert_eq!(settled, TupleSyncPlan::default());
    }

    #[test]
    fn hr_to_manager_demotes_the_single_tuple() {
        let object = Object::Workspace(
            Uuid::parse_str("fa76881f-6367-4b80-a89e-a3e01206a806").unwrap(),
        );
        let desired_relation = HrmConfig::role_relation(Some(HRM_MANAGER_ROLE)).unwrap();
        let plan = plan_tuple_sync(
            "user:role-change-2",
            &object,
            desired_relation,
            Relation::Admin,
            false,
            true,
        );

        assert_eq!(plan.writes.len(), 1);
        assert_eq!(plan.writes[0].relation, "member");
        assert_eq!(plan.deletes.len(), 1);
        assert_eq!(plan.deletes[0].relation, "admin");

        let settled = plan_tuple_sync(
            "user:role-change-2",
            &object,
            desired_relation,
            Relation::Admin,
            true,
            false,
        );
        assert_eq!(settled, TupleSyncPlan::default());
    }
}
