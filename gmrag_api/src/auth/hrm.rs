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
            Some(HRM_EMPLOYEE_ROLE) => Ok(Relation::Member),
            Some(HRM_MANAGER_ROLE) => Ok(Relation::Admin),
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

    let user = format!("user:{}", claims.sub);
    let object = Object::Workspace(config.workspace_id);
    let desired_tuple = tuple_key(&user, desired_relation, &object);
    let other_relation = match desired_relation {
        Relation::Member => Relation::Admin,
        Relation::Admin => Relation::Member,
        _ => unreachable!("HRM role relation is restricted to member/admin"),
    };
    let other_tuple = tuple_key(&user, other_relation, &object);

    let desired_present = authz
        .check_fga(&user, desired_relation, &object)
        .await
        .map_err(HrmProvisionError::Authz)?;
    let other_present = authz
        .check_fga(&user, other_relation, &object)
        .await
        .map_err(HrmProvisionError::Authz)?;

    let writes = (!desired_present).then_some(vec![desired_tuple]).unwrap_or_default();
    let deletes = other_present.then_some(vec![other_tuple]).unwrap_or_default();
    if !writes.is_empty() || !deletes.is_empty() {
        authz
            .write_tuples(writes, deletes)
            .await
            .map_err(HrmProvisionError::Authz)?;
    }

    Ok(())
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

    #[test]
    fn role_mapping_is_fail_closed_and_exact() {
        assert_eq!(
            HrmConfig::role_relation(Some(HRM_EMPLOYEE_ROLE)).unwrap(),
            Relation::Member
        );
        assert_eq!(
            HrmConfig::role_relation(Some(HRM_MANAGER_ROLE)).unwrap(),
            Relation::Admin
        );
        assert!(matches!(
            HrmConfig::role_relation(None),
            Err(HrmProvisionError::InvalidRole)
        ));
        assert!(matches!(
            HrmConfig::role_relation(Some("ADMIN")),
            Err(HrmProvisionError::InvalidRole)
        ));
        assert!(matches!(
            HrmConfig::role_relation(Some("")),
            Err(HrmProvisionError::InvalidRole)
        ));
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
}
