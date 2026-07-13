use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::audit::{AuditEventRecord, AuditEventType, insert_audit_event};
use crate::auth::authz::{AuthzClient, Object, Relation, TupleKey};
use crate::auth::keycloak::KeycloakClient;
use crate::auth::outbox::enqueue_tuple_write;
use crate::invite::normalize_email;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    DryRun,
    Apply,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryTarget {
    UserId(String),
    Email(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    WouldRecover { target_user_id: String },
    Recovered { target_user_id: String },
    AlreadyHealthy,
    WorkspaceNotFound,
}

#[derive(Debug)]
pub enum RecoveryError {
    IdentityUnavailable,
    IdentityNotVerified,
    Database(sqlx::Error),
    Authorization(crate::auth::authz::AuthzError),
    PartialFailure,
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IdentityUnavailable => write!(formatter, "identity lookup failed"),
            Self::IdentityNotVerified => {
                write!(formatter, "target identity is not enabled and verified")
            }
            Self::Database(_) => write!(formatter, "database operation failed"),
            Self::Authorization(_) => write!(formatter, "authorization store operation failed"),
            Self::PartialFailure => write!(formatter, "recovery left a repair event for follow-up"),
        }
    }
}

impl From<sqlx::Error> for RecoveryError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Khôi phục duy nhất một quản trị viên khi workspace không còn management path hợp lệ.
pub async fn recover_workspace_admin(
    pool: &PgPool,
    authz: &AuthzClient,
    keycloak: &KeycloakClient,
    workspace_id: Uuid,
    target: RecoveryTarget,
    mode: RecoveryMode,
) -> Result<RecoveryOutcome, RecoveryError> {
    let user = resolve_target(keycloak, target).await?;
    let mut transaction = pool.begin().await?;
    let tenant_id = match lock_workspace_tenant(&mut transaction, workspace_id).await? {
        Some(tenant_id) => tenant_id,
        None => return Ok(RecoveryOutcome::WorkspaceNotFound),
    };

    if workspace_has_management_path(&mut transaction, authz, workspace_id, tenant_id).await? {
        return Ok(RecoveryOutcome::AlreadyHealthy);
    }

    if mode == RecoveryMode::DryRun {
        return Ok(RecoveryOutcome::WouldRecover {
            target_user_id: user.id,
        });
    }

    sqlx::query(
        r#"
        INSERT INTO users (id, email)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
        "#,
    )
    .bind(&user.id)
    .bind(&user.email)
    .execute(&mut *transaction)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO workspace_members (workspace_id, user_id, role)
        VALUES ($1, $2, 'ADMIN')
        ON CONFLICT (workspace_id, user_id) DO UPDATE SET role = 'ADMIN'
        "#,
    )
    .bind(workspace_id)
    .bind(&user.id)
    .execute(&mut *transaction)
    .await?;

    let tuple = TupleKey {
        user: format!("user:{}", user.id),
        relation: Relation::Admin.as_str().to_string(),
        object: Object::Workspace(workspace_id).to_string(),
    };
    transaction.commit().await?;

    let fga_synchronized = match authz.check_workspace_admin(&user.id, workspace_id).await {
        Ok(true) => true,
        Ok(false) => match authz.write_tuples(vec![tuple.clone()], Vec::new()).await {
            Ok(()) => true,
            Err(error) => is_duplicate_tuple_error(&error),
        },
        Err(_) => false,
    };
    if !fga_synchronized {
        let _ = enqueue_tuple_write(pool, &tuple).await;
    }

    insert_audit_event(
        pool,
        AuditEventRecord::new(AuditEventType::WorkspaceAdminRecovered)
            .with_workspace_id(workspace_id)
            .with_tenant_id(tenant_id)
            .with_target("workspace_member", user.id.clone())
            .with_metadata(serde_json::json!({
                "recovery_mode": "operator_apply",
                "target_user_id": user.id,
                "fga_synchronized": fga_synchronized,
            })),
    )
    .await
    .map_err(RecoveryError::Database)?;

    if fga_synchronized {
        Ok(RecoveryOutcome::Recovered {
            target_user_id: user.id,
        })
    } else {
        Err(RecoveryError::PartialFailure)
    }
}

fn is_duplicate_tuple_error(error: &crate::auth::authz::AuthzError) -> bool {
    match error {
        crate::auth::authz::AuthzError::OpenFga { body, .. } => {
            let body = body.to_ascii_lowercase();
            body.contains("already exists") || body.contains("already existed")
        }
        crate::auth::authz::AuthzError::Http(_) => false,
    }
}

struct ResolvedTarget {
    id: String,
    email: String,
}

async fn resolve_target(
    keycloak: &KeycloakClient,
    target: RecoveryTarget,
) -> Result<ResolvedTarget, RecoveryError> {
    let user = match target {
        RecoveryTarget::UserId(user_id) => keycloak
            .get_verified_user_by_id(user_id.trim())
            .await
            .map_err(|_| RecoveryError::IdentityUnavailable)?,
        RecoveryTarget::Email(email) => keycloak
            .get_verified_user_by_email(&normalize_email(&email))
            .await
            .map_err(|_| RecoveryError::IdentityUnavailable)?,
    }
    .ok_or(RecoveryError::IdentityNotVerified)?;

    let email = user
        .email
        .as_deref()
        .map(normalize_email)
        .filter(|email| !email.is_empty())
        .ok_or(RecoveryError::IdentityNotVerified)?;
    Ok(ResolvedTarget { id: user.id, email })
}

async fn lock_workspace_tenant(
    transaction: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_id FROM workspaces WHERE id = $1 FOR UPDATE")
        .bind(workspace_id)
        .fetch_optional(&mut **transaction)
        .await
}

async fn workspace_has_management_path(
    transaction: &mut Transaction<'_, Postgres>,
    authz: &AuthzClient,
    workspace_id: Uuid,
    tenant_id: Uuid,
) -> Result<bool, RecoveryError> {
    let admin_ids: Vec<String> = sqlx::query_scalar(
        "SELECT user_id FROM workspace_members WHERE workspace_id = $1 AND role = 'ADMIN'",
    )
    .bind(workspace_id)
    .fetch_all(&mut **transaction)
    .await?;
    for user_id in admin_ids {
        if authz
            .check_workspace_admin(&user_id, workspace_id)
            .await
            .map_err(RecoveryError::Authorization)?
        {
            return Ok(true);
        }
    }

    let owner_ids: Vec<String> = sqlx::query_scalar(
        "SELECT user_id FROM tenant_members WHERE tenant_id = $1 AND role = 'OWNER'",
    )
    .bind(tenant_id)
    .fetch_all(&mut **transaction)
    .await?;
    for user_id in owner_ids {
        if authz
            .check_tenant_owner(&user_id, tenant_id)
            .await
            .map_err(RecoveryError::Authorization)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}
