//! Canonical workspace member roles for public API and SQL storage.
//!
//! Public API accepts only `member` | `admin`. Tenant Owner is a tenant-level
//! OpenFGA relation, not a workspace role alias.

use axum::http::StatusCode;

use super::authz::{ApiError, Relation};

/// Vai trò workspace được API chấp nhận (map sang SQL ADMIN/MEMBER).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceMemberRole {
    Member,
    Admin,
}

impl WorkspaceMemberRole {
    /// Parse canonical public API role. Rejects `user`, `owner`, empty, unknown.
    pub fn parse_api(input: &str) -> Result<Self, ApiError> {
        match input.trim().to_ascii_lowercase().as_str() {
            "member" => Ok(Self::Member),
            "admin" => Ok(Self::Admin),
            _ => Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "INVALID_MEMBER_ROLE",
                message: "role must be member or admin".to_string(),
            }),
        }
    }

    /// Giá trị lưu trong SQL (`workspace_members.role`).
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::Member => "MEMBER",
            Self::Admin => "ADMIN",
        }
    }

    /// Relation OpenFGA tương ứng.
    pub fn as_fga_relation(self) -> Relation {
        match self {
            Self::Member => Relation::Member,
            Self::Admin => Relation::Admin,
        }
    }

    /// Parse role từ SQL read model.
    pub fn from_sql(role: &str) -> Option<Self> {
        match role.trim().to_ascii_uppercase().as_str() {
            "MEMBER" => Some(Self::Member),
            "ADMIN" => Some(Self::Admin),
            _ => None,
        }
    }

    pub fn is_admin(self) -> bool {
        matches!(self, Self::Admin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_api_accepts_member_and_admin_case_insensitive() {
        assert_eq!(
            WorkspaceMemberRole::parse_api("member").unwrap(),
            WorkspaceMemberRole::Member
        );
        assert_eq!(
            WorkspaceMemberRole::parse_api("ADMIN").unwrap(),
            WorkspaceMemberRole::Admin
        );
        assert_eq!(
            WorkspaceMemberRole::parse_api(" Member ").unwrap(),
            WorkspaceMemberRole::Member
        );
    }

    #[test]
    fn parse_api_rejects_aliases_and_unknown() {
        for bad in ["user", "owner", "USER", "OWNER", "", "root", "superadmin"] {
            let err = WorkspaceMemberRole::parse_api(bad).unwrap_err();
            assert_eq!(err.code, "INVALID_MEMBER_ROLE");
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn sql_and_fga_mapping() {
        assert_eq!(WorkspaceMemberRole::Member.as_sql(), "MEMBER");
        assert_eq!(WorkspaceMemberRole::Admin.as_sql(), "ADMIN");
        assert_eq!(
            WorkspaceMemberRole::Member.as_fga_relation(),
            Relation::Member
        );
        assert_eq!(
            WorkspaceMemberRole::Admin.as_fga_relation(),
            Relation::Admin
        );
    }
}
