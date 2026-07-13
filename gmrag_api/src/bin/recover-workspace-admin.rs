use gmrag_api::{
    auth::{authz::AuthzClient, keycloak::KeycloakClient},
    workspace_admin_recovery::{
        RecoveryMode, RecoveryOutcome, RecoveryTarget, recover_workspace_admin,
    },
};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => exit_with(1, &message),
    };
    let pool = match PgPoolOptions::new()
        .max_connections(4)
        .connect(&required_env("DATABASE_URL"))
        .await
    {
        Ok(pool) => pool,
        Err(_) => exit_with(1, "Không thể kết nối PostgreSQL."),
    };
    if let Err(_) = sqlx::migrate!("./migrations").run(&pool).await {
        exit_with(1, "Không thể chạy migration trước recovery.");
    }
    let authz = match AuthzClient::from_env() {
        Ok(client) => client,
        Err(_) => exit_with(1, "Thiếu cấu hình OpenFGA."),
    };
    let keycloak = match KeycloakClient::from_env() {
        Ok(client) => client,
        Err(_) => exit_with(1, "Thiếu cấu hình Keycloak Admin."),
    };

    match recover_workspace_admin(
        &pool,
        &authz,
        &keycloak,
        args.workspace_id,
        args.target,
        args.mode,
    )
    .await
    {
        Ok(RecoveryOutcome::WouldRecover { target_user_id }) => {
            println!(
                "dry-run: workspace không có management path; sẽ khôi phục ADMIN cho user_id={target_user_id}"
            );
        }
        Ok(RecoveryOutcome::Recovered { target_user_id }) => {
            println!("đã khôi phục ADMIN cho user_id={target_user_id}");
        }
        Ok(RecoveryOutcome::AlreadyHealthy) => {
            exit_with(2, "workspace đã có management path hợp lệ; không thay đổi.");
        }
        Ok(RecoveryOutcome::WorkspaceNotFound) => exit_with(1, "workspace không tồn tại."),
        Err(error) => exit_with(3, &format!("recovery thất bại: {error}")),
    }
}

struct Arguments {
    workspace_id: Uuid,
    target: RecoveryTarget,
    mode: RecoveryMode,
}

fn parse_args() -> Result<Arguments, String> {
    let mut workspace_id = None;
    let mut user_id = None;
    let mut email = None;
    let mut apply = false;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace-id" => {
                index += 1;
                workspace_id = args.get(index).cloned();
            }
            "--user-id" => {
                index += 1;
                user_id = args.get(index).cloned();
            }
            "--email" => {
                index += 1;
                email = args.get(index).cloned();
            }
            "--apply" => apply = true,
            "--dry-run" => {}
            "--help" | "-h" => {
                return Err("Cách dùng: recover-workspace-admin --workspace-id <uuid> (--user-id <keycloak_sub> | --email <verified_email>) [--dry-run|--apply]".to_string());
            }
            other => return Err(format!("Tham số không hỗ trợ: {other}")),
        }
        index += 1;
    }
    let workspace_id = workspace_id
        .ok_or_else(|| "Thiếu --workspace-id.".to_string())
        .and_then(|value| {
            Uuid::parse_str(&value).map_err(|_| "workspace-id phải là UUID.".to_string())
        })?;
    let target = match (user_id, email) {
        (Some(user_id), None) if !user_id.trim().is_empty() => RecoveryTarget::UserId(user_id),
        (None, Some(email)) if !email.trim().is_empty() => RecoveryTarget::Email(email),
        _ => return Err("Phải truyền chính xác một trong --user-id hoặc --email.".to_string()),
    };
    Ok(Arguments {
        workspace_id,
        target,
        mode: if apply {
            RecoveryMode::Apply
        } else {
            RecoveryMode::DryRun
        },
    })
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| exit_with(1, &format!("Thiếu biến môi trường {name}.")))
}

fn exit_with(code: i32, message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(code)
}
