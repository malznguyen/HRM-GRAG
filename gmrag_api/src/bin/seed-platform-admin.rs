use gmrag_api::auth::keycloak::KeycloakClient;
use serde::Serialize;

#[derive(Serialize)]
struct TupleKey {
    user: String,
    relation: String,
    object: String,
}

#[derive(Serialize)]
struct TupleKeys {
    tuple_keys: Vec<TupleKey>,
}

#[derive(Serialize)]
struct WriteRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    writes: Option<TupleKeys>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletes: Option<TupleKeys>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let args: Vec<String> = std::env::args().collect();
    let mut user_id = None;
    let mut email = None;
    for arg in &args {
        if arg.starts_with("--user-id=") {
            user_id = Some(arg.trim_start_matches("--user-id=").to_string());
        }
        if arg.starts_with("--email=") {
            email = Some(arg.trim_start_matches("--email=").to_string());
        }
    }

    if user_id.is_some() && email.is_some() {
        eprintln!("Lỗi: Chỉ dùng một trong --user-id hoặc --email.");
        std::process::exit(1);
    }
    let uid = match (user_id, email) {
        (Some(user_id), None) if !user_id.trim().is_empty() => user_id,
        (None, Some(email)) if !email.trim().is_empty() => {
            let keycloak =
                KeycloakClient::from_env().expect("Không thể khởi tạo Keycloak Admin client");
            match keycloak.get_verified_user_by_email(email.trim()).await {
                Ok(Some(user)) => user.id,
                Ok(None) => {
                    eprintln!("Lỗi: Không tìm thấy user Keycloak có email đã verify.");
                    std::process::exit(1);
                }
                Err(err) => {
                    eprintln!("Lỗi khi lookup Keycloak: {err}");
                    std::process::exit(1);
                }
            }
        }
        _ => {
            eprintln!("Lỗi: Thiếu --user-id=<keycloak_sub> hoặc --email=<verified_email>.");
            eprintln!("Cách dùng: cargo run --bin seed-platform-admin -- --user-id=<keycloak_sub>");
            eprintln!("Hoặc: cargo run --bin seed-platform-admin -- --email=<verified_email>");
            std::process::exit(1);
        }
    };

    println!("Bắt đầu seed Platform Admin cho Keycloak sub: {}", uid);

    let api_url =
        std::env::var("OPENFGA_API_URL").unwrap_or_else(|_| "http://localhost:8081".to_string());
    let store_id = match std::env::var("OPENFGA_STORE_ID") {
        Ok(id) => id,
        Err(_) => {
            eprintln!("Lỗi: Chưa cấu hình biến môi trường OPENFGA_STORE_ID trong file .env");
            std::process::exit(1);
        }
    };

    let url = format!(
        "{}/stores/{}/write",
        api_url.trim_end_matches('/'),
        store_id
    );
    let payload = WriteRequest {
        writes: Some(TupleKeys {
            tuple_keys: vec![TupleKey {
                user: format!("user:{}", uid),
                relation: "admin".to_string(),
                object: "platform:system".to_string(),
            }],
        }),
        deletes: None,
    };

    let client = reqwest::Client::new();
    match client.post(&url).json(&payload).send().await {
        Ok(response) => {
            if response.status().is_success() {
                println!(
                    "Đã seed thành công Platform Admin tuple cho user:{} trong OpenFGA!",
                    uid
                );
            } else {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                eprintln!("Lỗi OpenFGA: status {}, body {}", status, body);
                std::process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("Lỗi kết nối tới OpenFGA: {}", err);
            std::process::exit(1);
        }
    }
}
