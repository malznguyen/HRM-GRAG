use gmrag_api::auth::authz::AuthzClient;
use gmrag_api::auth::keycloak::KeycloakClient;
use gmrag_api::identity_report::{
    IdentityReportOptions, format_human_report, report_exit_code, run_identity_consistency_report,
};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Default)]
struct Args {
    json: bool,
    strict: bool,
    include_email: bool,
    tenant_id: Option<Uuid>,
    workspace_id: Option<Uuid>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) if message == "help" => {
            print_usage();
            return;
        }
        Err(message) => {
            eprintln!("{message}");
            print_usage();
            std::process::exit(1);
        }
    };
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");
    let keycloak = KeycloakClient::from_env().expect("Failed to initialize KeycloakClient");
    let authz = AuthzClient::from_env().expect("Failed to initialize AuthzClient");
    let options = IdentityReportOptions {
        tenant_id: args.tenant_id,
        workspace_id: args.workspace_id,
        include_email: args.include_email,
    };
    let report = run_identity_consistency_report(&pool, &keycloak, &authz, &options).await;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("Failed to serialize report")
        );
    } else {
        print!("{}", format_human_report(&report));
    }
    let code = report_exit_code(&report, args.strict);
    if code != 0 {
        std::process::exit(code);
    }
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut parsed = Args::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err("help".to_string()),
            "--json" => parsed.json = true,
            "--strict" => parsed.strict = true,
            "--include-email" => parsed.include_email = true,
            "--tenant-id" => parsed.tenant_id = Some(parse_uuid("--tenant-id", args.next())?),
            "--workspace-id" => {
                parsed.workspace_id = Some(parse_uuid("--workspace-id", args.next())?)
            }
            _ => return Err(format!("Unknown argument: {arg}")),
        }
    }
    Ok(parsed)
}

fn parse_uuid(flag: &str, value: Option<String>) -> Result<Uuid, String> {
    value
        .ok_or_else(|| format!("{flag} requires a UUID value"))?
        .parse()
        .map_err(|_| format!("{flag} must be a UUID"))
}

fn print_usage() {
    println!(
        "report-identity-consistency usage:\n  cargo run --bin report-identity-consistency\n  cargo run --bin report-identity-consistency -- --json --strict\n  cargo run --bin report-identity-consistency -- --tenant-id <uuid>\n  cargo run --bin report-identity-consistency -- --workspace-id <uuid>\n\noptions:\n  --json             Print machine-readable JSON.\n  --strict           Return non-zero for warnings as well as critical findings.\n  --include-email    Print full emails; default output masks them.\n  --tenant-id <uuid> Limit SQL membership/document checks to one tenant.\n  --workspace-id <uuid> Limit SQL membership/document checks to one workspace.\n  --help, -h         Show this help.\n\nSafety: this command is read-only. It never rewrites ids, merges identities, writes OpenFGA tuples, or prints credentials/tokens."
    );
}
