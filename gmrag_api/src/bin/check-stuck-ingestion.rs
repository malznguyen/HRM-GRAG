use gmrag_api::ingestion::stuck_health::{
    DEFAULT_MAX_AGE_MINUTES, StuckIngestionHealthOptions, check_stuck_ingestion_health,
};
use sqlx::postgres::PgPoolOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Args {
    max_age_minutes: i64,
    json: bool,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let args = match parse_args(std::env::args().skip(1)) {
        Ok(Some(args)) => args,
        Ok(None) => {
            print_usage();
            return;
        }
        Err(message) => {
            eprintln!("{message}");
            print_usage();
            std::process::exit(2);
        }
    };

    let database_url = match std::env::var("DATABASE_URL") {
        Ok(database_url) => database_url,
        Err(_) => {
            eprintln!("DATABASE_URL must be set");
            std::process::exit(2);
        }
    };
    let pool = match PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!("failed to connect to database: {error}");
            std::process::exit(2);
        }
    };

    let options = StuckIngestionHealthOptions {
        max_age_minutes: args.max_age_minutes,
    };
    let report = match check_stuck_ingestion_health(&pool, options).await {
        Ok(report) => report,
        Err(error) => {
            eprintln!("failed to query stuck ingestion health: {error}");
            std::process::exit(2);
        }
    };

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("health report must serialize")
        );
    } else {
        print!("{}", report.format_text());
    }

    std::process::exit(report.exit_code());
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Option<Args>, String> {
    let mut parsed = Args {
        max_age_minutes: DEFAULT_MAX_AGE_MINUTES,
        json: false,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(None),
            "--json" => parsed.json = true,
            "--max-age-minutes" => {
                parsed.max_age_minutes = parse_non_negative_i64("--max-age-minutes", args.next())?;
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(Some(parsed))
}

fn parse_non_negative_i64(flag: &str, value: Option<String>) -> Result<i64, String> {
    let value = value.ok_or_else(|| format!("{flag} requires a non-negative integer"))?;
    let parsed = value
        .parse::<i64>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))?;
    if parsed < 0 {
        return Err(format!("{flag} must be a non-negative integer"));
    }
    Ok(parsed)
}

fn print_usage() {
    println!(
        "check-stuck-ingestion\n\n\
Usage:\n  \
  cargo run --bin check-stuck-ingestion -- [--max-age-minutes N] [--json]\n\n\
Options:\n  \
  --max-age-minutes N  Alert when documents.status=PROCESSING older than N minutes by created_at (default 15).\n  \
  --json               Print the machine-readable JSON report.\n  \
  --help, -h           Show this help.\n\n\
Read-only synthetic alert health-check. It never mutates documents or ingestion_jobs.\n\
Age uses documents.created_at (no status_changed_at column). Active job = QUEUED|PROCESSING.\n\n\
Exit codes:\n  \
  0                    Health is below all thresholds.\n  \
  1                    A health threshold is exceeded.\n  \
  2                    The check could not be evaluated (CLI, database, or query error)."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_ops006_contract() {
        assert_eq!(
            parse_args(std::iter::empty()).unwrap(),
            Some(Args {
                max_age_minutes: 15,
                json: false,
            })
        );
    }

    #[test]
    fn invalid_cli_values_are_errors_for_exit_code_two() {
        assert!(parse_args(["--max-age-minutes", "-1"].into_iter().map(String::from)).is_err());
        assert!(parse_args(["--max-age-minutes"].into_iter().map(String::from)).is_err());
        assert!(parse_args(["--unknown"].into_iter().map(String::from)).is_err());
    }
}
