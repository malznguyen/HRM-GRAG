use chrono::{NaiveDateTime, SecondsFormat};
use serde::Serializer;

/// Serialize a timezone-naive PostgreSQL `TIMESTAMP` that is stored as UTC by
/// convention into an unambiguous RFC 3339 wire value with a `Z` suffix.
pub(crate) fn serialize<S>(value: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.and_utc().to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

/// Optional counterpart used by nullable timestamp fields in API responses.
pub(crate) fn serialize_optional<S>(
    value: &Option<NaiveDateTime>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serialize(value, serializer),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::json;

    #[derive(Serialize)]
    struct TimestampFixture {
        #[serde(serialize_with = "serialize")]
        timestamp: NaiveDateTime,
        #[serde(serialize_with = "serialize_optional")]
        optional: Option<NaiveDateTime>,
    }

    #[test]
    fn serializes_naive_utc_as_rfc3339_z_without_changing_precision() {
        let timestamp =
            NaiveDateTime::parse_from_str("2026-08-07 08:00:00.123456", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap();

        let value = serde_json::to_value(TimestampFixture {
            timestamp,
            optional: None,
        })
        .unwrap();

        assert_eq!(
            value,
            json!({
                "timestamp": "2026-08-07T08:00:00.123456Z",
                "optional": null
            })
        );
    }

    #[test]
    fn serializes_whole_seconds_with_z() {
        let timestamp =
            NaiveDateTime::parse_from_str("2026-08-07 08:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

        let value = serde_json::to_value(TimestampFixture {
            timestamp,
            optional: Some(timestamp),
        })
        .unwrap();

        assert_eq!(value["timestamp"], "2026-08-07T08:00:00Z");
        assert_eq!(value["optional"], "2026-08-07T08:00:00Z");
    }
}
