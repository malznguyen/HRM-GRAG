//! LIFE-007: report snapshot test cho retention residue.
//!
//! Hermetic — dùng core thuần `build_retention_report`, không cần PostgreSQL/Qdrant/MinIO.
//! Snapshot so sánh toàn bộ report dưới dạng JSON value nên mọi field mới/đổi tên đều fail.

use gmrag_api::retention_report::{
    CLASS_RECOVERY_DEAD, CLASS_RECOVERY_PENDING, CLASS_UNEXPLAINED, CLASS_UNRECOVERED,
    DeleteEventRow, OwedObjectTarget, OwedVectorTarget, RETENTION_EXIT_ACTION_REQUIRED,
    RETENTION_EXIT_CLEAR, RetentionInputs, ScannedVector, build_retention_report,
    format_human_report, retention_exit_code,
};
use serde_json::json;
use std::collections::HashSet;
use uuid::Uuid;

const TENANT: &str = "11111111-1111-1111-1111-111111111111";
const WORKSPACE_LIVE: &str = "22222222-2222-2222-2222-222222222222";
const WORKSPACE_GONE: &str = "33333333-3333-3333-3333-333333333333";
const DOCUMENT_LIVE: &str = "44444444-4444-4444-4444-444444444444";
const DOCUMENT_DELETED: &str = "55555555-5555-5555-5555-555555555555";
const DOCUMENT_IN_GONE_WORKSPACE: &str = "66666666-6666-6666-6666-666666666666";

fn uuid(raw: &str) -> Uuid {
    Uuid::parse_str(raw).expect("fixture UUID must parse")
}

fn timestamp(day: u32) -> chrono::NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2026, 7, day)
        .expect("fixture date must be valid")
        .and_hms_opt(12, 0, 0)
        .expect("fixture time must be valid")
}

fn object_key(workspace: &str, document: &str) -> String {
    format!("tenants/{TENANT}/workspaces/{workspace}/documents/{document}/original.pdf")
}

/// Fixture cover cả bốn class cùng lúc:
/// - vector của document đã xoá trong workspace còn sống, không outbox nợ  → unrecovered
/// - vector của workspace đã xoá, outbox PENDING còn nợ                    → recovery_pending
/// - object dưới tenant prefix có outbox DEAD                              → recovery_dead
/// - object lạ không outbox, không delete event                            → unexplained
/// - vector + object của document còn sống                                 → không phải residue
fn mixed_residue_inputs() -> RetentionInputs {
    RetentionInputs {
        live_workspaces: HashSet::from([uuid(WORKSPACE_LIVE)]),
        live_documents: HashSet::from([(uuid(WORKSPACE_LIVE), uuid(DOCUMENT_LIVE))]),
        live_object_keys: HashSet::from([object_key(WORKSPACE_LIVE, DOCUMENT_LIVE)]),
        scanned_vectors: Some(vec![
            ScannedVector {
                workspace_id: uuid(WORKSPACE_LIVE),
                document_id: uuid(DOCUMENT_LIVE),
            },
            ScannedVector {
                workspace_id: uuid(WORKSPACE_LIVE),
                document_id: uuid(DOCUMENT_DELETED),
            },
            ScannedVector {
                workspace_id: uuid(WORKSPACE_GONE),
                document_id: uuid(DOCUMENT_IN_GONE_WORKSPACE),
            },
        ]),
        scanned_object_keys: Some(vec![
            object_key(WORKSPACE_LIVE, DOCUMENT_LIVE),
            object_key(WORKSPACE_GONE, DOCUMENT_IN_GONE_WORKSPACE),
            "stray/orphan.bin".to_string(),
        ]),
        owed_vectors: vec![OwedVectorTarget::Workspace {
            workspace_id: uuid(WORKSPACE_GONE),
            status: "PENDING".to_string(),
        }],
        owed_objects: vec![OwedObjectTarget::Prefix {
            prefix: format!("tenants/{TENANT}/"),
            status: "DEAD".to_string(),
        }],
        delete_events: vec![
            DeleteEventRow {
                event_type: "document_deleted".to_string(),
                created_at: timestamp(1),
                tenant_id: None,
                workspace_id: Some(uuid(WORKSPACE_LIVE)),
                document_id: Some(uuid(DOCUMENT_DELETED)),
            },
            DeleteEventRow {
                event_type: "workspace_deleted".to_string(),
                created_at: timestamp(2),
                tenant_id: Some(uuid(TENANT)),
                workspace_id: Some(uuid(WORKSPACE_GONE)),
                document_id: None,
            },
        ],
    }
}

#[test]
fn retention_report_snapshot_covers_every_residue_class() {
    let report = build_retention_report(&mixed_residue_inputs(), 50);
    let actual = serde_json::to_value(&report).expect("report must serialize");

    let expected = json!({
        "vectors_probed": true,
        "objects_probed": true,
        "scanned_vector_points": 3,
        "scanned_object_keys": 3,
        "counts": {
            "vector_residue": 2,
            "object_residue": 2,
            "recovery_pending": 1,
            "recovery_dead": 1,
            "unrecovered": 1,
            "unexplained": 1
        },
        "by_class": {
            "recovery_dead": 1,
            "recovery_pending": 1,
            "unexplained": 1,
            "unrecovered": 1
        },
        "vector_residue": [
            {
                "workspace_id": WORKSPACE_LIVE,
                "document_id": DOCUMENT_DELETED,
                "workspace_live": true,
                "class": CLASS_UNRECOVERED,
                "owed_outbox_status": null,
                "delete_event": {
                    "event_type": "document_deleted",
                    "created_at": "2026-07-01T12:00:00"
                }
            },
            {
                "workspace_id": WORKSPACE_GONE,
                "document_id": DOCUMENT_IN_GONE_WORKSPACE,
                "workspace_live": false,
                "class": CLASS_RECOVERY_PENDING,
                "owed_outbox_status": "PENDING",
                "delete_event": {
                    "event_type": "workspace_deleted",
                    "created_at": "2026-07-02T12:00:00"
                }
            }
        ],
        "object_residue": [
            {
                "object_key": "stray/orphan.bin",
                "tenant_id": null,
                "workspace_id": null,
                "document_id": null,
                "class": CLASS_UNEXPLAINED,
                "owed_outbox_status": null,
                "delete_event": null
            },
            {
                "object_key": object_key(WORKSPACE_GONE, DOCUMENT_IN_GONE_WORKSPACE),
                "tenant_id": TENANT,
                "workspace_id": WORKSPACE_GONE,
                "document_id": DOCUMENT_IN_GONE_WORKSPACE,
                "class": CLASS_RECOVERY_DEAD,
                "owed_outbox_status": "DEAD",
                "delete_event": {
                    "event_type": "workspace_deleted",
                    "created_at": "2026-07-02T12:00:00"
                }
            }
        ],
        "vector_residue_truncated": false,
        "object_residue_truncated": false,
        "limitations": [
            "Residue is judged against the SQL inventory read in this run. A delete committing mid-scan can appear as residue on this pass and be clear on the next.",
            "A matching delete event proves the resource ID appeared in an audited delete; it does not by itself prove that delete caused this residue.",
            "`unexplained` means no owing outbox row and no delete event was found — treat it as unknown provenance, not proof of an unaudited delete."
        ]
    });

    assert_eq!(actual, expected);
}

#[test]
fn live_document_vector_and_object_stay_out_of_the_report() {
    let report = build_retention_report(&mixed_residue_inputs(), 50);

    assert!(
        !report
            .vector_residue
            .iter()
            .any(|residue| residue.document_id == uuid(DOCUMENT_LIVE)),
        "vectors of a live document must not be reported as residue"
    );
    assert!(
        !report
            .object_residue
            .iter()
            .any(|residue| residue.document_id == Some(uuid(DOCUMENT_LIVE))),
        "the object of a live document must not be reported as residue"
    );
}

#[test]
fn residue_needing_operator_action_exits_two() {
    let report = build_retention_report(&mixed_residue_inputs(), 50);

    assert_eq!(retention_exit_code(&report), RETENTION_EXIT_ACTION_REQUIRED);
}

#[test]
fn recovery_pending_alone_exits_zero() {
    let inputs = RetentionInputs {
        scanned_vectors: Some(vec![ScannedVector {
            workspace_id: uuid(WORKSPACE_GONE),
            document_id: uuid(DOCUMENT_IN_GONE_WORKSPACE),
        }]),
        scanned_object_keys: Some(Vec::new()),
        owed_vectors: vec![OwedVectorTarget::Workspace {
            workspace_id: uuid(WORKSPACE_GONE),
            status: "PENDING".to_string(),
        }],
        ..RetentionInputs::default()
    };

    let report = build_retention_report(&inputs, 50);

    assert_eq!(report.counts.recovery_pending, 1);
    assert_eq!(report.counts.unrecovered, 0);
    assert_eq!(retention_exit_code(&report), RETENTION_EXIT_CLEAR);
}

#[test]
fn human_report_snapshot_states_read_only_and_lists_classes() {
    let rendered = format_human_report(&build_retention_report(&mixed_residue_inputs(), 50));

    assert!(rendered.starts_with("LIFE-007 retention residue report (READ-ONLY)\n"));
    assert!(rendered.contains("vector_residue=2"));
    assert!(rendered.contains("object_residue=2"));
    assert!(rendered.contains(&format!("class={CLASS_UNRECOVERED}")));
    assert!(rendered.contains(&format!("class={CLASS_RECOVERY_PENDING}")));
    assert!(rendered.contains(&format!("class={CLASS_RECOVERY_DEAD}")));
    assert!(rendered.contains(&format!("class={CLASS_UNEXPLAINED}")));
    assert!(rendered.contains("object_key=stray/orphan.bin"));
    assert!(rendered.trim_end().ends_with(
        "READ ONLY — no vector, object, SQL row, outbox row, or audit row was modified."
    ));
}

#[test]
fn unprobed_store_is_reported_as_unknown_not_clean() {
    let inputs = RetentionInputs {
        scanned_object_keys: Some(vec!["stray/orphan.bin".to_string()]),
        ..RetentionInputs::default()
    };

    let report = build_retention_report(&inputs, 50);

    assert!(!report.vectors_probed);
    assert!(report.objects_probed);
    assert_eq!(report.counts.vector_residue, 0);
    assert!(
        report
            .limitations
            .iter()
            .any(|limitation| limitation.contains("Vectors were not probed")),
        "an unprobed store must be called out as unknown rather than silently zero"
    );
}

#[test]
fn sample_limit_is_reported_rather_than_silently_capping() {
    let keys: Vec<String> = (0..7).map(|index| format!("stray/{index}.bin")).collect();
    let inputs = RetentionInputs {
        scanned_object_keys: Some(keys),
        ..RetentionInputs::default()
    };

    let report = build_retention_report(&inputs, 3);

    assert_eq!(report.counts.object_residue, 7);
    assert_eq!(report.object_residue.len(), 3);
    assert!(report.object_residue_truncated);
    assert!(format_human_report(&report).contains("object_residue list truncated"));
}
