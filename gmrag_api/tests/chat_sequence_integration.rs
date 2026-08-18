use std::env;
use std::time::Duration;

use futures::future::join_all;
use gmrag_api::chat::{
    ChatTurnSequence, insert_chat_message, insert_user_chat_message_and_reserve_turn,
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

struct Fixture {
    tenant_id: Uuid,
    workspace_id: Uuid,
    user_id: String,
}

async fn test_pool() -> Option<PgPool> {
    if env::var("APP_ENV").ok().as_deref() != Some("test") {
        eprintln!("skip: APP_ENV=test is required for chat sequence integration test");
        return None;
    }
    let database_url = env::var("TEST_DATABASE_URL").ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await
        .ok()?;
    sqlx::migrate!("./migrations").run(&pool).await.ok()?;
    Some(pool)
}

async fn seed_fixture(pool: &PgPool) -> Fixture {
    let tenant_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let user_id = format!("phase16-sequence-{}", Uuid::new_v4());

    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(format!("phase16-sequence-{tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(&user_id)
        .bind(format!("{user_id}@example.test"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspaces (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(tenant_id)
        .bind("phase16-sequence")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO workspace_members (workspace_id, user_id, role) VALUES ($1, $2, 'MEMBER')",
    )
    .bind(workspace_id)
    .bind(&user_id)
    .execute(pool)
    .await
    .unwrap();

    Fixture {
        tenant_id,
        workspace_id,
        user_id,
    }
}

async fn create_session(pool: &PgPool, fixture: &Fixture, title: &str) -> Uuid {
    let session_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO chat_sessions (id, workspace_id, user_id, title) VALUES ($1, $2, $3, $4)",
    )
    .bind(session_id)
    .bind(fixture.workspace_id)
    .bind(&fixture.user_id)
    .bind(title)
    .execute(pool)
    .await
    .unwrap();
    session_id
}

async fn ordered_messages(pool: &PgPool, session_id: Uuid) -> Vec<(i64, String, String)> {
    sqlx::query_as(
        "SELECT message_sequence, role, content FROM chat_messages WHERE session_id = $1 ORDER BY message_sequence",
    )
    .bind(session_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn cleanup(pool: &PgPool, fixture: Fixture) {
    sqlx::query("DELETE FROM tenants WHERE id = $1")
        .bind(fixture.tenant_id)
        .execute(pool)
        .await
        .unwrap();
}

async fn persist_assistant(pool: &PgPool, session_id: Uuid, turn: ChatTurnSequence, content: &str) {
    insert_chat_message(
        pool,
        session_id,
        turn.assistant_sequence,
        "assistant",
        content,
        &[],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn chat_sequence_covers_immediate_parallel_partial_and_empty_turns() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let fixture = seed_fixture(&pool).await;

    // The second question is submitted about 100ms after the first answer has
    // been persisted. The reserved pair must remain user/assistant per turn.
    let immediate_session = create_session(&pool, &fixture, "immediate").await;
    let first =
        insert_user_chat_message_and_reserve_turn(&pool, immediate_session, "overtime policy")
            .await
            .unwrap();
    persist_assistant(&pool, immediate_session, first, "overtime answer").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second =
        insert_user_chat_message_and_reserve_turn(&pool, immediate_session, "annual leave")
            .await
            .unwrap();
    persist_assistant(&pool, immediate_session, second, "annual leave answer").await;
    assert_eq!(
        ordered_messages(&pool, immediate_session).await,
        vec![
            (1, "user".to_string(), "overtime policy".to_string()),
            (2, "assistant".to_string(), "overtime answer".to_string()),
            (3, "user".to_string(), "annual leave".to_string()),
            (
                4,
                "assistant".to_string(),
                "annual leave answer".to_string()
            ),
        ]
    );

    // Lock acquisition determines turn ownership. Outputs are persisted in
    // reverse completion order to prove that response completion cannot reorder
    // the reserved positions or create duplicates.
    let parallel_session = create_session(&pool, &fixture, "parallel").await;
    let turns = join_all((0..8).map(|index| {
        let pool = pool.clone();
        async move {
            insert_user_chat_message_and_reserve_turn(
                &pool,
                parallel_session,
                &format!("parallel question {index}"),
            )
            .await
            .unwrap()
        }
    }))
    .await;
    let mut user_sequences: Vec<i64> = turns.iter().map(|turn| turn.user_sequence).collect();
    user_sequences.sort_unstable();
    assert_eq!(user_sequences, vec![1, 3, 5, 7, 9, 11, 13, 15]);
    for turn in turns.iter().rev() {
        persist_assistant(&pool, parallel_session, *turn, "parallel answer").await;
    }
    let parallel_rows = ordered_messages(&pool, parallel_session).await;
    assert_eq!(parallel_rows.len(), 16);
    for (index, (_, role, _)) in parallel_rows.iter().enumerate() {
        assert_eq!(role, if index % 2 == 0 { "user" } else { "assistant" });
    }
    assert_eq!(
        parallel_rows
            .iter()
            .map(|(sequence, _, _)| *sequence)
            .collect::<Vec<_>>(),
        (1..=16).collect::<Vec<_>>()
    );

    // A client disconnect after receiving some tokens persists the partial
    // assistant into its reserved second position.
    let partial_session = create_session(&pool, &fixture, "partial").await;
    let partial_turn =
        insert_user_chat_message_and_reserve_turn(&pool, partial_session, "partial question")
            .await
            .unwrap();
    persist_assistant(&pool, partial_session, partial_turn, "partial answer").await;
    let next_turn =
        insert_user_chat_message_and_reserve_turn(&pool, partial_session, "next question")
            .await
            .unwrap();
    persist_assistant(&pool, partial_session, next_turn, "next answer").await;
    assert_eq!(
        ordered_messages(&pool, partial_session)
            .await
            .iter()
            .map(|(sequence, role, _)| (*sequence, role.clone()))
            .collect::<Vec<_>>(),
        vec![
            (1, "user".to_string()),
            (2, "assistant".to_string()),
            (3, "user".to_string()),
            (4, "assistant".to_string()),
        ]
    );

    // An empty buffer creates no assistant row, but the reserved position is
    // intentionally left as a gap. The following turn still remains ordered.
    let empty_session = create_session(&pool, &fixture, "empty").await;
    let empty_turn = insert_user_chat_message_and_reserve_turn(
        &pool,
        empty_session,
        "provider failed before first token",
    )
    .await
    .unwrap();
    assert_eq!(empty_turn.assistant_sequence, 2);
    let recovered_turn =
        insert_user_chat_message_and_reserve_turn(&pool, empty_session, "recovery question")
            .await
            .unwrap();
    persist_assistant(&pool, empty_session, recovered_turn, "recovery answer").await;
    assert_eq!(
        ordered_messages(&pool, empty_session).await,
        vec![
            (
                1,
                "user".to_string(),
                "provider failed before first token".to_string()
            ),
            (3, "user".to_string(), "recovery question".to_string()),
            (4, "assistant".to_string(), "recovery answer".to_string()),
        ]
    );

    cleanup(&pool, fixture).await;
}
