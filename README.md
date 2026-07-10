# GMRAG

GMRAG is a multi-tenant GraphRAG platform under active refactor.

Current live architecture after Phase 2 + Phase 3A hardening:

- tenant and workspace hierarchy in PostgreSQL,
- OpenFGA as the authorization source of truth,
- Keycloak-backed tenant-owner bootstrap and workspace member addition (verified users only),
- original PDF storage in MinIO/S3 through `aws-sdk-s3` v1,
- Qdrant vector retrieval for ACL-aware chunk search,
- PostgreSQL graph store with `graph_nodes.embedding` on the ingestion forward-path (HNSW `vector_l2_ops`; legacy NULL nodes need operator `backfill-graph-node-embeddings`),
- document-level ACL fully live; Phase 3A operator workers (outbox, storage cleanup, invite-placeholder cleanup) available.

## Current Docs

Use these files as the source of truth:

- `docs/GMRAG-refactor-doc.md`
- `docs/CURRENT_ARCHITECTURE.md`
- `docs/CURRENT_DATABASE_SCHEMA.md`
- `docs/CURRENT_API_CONTRACT.md`
- `docs/RUNBOOK.md`

Historical v1 audit snapshots are archived under `docs/archive/v1/`.

## Current Architecture Snapshot

| Area | Current state |
| --- | --- |
| Identity and admin lookup | Keycloak-backed owner bootstrap + member addition (verified users only); bearer JWT validation is live in the backend |
| Authorization | OpenFGA via `AuthzClient` and route-level relation checks |
| Original document storage | S3-compatible object storage; MinIO in local development |
| Current retrieval path | Qdrant vector search (ACL-aware chunk filtering) and PostgreSQL graph tables (`graph_nodes.embedding` L2 + ILIKE fallback) |
| Phase 3A hardening | ✅ complete — authz outbox processor, storage cleanup, invite-placeholder cleanup, audit trail (see `docs/RUNBOOK.md`) |
| Phase 3B/3C | 🟡 partial — Qdrant lifecycle/outbox (claim, backoff, `DEAD`) + orphan cleanup shipped; daemon orchestration and other follow-up remain (see `docs/authz-refactor-taskboard.md`) |

## Local Services

`docker-compose.yml` currently starts these local services:

- app Postgres
- OpenFGA Postgres
- OpenFGA migrate
- OpenFGA server
- MinIO
- minio-init bucket creation
- Qdrant (REST `6333`, gRPC `6334`)
- Keycloak (`8080`, `start-dev` + realm import for Admin API)

**Ollama is not in Docker.** Install Ollama on the host and pull the ADR-21 embedding model before ingestion or chat retrieval:

```bash
ollama pull AITeamVN/Vietnamese_Embedding
```

This model is the default for chunk embedding (ingestion), graph node embedding (ingestion forward-path), and query embedding (chat). Runtime output is **768** dimensions (matches Postgres `vector(768)` and Qdrant). The HuggingFace model card lists 1024-d; the Ollama registry copy used here is verified at **768-d** — do not change schema solely because of the card (see `docs/RUNBOOK.md` §6). Override with `OLLAMA_EMBED_MODEL` only if you accept retrieval-quality risk and a full re-embed.

**Keycloak scope in local compose:** the container is for **Admin API lookup** used by tenant-owner bootstrap and workspace member addition (`KeycloakClient.get_verified_user_by_email`). JWT validation still uses `CLERK_ISSUER` / the existing `test-bypass-jwt` path — Keycloak is **not** used for login or bearer-token validation in the current backend.

Local Keycloak admin console: `http://localhost:8080` (default demo credentials `admin` / `admin`). Realm `gmrag` and confidential client `gmrag-admin-client` are imported from `docker/keycloak/gmrag-realm.json` on first start.

## Backend Environment

Copy `gmrag_api/.env.example` to `gmrag_api/.env` and fill in the values for:

- database connection
- bearer-token issuer and JWKS configuration used by the current backend
- OpenFGA: `OPENFGA_API_URL`, `OPENFGA_STORE_ID`, `OPENFGA_MODEL_ID`
- Authz outbox worker: `AUTHZ_OUTBOX_BATCH_SIZE`, `AUTHZ_OUTBOX_MAX_RETRIES`
- Qdrant: `QDRANT_URL`, `QDRANT_COLLECTION`, `QDRANT_VECTOR_SIZE`, optional `QDRANT_API_KEY`
- Qdrant delete timeouts: `QDRANT_DELETE_REQUEST_TIMEOUT_SECS` (HTTP path), `QDRANT_DELETE_WORKER_TIMEOUT_SECS` (outbox/cleanup)
- Qdrant outbox worker: `QDRANT_OUTBOX_BATCH_SIZE`, `QDRANT_OUTBOX_MAX_RETRIES`, `QDRANT_OUTBOX_BASE_BACKOFF_SECS`, `QDRANT_OUTBOX_MAX_BACKOFF_SECS`, `QDRANT_OUTBOX_CLAIM_LEASE_SECS`
- Keycloak admin lookup: `KEYCLOAK_ADMIN_URL`, `KEYCLOAK_REALM`, `KEYCLOAK_CLIENT_ID`, `KEYCLOAK_CLIENT_SECRET`
- S3/MinIO: `S3_ENDPOINT_URL`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_FORCE_PATH_STYLE`, `S3_PRESIGN_EXPIRY_SECS`
- DeepSeek and Ollama settings for chat, graph extraction, and embeddings (`OLLAMA_EMBED_MODEL=AITeamVN/Vietnamese_Embedding` recommended)

## Local Bootstrap

1. Start local infrastructure:

   ```bash
   docker compose up -d
   ```

2. Pull the embedding model on the host (Ollama is outside Docker):

   ```bash
   ollama pull AITeamVN/Vietnamese_Embedding
   ```

3. Configure backend environment and run the API:

   ```bash
   cd gmrag_api
   cargo run
   ```

4. Bootstrap a platform admin tuple in OpenFGA for a real user id:

   ```bash
   cargo run --bin seed-platform-admin -- --user-id=<keycloak_sub_id>
   ```

5. **Member management (breaking change vs invite placeholders):** users must **sign up and verify email in identity first**, then a workspace admin can add them by email via `POST /workspaces/{id}/members`. The API resolves the real Keycloak `sub` and writes SQL + OpenFGA together. Adding someone who has not registered returns `USER_NOT_FOUND_IN_IDENTITY`. There is no invite-placeholder / pending-member flow — Keycloak is the identity source of truth so SQL and OpenFGA never desync on fake `invite_*` ids.

6. **Upgrade cleanup (legacy DBs only):** if this environment previously used invite placeholders, run the operator cleanup **after deploying** the build that stopped creating them:

   ```bash
   cargo run --bin cleanup-invite-placeholders -- --dry-run
   cargo run --bin cleanup-invite-placeholders -- --delete
   ```

   Default is dry-run; nothing is deleted without `--delete`. Safe to re-run. See `docs/RUNBOOK.md` §5.

## Phase 3A / 3B Operator Commands

Run these from `gmrag_api/`:

```bash
# Phase 3A — authz + storage + invite cleanup
cargo run --bin process-authz-outbox
cargo run --bin cleanup-storage-objects -- --dry-run
cargo run --bin cleanup-storage-objects -- --delete-orphans --delete
cargo run --bin cleanup-storage-objects -- --workspace-id <workspace_uuid> --delete
cargo run --bin cleanup-storage-objects -- --tenant-id <tenant_uuid> --delete
cargo run --bin cleanup-invite-placeholders -- --dry-run
cargo run --bin cleanup-invite-placeholders -- --delete
cargo run --bin backfill-document-workspace-tuples

# Phase 3B — Qdrant lifecycle / recovery
cargo run --bin process-qdrant-outbox
cargo run --bin cleanup-qdrant-orphans -- --dry-run
cargo run --bin cleanup-qdrant-orphans -- --delete
cargo run --bin cleanup-qdrant-orphans -- --full-scan --dry-run

# Graph node embeddings (legacy NULL backfill)
cargo run --bin backfill-graph-node-embeddings -- --dry-run
cargo run --bin backfill-graph-node-embeddings -- --apply
```

- **Qdrant outbox:** claim with `FOR UPDATE SKIP LOCKED` + lease, exponential backoff on `FAILED`, status `DEAD` for poison/exhausted retries. See `docs/RUNBOOK.md` §7 (env vars, DEAD inspection, dual-write caveat).
- **Orphan cleanup:** prioritizes outbox (`PENDING`/`FAILED`/`DEAD`) + failed audit deletes; optional expensive `--full-scan`. Default dry-run; mutations need `--delete`.
- Graph node embedding backfill fills legacy `graph_nodes.embedding IS NULL` only (manual; default dry-run). See `docs/RUNBOOK.md` §10. HNSW apply notes: §9.

## Test Commands

Run these from `gmrag_api/`:

```bash
cargo check
cargo test
cargo test --test authz_integration
cargo test --test storage_integration
cargo test --test phase3a_hardening_integration
cargo test --test graph_node_embedding_backfill_integration
```

## Current Implementation Notes

- Original PDFs live in MinIO/S3; they are no longer stored on local disk as the source of truth.
- Document delete performs SQL cleanup first and storage cleanup second on a best-effort basis.
- Retry reads the original object from storage and returns `DOCUMENT_OBJECT_MISSING` when the object is gone.
- Document-level ACL enforcement, restricted-document behavior, and Qdrant retrieval are fully implemented and verified.
- Document/workspace delete is SQL-first then **best-effort** Qdrant filter-delete + outbox enqueue (not a distributed transaction). Recovery: `process-qdrant-outbox` / `cleanup-qdrant-orphans` — see `docs/RUNBOOK.md` §7.
- Operator runbook for Phase 2/3A/3B (including Ollama, HNSW, graph node embedding backfill, and Qdrant outbox) is in `docs/RUNBOOK.md`.
