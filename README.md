# GMRAG

GMRAG is a multi-tenant GraphRAG platform under active refactor.

Current live architecture after Phase 2 + Phase 3A hardening:

- tenant and workspace hierarchy in PostgreSQL,
- OpenFGA as the authorization source of truth,
- Keycloak-only login, verified-user directory, and tenant/workspace onboarding,
- original PDF storage in MinIO/S3 through `aws-sdk-s3` v1,
- Qdrant vector retrieval for ACL-aware chunk search,
- PostgreSQL graph store: ACL-aware graph retrieval ranks document-scoped `graph_node_sources.embedding` after visible-provenance filtering and falls back to source-scoped text search; `graph_nodes.embedding` remains global compatibility/read-model data (HNSW `vector_l2_ops`; not ACL-filtered content fallback; legacy NULL global nodes need operator `backfill-graph-node-embeddings`),
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
| Identity and admin lookup | Keycloak-only OIDC issuer and verified-user directory; access-token `sub` is the canonical SQL/OpenFGA subject. **Phase 0A ✅ COMPLETE** — API identity E2E + browser PKCE smoke + full cleanup verified. |
| Authorization | OpenFGA via `AuthzClient` and route-level relation checks |
| Original document storage | S3-compatible object storage; MinIO in local development |
| Current retrieval path | **Chunk:** Qdrant (ACL-aware). **Graph:** ACL-aware retrieval ranks document-scoped `graph_node_sources.embedding` after visible-provenance filtering and falls back to source-scoped text search. `graph_nodes.embedding` remains global compatibility/read-model data and is not used as ACL-filtered graph content fallback. |
| Phase 3A hardening | ✅ complete — authz outbox processor, storage cleanup, invite-placeholder cleanup, audit trail (see `docs/RUNBOOK.md`) |
| Phase 1 durable ingestion | ✅ complete — API enqueue only; independent worker claim/lease, retry/backoff, stable Qdrant replay, restart recovery, and terminal failure states (see `docs/RUNBOOK.md` §11) |
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
- Keycloak (`8080`, `start-dev` + realm import for browser OIDC and Admin API)

**Ollama is not in Docker.** Install Ollama on the host and pull the ADR-21 embedding model before ingestion or chat retrieval:

```bash
ollama pull AITeamVN/Vietnamese_Embedding
```

This model is the default for chunk embedding (ingestion), graph node embedding (ingestion forward-path), and query embedding (chat). Runtime output is **768** dimensions (matches Postgres `vector(768)` and Qdrant). The HuggingFace model card lists 1024-d; the Ollama registry copy used here is verified at **768-d** — do not change schema solely because of the card (see `docs/RUNBOOK.md` §6). Override with `OLLAMA_EMBED_MODEL` only if you accept retrieval-quality risk and a full re-embed.

**Keycloak scope in local compose:** Keycloak is the only login IdP, bearer-token issuer, and verified-user directory. The `gmrag-frontend` public client uses Authorization Code Flow with PKCE S256, `fullScopeAllowed=false`, and explicit default scopes (`basic`, `profile`, `email`, `roles` — `basic` required so access tokens include `sub`); `gmrag-api` is the required token audience. The backend validates `JWT_ISSUER`, `JWT_AUDIENCE`, and `JWT_JWKS_URL`. Required invariant: access-token `sub` must equal Keycloak Admin `user.id` and is the canonical id for SQL and OpenFGA. Workspace public API roles are only `member`|`admin` (Tenant Owner is not a workspace role).

Two local identity harnesses (do not conflate):

| Harness | Script | Client | Proves |
| --- | --- | --- | --- |
| API identity E2E | `scripts/run-keycloak-identity-e2e.ps1` | local-test-only confidential `gmrag-e2e` | token validation, `sub` equality, SQL/OpenFGA, role flows, `/users/sync`, tracked cleanup |
| Browser PKCE smoke | `scripts/run-keycloak-browser-smoke.ps1` | public `gmrag-frontend` | Authorization Code + PKCE S256, session, protected API, logout (auto-starts frontend if port 3000 free) |

Direct Grant is disabled for `gmrag-frontend` and must never be used by the browser or production user flow. A local-test-only confidential client may use it solely for backend API identity automation. API E2E does **not** prove browser PKCE.

Local Keycloak admin console: `http://localhost:8080` (default demo credentials `admin` / `admin`). Realm `gmrag` and confidential client `gmrag-admin-client` are imported from `docker/keycloak/gmrag-realm.json` on first start.

## Backend Environment

Copy `gmrag_api/.env.example` to `gmrag_api/.env` and fill in the values for:

- database connection
- Keycloak JWT: `JWT_ISSUER`, `JWT_AUDIENCE`, `JWT_JWKS_URL`
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

   The default API bind is `127.0.0.1:8083` (`API_BIND_ADDR`), leaving local
   Keycloak on `127.0.0.1:8080`.

   Run durable ingestion separately (a successful upload only enqueues work):

   ```bash
   cargo run --bin ingestion-worker
   # one polling pass for development/ops
   cargo run --bin ingestion-worker -- --once
   ```

4. Bootstrap a platform admin tuple in OpenFGA for a real user id:

   ```bash
   cargo run --bin seed-platform-admin -- --user-id=<keycloak_sub_id>
   ```

   Or resolve a verified Keycloak account safely:

   ```bash
   cargo run --bin seed-platform-admin -- --email=<verified_email>
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
# Durable Phase 1 ingestion
cargo run --bin ingestion-worker -- --worker-id local-worker
cargo run --bin recover-stale-ingestion-jobs -- --dry-run
cargo run --bin recover-stale-ingestion-jobs -- --apply

# Phase 3A — authz + storage + invite cleanup
cargo run --bin process-authz-outbox
cargo run --bin cleanup-storage-objects -- --dry-run
cargo run --bin cleanup-storage-objects -- --delete-orphans --delete
cargo run --bin cleanup-storage-objects -- --workspace-id <workspace_uuid> --delete
cargo run --bin cleanup-storage-objects -- --tenant-id <tenant_uuid> --delete
cargo run --bin cleanup-invite-placeholders -- --dry-run
cargo run --bin cleanup-invite-placeholders -- --delete
cargo run --bin backfill-document-workspace-tuples
cargo run --bin report-identity-consistency

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
cargo fmt --check
cargo check --locked
cargo test --locked
cargo test --locked --test authz_integration -- --test-threads=1
cargo test --locked --test document_acl_phase2_integration -- --test-threads=1
cargo test --locked --test storage_integration -- --nocapture
cargo test --locked --test phase3a_hardening_integration -- --test-threads=1
cargo test --locked --test graph_node_embedding_backfill_integration -- --test-threads=1
```

Real-Keycloak identity E2E (requires local Docker Keycloak/Postgres/OpenFGA and a running API with real JWT config, not bypass):

```powershell
$env:APP_ENV = "test"
$env:ALLOW_IDENTITY_E2E = "1"
$env:KEYCLOAK_ADMIN = "admin"
$env:KEYCLOAK_ADMIN_PASSWORD = "admin"
./scripts/run-keycloak-identity-e2e.ps1
./scripts/run-keycloak-browser-smoke.ps1
```

Identity E2E cleanup removes tracked SQL rows, Keycloak `e2e_` users, and OpenFGA tuples written by the run (set `KEEP_E2E_DATA=1` to retain). Browser smoke manages its own frontend process lifecycle when port 3000 is free.

## Current Implementation Notes

- Original PDFs live in MinIO/S3; they are no longer stored on local disk as the source of truth.
- Document delete performs SQL cleanup first and storage cleanup second on a best-effort basis.
- Retry reads the original object from storage and returns `DOCUMENT_OBJECT_MISSING` when the object is gone.
- Document-level ACL enforcement, restricted-document behavior, and Qdrant retrieval are fully implemented and verified.
- Document/workspace delete is SQL-first then **best-effort** Qdrant filter-delete + outbox enqueue (not a distributed transaction). Recovery: `process-qdrant-outbox` / `cleanup-qdrant-orphans` — see `docs/RUNBOOK.md` §7.
- Operator runbook for Phase 2/3A/3B (including Ollama, HNSW, graph node embedding backfill, and Qdrant outbox) is in `docs/RUNBOOK.md`.
