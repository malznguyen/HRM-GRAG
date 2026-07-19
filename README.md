# GMRAG

GMRAG is a multi-tenant GraphRAG platform under active refactor.

Current live architecture after Phase 4 API consistency and defense-in-depth closure:

- tenant and workspace hierarchy in PostgreSQL,
- OpenFGA as the authorization source of truth,
- Keycloak-only login, verified-user directory, and tenant/workspace onboarding,
- original PDF/DOCX/TXT/MD storage in MinIO/S3 through `aws-sdk-s3` v1,
- Qdrant vector retrieval for ACL-aware chunk search,
- PostgreSQL graph store: ACL-aware graph retrieval ranks document-scoped `graph_node_sources.embedding` after visible-provenance filtering and falls back to source-scoped text search; `graph_nodes.embedding` remains global compatibility/read-model data (HNSW `vector_l2_ops`; not ACL-filtered content fallback; legacy NULL global nodes need operator `backfill-graph-node-embeddings`),
- document-level ACL fully live; Phase 3A operator workers (outbox, storage cleanup, invite-placeholder cleanup) available.

## Current Docs

Use these files as the source of truth:

- `docs/GMRAG-refactor-doc.md`
- `docs/CURRENT_ARCHITECTURE.md`
- `docs/CURRENT_DATABASE_SCHEMA.md`
- `docs/CURRENT_API_CONTRACT.md`
- `docs/CURRENT_FRONTEND.md`
- `docs/RUNBOOK.md`

Historical v1 audit snapshots are archived under `docs/archive/v1/`.

## Current Architecture Snapshot

| Area | Current state |
| --- | --- |
| Identity and admin lookup | Keycloak-only OIDC issuer and verified-user directory; access-token `sub` is the canonical SQL/OpenFGA subject. **Phase 0A ✅ COMPLETE** — API identity E2E + browser PKCE smoke + full cleanup verified. |
| Authorization | OpenFGA via `AuthzClient` and route-level relation checks |
| Original document storage | S3-compatible object storage; MinIO in local development |
| Current retrieval path | **Chunk:** Qdrant (ACL-aware). **Graph:** ACL-aware retrieval ranks document-scoped `graph_node_sources.embedding` after visible-provenance filtering and falls back to source-scoped text search. `graph_nodes.embedding` remains global compatibility/read-model data and is not used as ACL-filtered graph content fallback. |
| Chat Sandbox | SSE chat with persisted sessions/messages, assistant-only GFM, shared session fishbone/thread state, and ACL-filtered citation resolution into per-message chips plus the newest-response References panel. |
| Knowledge Graph | Interactive Sigma/Graphology workspace graph with ACL-filtered nodes and links, bounded client layout, search, selection, and detail inspection. |
| Phase 3A hardening | ✅ complete — authz outbox processor, storage cleanup, invite-placeholder cleanup, audit trail (see `docs/RUNBOOK.md`) |
| Phase 1 durable ingestion | ✅ complete — API enqueue only; independent worker claim/lease, retry/backoff, stable Qdrant replay, restart recovery, and terminal failure states (see `docs/RUNBOOK.md` §11) |
| Phase 3B/3C | 🟡 partial — Qdrant lifecycle/outbox (claim, backoff, `DEAD`) + orphan cleanup shipped; daemon orchestration and other follow-up remain (see `docs/authz-refactor-taskboard.md`) |
| Phase 4 | ✅ complete — shared JSON API errors, hidden-not-found ACL behavior, operator-only workspace-admin recovery, route contract coverage, and ADR-25 (PostgreSQL RLS deferred) |

## Local Services

`docker-compose.yml` currently starts these local services:

- app Postgres
- OpenFGA Postgres
- OpenFGA migrate
- OpenFGA server
- MinIO
- minio-init bucket creation
- Qdrant (REST `6333`, gRPC `6334`)
- Ollama (`11434`, provisioned with the ADR-21 q8_0 GGUF)
- Keycloak (`8080`, `start-dev` + realm import for browser OIDC and Admin API)
- `process-authz-outbox` (loop drain of `authz_outbox`; requires `OPENFGA_STORE_ID`)
- `process-qdrant-outbox` (loop drain of `qdrant_outbox`; multi-replica claim-safe)
- `ingestion-worker` (durable ingestion poller; no Keycloak/JWT dependency)

**Local-demo only.** Docker Compose is the only supported orchestration. Workers are built from `docker/outbox-workers.Dockerfile` (existing Rust binaries). They are not staging/production readiness proof.

**Worker bootstrap note:** create an OpenFGA store (and optional model), then set `OPENFGA_STORE_ID` (and optionally `OPENFGA_MODEL_ID`) in a root `.env` or the shell before/while compose runs. Until `OPENFGA_STORE_ID` is set, `process-authz-outbox` will exit and restart (`restart: unless-stopped`).

**Canonical local topology:** Compose owns the infrastructure and workers, including
Ollama and `ingestion-worker`. `gmrag-api` and the Next.js frontend remain host-run
and use the published localhost ports. There is intentionally no API image or
`gmrag-api` Compose service.

The exact model name is `AITeamVN/Vietnamese_Embedding`. Local demo imports the
named third-party `model-q8_0.gguf` through `docker/ollama/Modelfile`; it returns
the model-native **1024** dimensions. `QDRANT_VECTOR_SIZE=1024` is shared by
Postgres guards, Qdrant, ingestion, and query embedding. Review the artifact trust
caveat and exact commands in `docs/RUNBOOK.md` §6 before provisioning it.

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
- OpenFGA client timeouts (authz hot path, fail-closed): `OPENFGA_CONNECT_TIMEOUT_SECS` (default `2`), `OPENFGA_REQUEST_TIMEOUT_SECS` (default `3`)
- Authz outbox worker: `AUTHZ_OUTBOX_BATCH_SIZE`, `AUTHZ_OUTBOX_MAX_RETRIES`
- Qdrant: `QDRANT_URL`, `QDRANT_COLLECTION`, `QDRANT_VECTOR_SIZE`, optional `QDRANT_API_KEY`
- Qdrant delete timeouts: `QDRANT_DELETE_REQUEST_TIMEOUT_SECS` (HTTP path), `QDRANT_DELETE_WORKER_TIMEOUT_SECS` (outbox/cleanup)
- Qdrant outbox worker: `QDRANT_OUTBOX_BATCH_SIZE`, `QDRANT_OUTBOX_MAX_RETRIES`, `QDRANT_OUTBOX_BASE_BACKOFF_SECS`, `QDRANT_OUTBOX_MAX_BACKOFF_SECS`, `QDRANT_OUTBOX_CLAIM_LEASE_SECS`
- Keycloak admin lookup: `KEYCLOAK_ADMIN_URL`, `KEYCLOAK_REALM`, `KEYCLOAK_CLIENT_ID`, `KEYCLOAK_CLIENT_SECRET`
- Auth-layer client timeouts (request-blocking, fail-closed): Keycloak admin `KEYCLOAK_CONNECT_TIMEOUT_SECS` / `KEYCLOAK_REQUEST_TIMEOUT_SECS` (defaults `3`/`5`); JWKS fetch `JWT_JWKS_CONNECT_TIMEOUT_SECS` / `JWT_JWKS_REQUEST_TIMEOUT_SECS` (defaults `3`/`5`)
- S3/MinIO: `S3_ENDPOINT_URL`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_FORCE_PATH_STYLE`, `S3_PRESIGN_EXPIRY_SECS`
- DeepSeek and Ollama settings for chat, graph extraction, and embeddings (`OLLAMA_EMBED_MODEL=AITeamVN/Vietnamese_Embedding` recommended)

## Local Bootstrap

1. Provision the q8_0 GGUF, import the exact Ollama model name, remove the stale
   768-d Qdrant collection, and apply the forward migration by following
   `docs/RUNBOOK.md` §6 in order.

2. Start the canonical Compose topology:

   ```bash
   docker compose up -d --build
   ```

3. Configure backend environment and run the API on the host:

   ```bash
   cd gmrag_api
   cargo run
   ```

   The default API bind is `127.0.0.1:8083` (`API_BIND_ADDR`), leaving local
   Keycloak on `127.0.0.1:8080`.

   A successful upload only enqueues work. The Compose `ingestion-worker` owns
   processing and waits for a healthy Ollama before its startup embedding probe.

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
# Durable Phase 1 ingestion is the root Compose `ingestion-worker` service
cargo run --bin recover-stale-ingestion-jobs -- --dry-run
cargo run --bin recover-stale-ingestion-jobs -- --apply

# Phase 3A — authz + storage + invite cleanup
cargo run --bin process-authz-outbox
# Unattended (OPS-001 local demo): root docker-compose service process-authz-outbox
# (--loop, restart unless-stopped). See docs/RUNBOOK.md §2.2. Not production proof.
cargo run --bin cleanup-storage-objects -- --dry-run
cargo run --bin cleanup-storage-objects -- --delete-orphans --delete
cargo run --bin cleanup-storage-objects -- --workspace-id <workspace_uuid> --delete
cargo run --bin cleanup-storage-objects -- --tenant-id <tenant_uuid> --delete
cargo run --bin cleanup-invite-placeholders -- --dry-run
cargo run --bin cleanup-invite-placeholders -- --delete
cargo run --bin backfill-document-workspace-tuples
cargo run --bin report-identity-consistency
cargo run --locked --bin delete-tenant -- --tenant-id <tenant_uuid> --dry-run
cargo run --locked --bin delete-tenant -- --tenant-id <tenant_uuid> --delete --yes
cargo run --locked --bin report-authz-orphans -- --dry-run
cargo run --locked --bin cleanup-authz-orphans -- --dry-run
cargo run --locked --bin cleanup-authz-orphans -- --delete --yes

# Phase 3B — Qdrant lifecycle / recovery
cargo run --bin process-qdrant-outbox
# Unattended (OPS-002 local demo): root docker-compose service process-qdrant-outbox
# (--loop; multi-replica safe via SKIP LOCKED + lease). See docs/RUNBOOK.md §7.2b.
# Not production proof.
cargo run --bin process-storage-outbox
cargo run --bin cleanup-qdrant-orphans -- --dry-run
cargo run --bin cleanup-qdrant-orphans -- --delete
cargo run --bin cleanup-qdrant-orphans -- --full-scan --dry-run

# Graph node embeddings (legacy NULL backfill)
cargo run --bin backfill-graph-node-embeddings -- --dry-run
cargo run --bin backfill-graph-node-embeddings -- --apply
```

- **Qdrant outbox:** claim with `FOR UPDATE SKIP LOCKED` + lease, exponential backoff on `FAILED`, status `DEAD` for poison/exhausted retries. Unattended local-demo drain: root Compose `process-qdrant-outbox` (OPS-002). See `docs/RUNBOOK.md` §7 / §7.2b (env vars, DEAD inspection, dual-write caveat).
- **Orphan cleanup (LIFE-006):** storage full list vs SQL (`cleanup-storage-objects`), Qdrant outbox/audit/`--full-scan` (`cleanup-qdrant-orphans`), and OpenFGA tuple-to-SQL report/cleanup tools. Default dry-run; mutations require explicit confirmation flags. Local-demo Qdrant outbox drain = root Compose (OPS-002); OPS-003 schedules only the dry-run storage scanner, not `process-storage-outbox`.
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
cargo test --locked --test phase4_api_contract -- --test-threads=1
cargo test --locked --test workspace_admin_recovery -- --test-threads=1
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

## Local frontend role fixtures

For local frontend demos only, seed seven persistent Keycloak identities plus a
two-tenant / three-workspace fixture that exercises cross-tenant and
cross-workspace isolation. The script preserves Keycloak as the login page and
uses the existing authenticated API/operator flows; it does not add a backend
password login or frontend role flags.

```powershell
$env:APP_ENV = "test"
./scripts/seed-local-test-users.ps1
```

| Username | Password | Domain role |
| --- | --- | --- |
| `super.test` | `Test1234!` | Platform Admin |
| `owner.test` | `Test1234!` | Tenant A Owner |
| `owner2.test` | `Test1234!` | Tenant B Owner |
| `wsadmin.test` | `Test1234!` | Workspace A1 Admin |
| `member.test` | `Test1234!` | Workspace A1 Member |
| `member2.test` | `Test1234!` | Workspace A2 Member |
| `outsider.test` | `Test1234!` | Authenticated outsider |

Fixture topology:

- **Tenant A** `GMRAG Test Tenant` (owner `owner.test`)
  - **Workspace A1** `GMRAG Test Workspace` — `wsadmin.test` (admin), `member.test` (member)
  - **Workspace A2** `GMRAG Test Workspace Beta` — `member2.test` (member)
- **Tenant B** `GMRAG Test Tenant Bravo` (owner `owner2.test`)
  - **Workspace B1** `GMRAG Test Workspace Bravo`

The seed verifies cross-workspace isolation (A1 vs A2 in the same tenant),
cross-tenant isolation (A vs B), and that a tenant owner sees every workspace in
their own tenant but none in another tenant.

Login continues to redirect to the Keycloak-hosted Authorization Code + PKCE
page for the public `gmrag-frontend` client. These credentials are local test
data only and must not be used outside local/test environments. See
`docs/RUNBOOK.md` for dry-run, reset, and verification steps.

## Current Implementation Notes

- Original PDFs live in MinIO/S3; they are no longer stored on local disk as the source of truth.
- Document delete revokes OpenFGA first, then commits SQL cleanup + `storage_outbox` / `qdrant_outbox` in one transaction; storage/Qdrant cleanup is best-effort after commit (LIFE-001 / LIFE-003).
- Workspace delete revokes its OpenFGA subtree first, then commits SQL cleanup + `storage_outbox` (`delete_prefix`) / `qdrant_outbox`; Qdrant cleanup is best-effort and there is no request-path S3 prefix call (LIFE-001 / LIFE-004).
- Retry reads the original object from storage and returns `DOCUMENT_OBJECT_MISSING` when the object is gone.
- Document-level ACL enforcement, restricted-document behavior, and Qdrant retrieval are fully implemented and verified.
- Delete/revoke/downgrade is FGA-first; grant/promote must commit SQL before FGA or provide compensation. External cleanup is not a distributed transaction. Recovery: `process-storage-outbox` / `process-qdrant-outbox` / orphan cleanup tools — see `docs/RUNBOOK.md` §3 and §7. OPS-003 schedules the dry-run scanner; LIFE-005 ships an operator tenant cascade but no public DELETE route.
- Operator runbook for Phase 2/3A/3B (including Ollama, HNSW, graph node embedding backfill, and Qdrant outbox) is in `docs/RUNBOOK.md`.
- HTTP application failures use the shared JSON envelope `{ "error": { "code", "message", "details"? } }`; successful `204` responses intentionally remain bodyless. `recover-workspace-admin` is an operator-only CLI, never an HTTP bypass.
