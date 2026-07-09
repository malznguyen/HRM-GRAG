# GMRAG

GMRAG is a multi-tenant GraphRAG platform under active refactor.

Current live architecture after Phase 1.5:

- tenant and workspace hierarchy in PostgreSQL,
- OpenFGA as the authorization source of truth,
- Keycloak-backed tenant-owner bootstrap and workspace member addition (verified users only),
- original PDF storage in MinIO/S3 through `aws-sdk-s3` v1,
- Qdrant vector retrieval for ACL-aware search,
- document-level ACL and Qdrant retrieval are now live in Phase 2.

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
| Current retrieval path | Qdrant vector search (ACL-aware filtering) and PostgreSQL graph tables |
| Planned Phase 3 hardening | Outbox processing and cleanup workers |

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

**Keycloak scope in local compose:** the container is for **Admin API lookup** used by tenant-owner bootstrap and workspace member addition (`KeycloakClient.get_verified_user_by_email`). JWT validation still uses `CLERK_ISSUER` / the existing `test-bypass-jwt` path — Keycloak is **not** used for login or bearer-token validation in the current backend.

Local Keycloak admin console: `http://localhost:8080` (default demo credentials `admin` / `admin`). Realm `gmrag` and confidential client `gmrag-admin-client` are imported from `docker/keycloak/gmrag-realm.json` on first start.

## Backend Environment

Copy `gmrag_api/.env.example` to `gmrag_api/.env` and fill in the values for:

- database connection
- bearer-token issuer and JWKS configuration used by the current backend
- OpenFGA: `OPENFGA_API_URL`, `OPENFGA_STORE_ID`, `OPENFGA_MODEL_ID`
- Outbox worker: `AUTHZ_OUTBOX_BATCH_SIZE`, `AUTHZ_OUTBOX_MAX_RETRIES`
- Keycloak admin lookup: `KEYCLOAK_ADMIN_URL`, `KEYCLOAK_REALM`, `KEYCLOAK_CLIENT_ID`, `KEYCLOAK_CLIENT_SECRET`
- S3/MinIO: `S3_ENDPOINT_URL`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_FORCE_PATH_STYLE`, `S3_PRESIGN_EXPIRY_SECS`
- DeepSeek and Ollama settings for chat, graph extraction, and embeddings

## Local Bootstrap

1. Start local infrastructure:

   ```bash
   docker compose up -d
   ```

2. Configure backend environment:

   ```bash
   cd gmrag_api
   cargo run
   ```

3. Bootstrap a platform admin tuple in OpenFGA for a real user id:

   ```bash
   cargo run --bin seed-platform-admin -- --user-id=<keycloak_sub_id>
   ```

4. **Member management (breaking change vs invite placeholders):** users must **sign up and verify email in identity first**, then a workspace admin can add them by email via `POST /workspaces/{id}/members`. The API resolves the real Keycloak `sub` and writes SQL + OpenFGA together. Adding someone who has not registered returns `USER_NOT_FOUND_IN_IDENTITY`. There is no invite-placeholder / pending-member flow — Keycloak is the identity source of truth so SQL and OpenFGA never desync on fake `invite_*` ids.

5. **Upgrade cleanup (legacy DBs only):** if this environment previously used invite placeholders, run the operator cleanup **after deploying** the build that stopped creating them:

   ```bash
   cargo run --bin cleanup-invite-placeholders -- --dry-run
   cargo run --bin cleanup-invite-placeholders -- --delete
   ```

   Default is dry-run; nothing is deleted without `--delete`. Safe to re-run. See `docs/RUNBOOK.md` §5.

## Phase 3A Hardening Commands

Run these from `gmrag_api/`:

```bash
cargo run --bin process-authz-outbox
cargo run --bin cleanup-storage-objects -- --dry-run
cargo run --bin cleanup-storage-objects -- --delete-orphans --delete
cargo run --bin cleanup-storage-objects -- --workspace-id <workspace_uuid> --delete
cargo run --bin cleanup-storage-objects -- --tenant-id <tenant_uuid> --delete
cargo run --bin cleanup-invite-placeholders -- --dry-run
cargo run --bin cleanup-invite-placeholders -- --delete
cargo run --bin backfill-document-workspace-tuples
```

## Test Commands

Run these from `gmrag_api/`:

```bash
cargo check
cargo test
cargo test --test authz_integration
cargo test --test storage_integration
cargo test --test phase3a_hardening_integration
```

## Current Implementation Notes

- Original PDFs live in MinIO/S3; they are no longer stored on local disk as the source of truth.
- Document delete performs SQL cleanup first and storage cleanup second on a best-effort basis.
- Retry reads the original object from storage and returns `DOCUMENT_OBJECT_MISSING` when the object is gone.
- Document-level ACL enforcement, restricted-document behavior, and Qdrant retrieval are fully implemented and verified.
- A pre-deploy runbook section for the backfill command is available in `docs/CURRENT_ARCHITECTURE.md`.
