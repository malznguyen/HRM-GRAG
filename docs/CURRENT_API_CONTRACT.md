# Current API Contract

This document reflects the routes registered by `gmrag_api/src/lib.rs` after Phase 4
API consistency and defense-in-depth closure.
Historical audit snapshots are archived under `docs/archive/v1/`.

**As of Phase 4 source baseline:** `3c865ebaf542c5880b1a83462452ce206b4719bb`.

Phase 3 adds operational endpoints for deployment/runtime observability:

- `GET /ready` for dependency-aware readiness checks.
- `GET /metrics` for Prometheus scrape output.

Operator-only binaries (including `backfill-graph-node-embeddings`) are still
documented in `docs/RUNBOOK.md`, not as HTTP routes.

Some existing write/delete endpoints now emit metadata-only audit events and may
enqueue internal authz outbox recovery rows on non-blocking OpenFGA sync failures.
These are internal side effects and do not change request/response contracts.

## Cross-Cutting Behavior

### Base URL

- Local backend bind: `http://127.0.0.1:8083` by default (`API_BIND_ADDR`); Keycloak owns local port `8080`.
- There is no version prefix.

### Authentication

- Protected routes use `Authorization: Bearer <Keycloak access token>`.
- The backend validates RS256 signature, issuer, audience, expiry, and non-empty subject through Keycloak JWKS.
- Required invariant: JWT `sub` must equal Keycloak Admin `user.id` and is the canonical user id for SQL and OpenFGA.
- **Phase 0A ✅ COMPLETE** evidence is two separate harnesses (see `docs/RUNBOOK.md` §0):
  - API identity E2E (`scripts/run-keycloak-identity-e2e.ps1`, local-test-only confidential `gmrag-e2e`) proves backend token validation, subject equality, SQL/OpenFGA consistency, role flows, and `/users/sync`.
  - Browser PKCE smoke (`scripts/run-keycloak-browser-smoke.ps1`, public `gmrag-frontend`) proves Authorization Code + PKCE end-user login/session/logout. API E2E does not prove browser PKCE.
- Direct Grant is disabled for `gmrag-frontend` and must never be used by the browser or production user flow. A local-test-only confidential client may use it solely for backend API identity automation.
- Keycloak Admin API is used for verified-user lookup on `POST /tenants/{tenant_id}/owners` and `POST /workspaces/{workspace_id}/members`.

### Authorization

Current route enforcement is OpenFGA-based through `Authz.require_relation(...)`.

Current relation-to-error mapping for `403` responses:

| Relation check | Current JSON error envelope |
| --- | --- |
| `admin` | `{"error":{"code":"WORKSPACE_ADMIN_REQUIRED","message":"Workspace admin access required"}}` |
| `owner` | `{"error":{"code":"TENANT_OWNER_REQUIRED","message":"Tenant owner access required"}}` |
| `can_manage_member` | `{"error":{"code":"MEMBER_MANAGEMENT_DENIED","message":"Workspace admin or tenant owner access required to manage members"}}` |
| `can_assign_role` | `{"error":{"code":"ROLE_ASSIGNMENT_DENIED","message":"Only tenant owners can assign roles"}}` |
| `member` and other default cases | `{"error":{"code":"FORBIDDEN","message":"Access denied"}}` |

A `403` above means OpenFGA answered "not allowed". When the OpenFGA **dependency
itself** fails — unreachable, error response, or **timeout** (the OpenFGA client has
explicit short connect/request timeouts; see `docs/RUNBOOK.md`) — the request
**fails closed** with `500` `AUTHZ_ERROR` (`{"error":{"code":"AUTHZ_ERROR","message":"Authorization service unavailable"}}`).
An authz-dependency failure is **never** treated as "allow".

### Error Response Format

Every HTTP application failure returns the JSON envelope shown below. This includes
handler failures, JWT/authz extraction failures, Axum request rejections, rate
limits, unknown routes, wrong methods, readiness failures, and initial chat
generation failures before SSE starts. Client messages are sanitized and never
contain raw SQL, OpenFGA, Keycloak, storage, or provider response bodies.

```json
{
  "error": {
    "code": "STABLE_MACHINE_CODE",
    "message": "Safe client-facing message",
    "details": {}
  }
}
```

`details` is omitted unless safe and useful. `GET /ready` returns `503
SERVICE_UNAVAILABLE` with its dependency report in `error.details`. Success
`204` responses and successful `/metrics` Prometheus output intentionally do
not use this envelope.

### Rate Limiting

- Current middleware applies in-memory sliding-window limits on selected routes:
  - `POST /workspaces/{workspace_id}/chat`
  - `POST /workspaces/{workspace_id}/documents/upload`
  - auth-sensitive routes: `POST /users/sync`, `POST /tenants`, `POST /tenants/{tenant_id}/owners`, `POST /workspaces/{workspace_id}/members`, `PATCH /workspaces/{workspace_id}/members/{member_id}`, `DELETE /workspaces/{workspace_id}/members/{member_id}`.
- User-scoped keys are derived from verified JWT `sub`; tenant-owner bootstrap route uses tenant-scoped keys (`tenant_id`).
- Limit breach returns `429` JSON envelope:
  - `{"error":{"code":"RATE_LIMITED","message":"Too many requests. Please retry later."}}`
- Config envs: `RATE_LIMIT_WINDOW_SECS`, `RATE_LIMIT_CHAT_PER_WINDOW`, `RATE_LIMIT_UPLOAD_PER_WINDOW`, `RATE_LIMIT_AUTH_PER_WINDOW`.

### Current Storage Contract

- Upload stores original PDF, DOCX, TXT, and MD files in MinIO/S3 through the storage module.
- Retry reads from MinIO/S3 and does not depend on local-file existence.
- Delete revokes the captured OpenFGA subtree first, then commits SQL cleanup
  and durable cleanup outboxes; object/Qdrant cleanup remains best-effort after
  commit.
- Public API responses do not expose raw storage object keys.

## Endpoint Inventory

All endpoint-specific error bullets below define only the relevant HTTP status
and stable domain code; the cross-cutting Phase 4 error contract applies to
every application failure.

| Method | Path | Authorization |
| --- | --- | --- |
| `GET` | `/health` | public |
| `GET` | `/ready` | public |
| `GET` | `/metrics` | public |
| `GET` | `/users/me` | authenticated user |
| `POST` | `/users/sync` | authenticated user |
| `GET` | `/me/tenants` | authenticated user; SQL owner candidates intersected with OpenFGA `owner` |
| `GET` | `/tenants` | `admin` on `platform:system` |
| `POST` | `/tenants` | `admin` on `platform:system` |
| `POST` | `/tenants/{tenant_id}/owners` | `admin` on `platform:system` |
| `POST` | `/tenants/{tenant_id}/workspaces` | `owner` on `tenant:{tenant_id}` |
| `GET` | `/workspaces` | authenticated user; SQL membership candidates intersected with OpenFGA `member` |
| `DELETE` | `/workspaces/{workspace_id}` | `can_assign_role` on `workspace:{workspace_id}` |
| `GET` | `/workspaces/{workspace_id}/documents` | `member` on `workspace:{workspace_id}` |
| `POST` | `/workspaces/{workspace_id}/documents/upload` | `admin` on `workspace:{workspace_id}` |
| `DELETE` | `/workspaces/{workspace_id}/documents/{document_id}` | `admin` on `workspace:{workspace_id}` |
| `POST` | `/workspaces/{workspace_id}/documents/{document_id}/retry` | `admin` on `workspace:{workspace_id}` |
| `PATCH` | `/workspaces/{workspace_id}/documents/{document_id}/access-mode` | `admin` on `workspace:{workspace_id}` |
| `GET` | `/workspaces/{workspace_id}/documents/{document_id}/shares` | `admin` on `workspace:{workspace_id}` |
| `POST` | `/workspaces/{workspace_id}/documents/{document_id}/shares/{user_id}` | `admin` on `workspace:{workspace_id}` |
| `DELETE` | `/workspaces/{workspace_id}/documents/{document_id}/shares/{user_id}` | `admin` on `workspace:{workspace_id}` |
| `PUT` | `/workspaces/{workspace_id}/documents/{document_id}/permissions` | `admin` on `workspace:{workspace_id}` |
| `GET` | `/workspaces/{workspace_id}/documents/{document_id}/preview` | `member` on `workspace:{workspace_id}` |
| `GET` | `/workspaces/{workspace_id}/chunks/{chunk_id}` | `member` on `workspace:{workspace_id}` |
| `POST` | `/workspaces/{workspace_id}/chat` | `member` on `workspace:{workspace_id}` plus chat-session ownership |
| `GET` | `/workspaces/{workspace_id}/chat/history` | `member` on `workspace:{workspace_id}` plus chat-session ownership |
| `GET` | `/workspaces/{workspace_id}/chat/sessions` | `member` on `workspace:{workspace_id}` |
| `GET` | `/workspaces/{workspace_id}/chat/sessions/{session_id}/messages` | `member` on `workspace:{workspace_id}` plus chat-session ownership |
| `DELETE` | `/workspaces/{workspace_id}/chat/sessions/{session_id}` | `member` on `workspace:{workspace_id}` plus chat-session ownership |
| `GET` | `/workspaces/{workspace_id}/graph` | `member` on `workspace:{workspace_id}` |
| `GET` | `/workspaces/{workspace_id}/members` | `member` on `workspace:{workspace_id}` |
| `POST` | `/workspaces/{workspace_id}/members` | `can_manage_member` on `workspace:{workspace_id}` |
| `PATCH` | `/workspaces/{workspace_id}/members/{member_id}` | `can_assign_role` on `workspace:{workspace_id}` |
| `DELETE` | `/workspaces/{workspace_id}/members/{member_id}` | `can_manage_member` on `workspace:{workspace_id}` |

## Health And Compatibility

### `GET /health`

- Auth: none.
- Authorization: none.
- Request body: none.
- Success: `200` with `{ status, db }`.
- Errors: `500 INTERNAL_ERROR` JSON envelope if `SELECT 1` fails.
- Side effects: none.
- Security notes: safe public liveness check only.

### `GET /ready`

- Auth: none.
- Authorization: none.
- Request body: none.
- Success: `200` with JSON `{ status: "ready", role, dependencies, failed_dependencies }` when every required dependency for `APP_RUNTIME_ROLE` is healthy.
- Errors: `503` JSON envelope `SERVICE_UNAVAILABLE` when one or more required dependencies fail. Its `error.details` is `{ status: "not_ready", role, dependencies, failed_dependencies }`.
- Dependency matrix by role: `api` requires PostgreSQL + OpenFGA; `ingestion-worker` requires PostgreSQL + Qdrant + object storage; `process-authz-outbox` requires PostgreSQL + OpenFGA; `process-qdrant-outbox` requires PostgreSQL + Qdrant; `storage-worker` requires PostgreSQL + object storage.
- Side effects: none.
- Security notes: dependency checks are metadata only (no mutation); this endpoint is intended for orchestrator readiness probes.

### `GET /metrics`

- Auth: none.
- Authorization: none.
- Request body: none.
- Success: `200` text format Prometheus exposition (`text/plain; version=0.0.4`) with runtime and operational metrics.
- Current metric groups: HTTP request count by `method`/`route`/`status`, model latency histogram (`gmrag_model_latency_seconds`), ingestion latency gauges (`avg`/`max`), ingestion failure count, and outbox depth gauges (`authz`/`qdrant`/`storage` by status).
- Side effects: no business-data mutation; scrape path may run read-only SQL aggregates/counts for operational gauges.
- Security notes: endpoint is intentionally unauthenticated for Prometheus scrape compatibility; operators must restrict network exposure at deployment boundary.

## User Endpoints

### `GET /users/me`

- Auth: bearer JWT.
- Authorization: any authenticated user.
- Request body: none.
- Success: `200` with the current SQL user row plus a boolean platform-admin flag derived from OpenFGA.
- Errors: `404 RESOURCE_NOT_FOUND`; `500 INTERNAL_ERROR`, both JSON envelopes.
- Side effects: none (invite reconciliation was removed from this path).
- Security notes: platform-admin status is derived from OpenFGA, not a SQL column.

### `POST /users/sync`

- Auth: bearer JWT.
- Authorization: any authenticated user.
- Request body: none.
- Success: `200` empty body.
- Errors: `400 IDENTITY_EMAIL_REQUIRED` or `IDENTITY_EMAIL_UNVERIFIED`; `409 IDENTITY_EMAIL_CONFLICT`; `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: upserts the SQL `users` row for verified JWT `sub` + verified `email` claim. It never accepts an email from the request body and never reconciles identities.

### `GET /me/tenants`

- Auth: bearer JWT.
- Authorization: any authenticated user; each SQL candidate from `tenant_members` with the caller's verified JWT `sub` and role `OWNER` is checked for OpenFGA `owner` on `tenant:{id}`.
- Request body: none.
- Success: `200` with an array of `{ id, name, created_at }` that pass both the SQL owner read model and OpenFGA owner check, ordered by `created_at DESC` with duplicates removed.
- Errors: `500 INTERNAL_ERROR` on SQL failure or `500 AUTHZ_ERROR` when an OpenFGA check fails; both use the shared JSON envelope.
- Side effects: none.
- Security notes: SQL is only a candidate source. Stale SQL owner rows without the matching OpenFGA relation are omitted, and an OpenFGA dependency failure is fail-closed without returning SQL-only data.

## Tenant And Workspace Lifecycle

The merged router exposes **33 method/path combinations**. The inventory above
counts methods separately when a path supports more than one method.
### `GET /tenants`

- Auth: bearer JWT.
- Authorization: `admin` on `platform:system`.
- Query parameters: optional `limit` (default `20`, range `0-100`), optional non-negative `offset` (default `0`), and optional trimmed `q`.
- Search: `q` is a case-insensitive partial match across tenant `name`, tenant UUID text, and owner email. Search is applied before `total`, ordering, and pagination are calculated.
- Success: `200` with `{ tenants, total, limit, offset }`. Each tenant is `{ id, name, created_at, owners }`; `owners` is an email-ordered array of `{ id, email }`. Tenants are ordered by `created_at DESC, id DESC`.
- Errors: `400 INVALID_REQUEST` for invalid pagination; authz `403`; `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: none.
- Security notes: the endpoint is a Platform Admin directory read. Owner lookup uses the SQL read model and does not grant tenant access.

### `POST /tenants`

- Auth: bearer JWT.
- Authorization: `admin` on `platform:system`.
- Request body: JSON `{ "name": "Tenant Name" }`.
- Success: `201` with `{ id, name, created_at }`.
- Errors: `400 INVALID_REQUEST`; authz `403`; `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: inserts the tenant row and writes the `platform -> tenant` tuple in OpenFGA.
- Security notes: creating a tenant does not grant the caller implicit business-data access inside its workspaces.

### `POST /tenants/{tenant_id}/owners`

- Auth: bearer JWT.
- Authorization: `admin` on `platform:system`.
- Request body: JSON `{ "email": "owner@example.com" }`.
- Success: `204` empty body.
- Errors: `400 INVALID_REQUEST`; authz `403`; `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: ensures a SQL `users` row exists, inserts `tenant_members`, and writes the `user owner tenant` tuple.
- Security notes: only verified users from Keycloak are accepted as tenant owners.

### `POST /tenants/{tenant_id}/workspaces`

- Auth: bearer JWT.
- Authorization: `owner` on `tenant:{tenant_id}`.
- Request body: JSON `{ "name": "Workspace Name" }`.
- Success: `201` with `{ id, name, created_at }`.
- Errors: `400 INVALID_REQUEST`; authz `403`; `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: inserts the workspace row, inserts a SQL `workspace_members` admin row for the creator, writes the `tenant -> workspace` tuple, and writes the workspace-admin tuple for the creator.
- Security notes: current workspace creation is tenant-owner only.

### `GET /workspaces`

- Auth: bearer JWT.
- Authorization: any authenticated user; each SQL candidate is then checked for OpenFGA `member` on `workspace:{id}` (admin and tenant owner inherit `member` via the model).
- Request body: none.
- Success: `200` with an array of `{ id, name, created_at }` that pass **both** SQL candidate membership (workspace_members or tenant OWNER) **and** OpenFGA `member`. Deterministic order by `created_at DESC`; duplicates removed.
- Errors: `500 INTERNAL_ERROR` on SQL failure or `500 AUTHZ_ERROR` if OpenFGA check fails; both are JSON envelopes and never return the SQL-only candidate list.
- Side effects: none.
- Security notes: SQL membership is a read-model candidate list only. Stale SQL rows without an OpenFGA relation are omitted. Platform Admin without a business relation is not listed.

### Tenant deletion (operator only)

There is no `DELETE /tenants/{tenant_id}` HTTP route. `delete-tenant` is the
active operator lifecycle: it captures the tenant subtree and writes a recovery
file, revokes matching OpenFGA tuples first, then commits the SQL cascade,
audit event, `qdrant_outbox` and `storage_outbox` in one PostgreSQL transaction.
Post-commit S3/Qdrant cleanup is best-effort; durable outboxes remain retryable.
If SQL fails after FGA revoke, the command exits `3` and identifies the recovery
file so the operator can retry deletion or restore tuples. See RUNBOOK §3.4.

Evidence: `gmrag_api/src/tenant_cleanup.rs:373-428,649-706` and
`gmrag_api/src/bin/delete-tenant.rs:109-158`.

### `DELETE /workspaces/{workspace_id}`

- Auth: bearer JWT.
- Authorization: `can_assign_role` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `204` empty body.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND`; `500 AUTHZ_REVOKE_FAILED`
  when subtree revoke fails before SQL mutation; `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: captures the workspace subtree, revokes its OpenFGA tuples first,
  then deletes the workspace row and inserts `qdrant_outbox`
  (`delete_by_workspace`), `storage_outbox` (`delete_prefix`), and audit metadata
  in one PostgreSQL transaction. After commit it best-effort deletes Qdrant
  points; no S3 prefix delete runs on the request path.
- Security notes: OpenFGA failure stops before SQL. If SQL fails after successful
  revoke, access remains denied (fail closed) and operator recovery may be
  required. S3/Qdrant failures after commit do not change the `204`; outbox rows
  provide durable recovery. Evidence: `gmrag_api/src/routes/workspaces.rs:493-548`.

## Workspace Members

### `GET /workspaces/{workspace_id}/members`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with an array of member rows containing user id, email, role, and join time.
- Errors: authz `403` or `500 INTERNAL_ERROR`, both JSON envelopes.
- Side effects: none.
- Security notes: role values are SQL read-model data; authorization still comes from OpenFGA.

### `POST /workspaces/{workspace_id}/members`

- Auth: bearer JWT.
- Authorization:
  1. Always require `can_manage_member` on `workspace:{workspace_id}` (Tenant Owner or Workspace Admin).
  2. If `role` is `admin`, also require `can_assign_role` (Tenant Owner only). Workspace Admin cannot create admins.
- Request body: JSON `{ "email": "user@example.com", "role": "member|admin" }` only. Aliases `user` / `owner` are **rejected**.
- Success: `201` with the inserted member row (`id` is the Keycloak `sub`). SQL role is `MEMBER` or `ADMIN`.
- Errors:
  | Status | Body | When |
  | --- | --- | --- |
  | `400` | JSON `INVALID_MEMBER_ROLE` | Role not exactly `member` or `admin` |
  | `400` | JSON `INVALID_EMAIL` | Invalid email |
  | `403` | JSON `MEMBER_MANAGEMENT_DENIED` | Caller lacks `can_manage_member` |
  | `403` | JSON `ROLE_ASSIGNMENT_DENIED` / message *Only tenant owners can assign workspace admin roles* | Workspace Admin (or other non-owner) requested `role=admin` |
  | `404` | JSON `USER_NOT_FOUND_IN_IDENTITY` | Keycloak has no **verified** user for that email |
  | `409` | JSON `ALREADY_WORKSPACE_MEMBER` | Membership row already exists |
  | `500` | JSON `INTERNAL_ERROR` | Keycloak lookup, SQL, or unexpected failure |
- Side effects (only after all authz checks pass):
  1. Looks up a **verified** Keycloak user by email.
  2. Upserts SQL `users` with Keycloak `sub` + email (no `invite_*`).
  3. Inserts `workspace_members` with normalized SQL role.
  4. Writes OpenFGA `admin` or `member` tuple; on post-commit FGA failure enqueues authz outbox recovery.
  5. Metadata-only `member_added` audit event.
- Security notes: denied admin assignment has **no** SQL/FGA/audit side effects. Tenant Owner is not a workspace role alias.

### `PATCH /workspaces/{workspace_id}/members/{member_id}`

- Auth: bearer JWT.
- Authorization: `can_assign_role` on `workspace:{workspace_id}` (Tenant Owner only).
- Request body: JSON `{ "role": "member|admin" }` only.
- Success: `204` empty body. Idempotent if already that role.
- Errors: `400 INVALID_MEMBER_ROLE`; authz `403`; `404 RESOURCE_NOT_FOUND`; `409 LAST_WORKSPACE_ADMIN`; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: serializes via workspace row lock (`FOR UPDATE`), reloads target role, last-admin guard on ADMIN→MEMBER, then updates SQL + swaps OpenFGA tuples before commit.
- Security notes: role changes are tenant-owner-only. Last-admin guard requires another verified workspace ADMIN (SQL + FGA) or a valid Tenant Owner (SQL OWNER + FGA owner on tenant).

### `DELETE /workspaces/{workspace_id}/members/{member_id}`

- Auth: bearer JWT.
- Authorization: `can_manage_member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `204` empty body — only after both OpenFGA revoke and SQL membership deletion succeed.
- Errors:
  - `400` JSON `CANNOT_REMOVE_SELF`
  - `403` JSON authz envelope
  - `404` JSON `RESOURCE_NOT_FOUND` if the member row is not found
  - `409` JSON `LAST_WORKSPACE_ADMIN` when removing the last valid management path
  - `500` JSON `AUTHZ_REVOKE_FAILED` when OpenFGA revoke fails — SQL is not mutated
  - `500` JSON `MEMBER_REMOVE_FAILED` when SQL deletion fails after a successful OpenFGA revoke
- Side effects (ordered, fail-closed, workspace row lock for admin targets):
  1. Lock workspace; reload role; last-admin guard if target is ADMIN.
  2. Delete the matching OpenFGA membership tuple first (`admin` or `member`). Missing tuple = idempotent success.
  3. Delete the SQL `workspace_members` row and commit.
  4. Write `member_removed` audit only after success.
  5. If SQL fails after OpenFGA revoke, enqueue authz outbox `tuple_write` recovery (temporary denial preferred over false success).
- Security notes: self-removal blocked. OpenFGA is authorization truth.

## Documents

### `GET /workspaces/{workspace_id}/documents`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Query parameters: optional `limit` (default `20`, range `0-100`), optional non-negative `offset` (default `0`), optional trimmed `q`, and optional exact `status` (`PROCESSING` | `COMPLETED` | `FAILED`).
- Request body: none.
- Search and pagination: `q` is a case-insensitive partial match on `filename`. Processing order is ACL visibility, then `q`/`status`, then `total`, then `created_at DESC, id DESC`, then `limit`/`offset`.
- Success: `200` with `{ documents, total, limit, offset }`. Each document contains `id`, `filename`, `status`, `processing_stage`, optional `failure_code`/`failure_message`, `created_at`, nullable `size_bytes`, `access_mode`, `uploaded_by`, nullable `uploaded_by_email`, and nullable `content_type`.
- Errors: `400 INVALID_REQUEST` for invalid pagination or status; authz `403`; `500 AUTHZ_ERROR` when OpenFGA visibility lookup fails; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: none.
- Security notes: `workspace_default` documents are visible to workspace members; `restricted` documents require explicit or bypass viewer access. OpenFGA supplies the restricted-document candidate set before SQL search/count/pagination, so `total` never reveals hidden restricted documents. Authz dependency failures are fail-closed. Storage internals (`object_key`, `bucket`, `checksum_sha256`, `storage_etag`) are not exposed.

### `POST /workspaces/{workspace_id}/documents/upload`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: `multipart/form-data` with one or more `file` parts, and an optional text field `access_mode` (`workspace_default` | `restricted`). Supported files are PDF, DOCX, TXT, and MD. When omitted, `access_mode` defaults to `workspace_default`. The same `access_mode` applies to every accepted file in the request.
- Success: `202` with `{ documents: [{ document_id, filename }] }` for the accepted files.
- Errors: `400 INVALID_REQUEST` when no acceptable file is accepted; `400 INVALID_ACCESS_MODE`; authz `403`; `404 RESOURCE_NOT_FOUND`; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: uploads original bytes to MinIO/S3, then atomically inserts `documents` metadata (including `access_mode`) and one durable `ingestion_jobs` row. Processing is performed by the separate ingestion worker after commit.
- Validation: PDF requires a `%PDF-` signature plus successful `lopdf` structural parsing. DOCX requires the ZIP local-file signature, a readable archive, `[Content_Types].xml`, and `word/document.xml`. TXT/MD require non-empty valid UTF-8 bytes with no NUL; because their content validators are identical, only the sanitized `.txt`/`.md` extension selects which server MIME is stored. `DOCUMENT_MAX_UPLOAD_BYTES` limits each file and the multipart request body (default `52428800`, 50 MiB).
- Security notes: client MIME is ignored for acceptance and persistence. `documents.content_type` is always one of the server-validated values `application/pdf`, `application/vnd.openxmlformats-officedocument.wordprocessingml.document`, `text/plain`, or `text/markdown`. Filename suffix does not establish PDF/DOCX identity and is used only to disambiguate valid TXT from MD. Responses do not expose raw object keys. Uploading as `restricted` does not auto-create `document_shares` or `explicit_viewer` tuples for the uploader; visibility follows the same restricted-document rules as after `PATCH .../access-mode`.

### `DELETE /workspaces/{workspace_id}/documents/{document_id}`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `204` empty body.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND`; `500 AUTHZ_REVOKE_FAILED`
  when document tuple revoke fails before SQL mutation; or `500 INTERNAL_ERROR`,
  all JSON envelopes.
- Side effects: captures document tuples and object metadata, revokes OpenFGA
  first, then removes graph provenance/document SQL and inserts audit,
  `qdrant_outbox` (`delete_by_document`) and `storage_outbox` (`delete_object`)
  in one SQL transaction. Storage and Qdrant cleanup are best-effort after commit;
  HTTP remains `204` if either external cleanup fails.
- Security notes: FGA revoke failure leaves SQL untouched. Cleanup recovery rows
  commit atomically with SQL deletion; object keys remain internal. Evidence:
  `gmrag_api/src/routes/documents.rs:725-765,825-848`.

### `POST /workspaces/{workspace_id}/documents/{document_id}/retry`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `202` empty body.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND`; `409 DOCUMENT_NOT_RETRYABLE`; `410 DOCUMENT_OBJECT_MISSING`; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: checks object existence in MinIO/S3, then atomically resets a `FAILED` document to `PROCESSING` / `QUEUED`, clears prior failure fields, and inserts exactly one durable job. Concurrent retries have one `202` winner.
- Invariant: only `COMPLETED` / `DONE` documents can contribute Qdrant chunks to chat retrieval. `INDEXING` is the final non-terminal processing stage.
- Security notes: retry is object-storage-based and does not depend on a local source file.

Ingestion lifecycle closure: the API returns `202` after the durable document
and job transaction commits; the independent worker owns parsing, persistence,
Qdrant indexing, retry/backoff, lease recovery, and terminal failure updates.
Retryable failures keep the document `PROCESSING` / `QUEUED`; non-retryable or
exhausted failures set `FAILED` / `FAILED` with a structured `failure_code`.

Stable document `failure_code` values currently emitted by the worker include:

| `failure_code` | Meaning | Auto-retry |
| --- | --- | --- |
| `DOCUMENT_OBJECT_MISSING` | Original object absent in storage | No |
| `NEEDS_OCR` | Page(s) need OCR and no usable OCR result is available (no production provider yet) | No |
| `PDF_PARSE_FAILED` | PDF could not be parsed or chunked | No |
| `DOCX_PARSE_FAILED` | DOCX archive/XML could not be parsed or produced no usable text | No |
| `TEXT_DECODE_FAILED` | TXT/MD could not be decoded, or stored `content_type` is missing/unsupported | No |
| `EMBEDDING_PROVIDER_UNAVAILABLE` | Embeddings failed | Yes (until max attempts) |
| `GRAPH_EXTRACTION_FAILED` | Graph extraction failed | Yes (until max attempts) |
| `QDRANT_INDEX_FAILED` | Vector index failed | Yes (until max attempts) |
| `DATABASE_SAVE_FAILED` | SQL persistence failed | Yes (until max attempts) |
| `INTERNAL_INGESTION_ERROR` | Other internal ingestion error | Yes (until max attempts) |
| `INGESTION_MAX_ATTEMPTS_EXCEEDED` | Retryable failure exhausted `max_attempts` | No (terminal after retries) |

`NEEDS_OCR` applies only to PDF and is non-retryable for the worker while no OCR provider is configured,
avoiding futile claim loops. After a future OCR provider/configuration change
(OCR-003+), operators may re-queue via `POST .../documents/{document_id}/retry`
(document must be terminal `FAILED`, object must still exist).
`GET /workspaces/{workspace_id}/documents` already surfaces optional
`failure_code` / `failure_message` on each row; no API schema change is required.

OCR-004 corpus audit/reingest planning is an **operator binary**
(`audit-ocr-affected-documents`), not a public REST endpoint. Dry-run is the
default and is fully read-only (no SQL mutation including no `audit_events`, no
object/Qdrant writes). `--apply` is refused while production OCR capability is
closed so operators do not enqueue futile jobs; refused/completed apply may
write metadata-only audit rows. See `docs/RUNBOOK.md` (OCR-004 section).

### `PATCH /workspaces/{workspace_id}/documents/{document_id}/access-mode`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: JSON `{ "access_mode": "workspace_default" | "restricted" }`.
- Success: `204` empty body.
- Errors: `400 INVALID_ACCESS_MODE`; authz `403`; `404 RESOURCE_NOT_FOUND`; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: updates `documents.access_mode`; when the target mode is `workspace_default`, deletes all `document_shares` rows for the document and removes the corresponding `explicit_viewer` OpenFGA tuples (OpenFGA first, then SQL); writes a metadata-only `audit_events` row recording the previous mode, new mode, and `shares_cleaned` count.
- Security notes: mode is updated before share cleanup so a mid-cleanup failure cannot leave the document `restricted` without its prior explicit viewers. Cleanup also runs on re-apply of `workspace_default` so a partial previous cleanup can be retried safely. Setting `restricted` does not create shares.

### `GET /workspaces/{workspace_id}/documents/{document_id}/shares`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with `{ document_id, access_mode, shares: [{ user_id, email, shared_at }] }`; shares are ordered by `email ASC`, then `user_id ASC` for deterministic ties.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND` when the document does not belong to the workspace; `500 AUTHZ_ERROR` on an authorization dependency failure; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: none.

### `PUT /workspaces/{workspace_id}/documents/{document_id}/permissions`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: declarative target state `{ "access_mode": "workspace_default" | "restricted", "authorized_user_ids": ["<keycloak_sub>", ...] }`; `authorized_user_ids` defaults to `[]`.
- Validation: `workspace_default` requires an empty user list. For `restricted`, every new or recovery grant target must exist in both SQL `workspace_members` and OpenFGA `member` before any mutation. A bad target returns `400 USER_NOT_WORKSPACE_MEMBER` with `error.details.user_id`; other invalid bodies return `400 INVALID_REQUEST`.
- Success: `200` with `{ document_id, access_mode, shares: [{ user_id, email, shared_at }] }`, using the same ordering and shape as `GET .../shares`.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND` when the document does not belong to the workspace; `500 AUTHZ_ERROR` for OpenFGA check/write failure; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Apply order: update `access_mode` first when needed; revoke `explicit_viewer` in OpenFGA before deleting its SQL share; insert each SQL share before granting its OpenFGA tuple. A failed OpenFGA grant enqueues authz-outbox recovery best-effort.
- Idempotency and recovery: duplicate target ids are collapsed. A fully converged re-PUT is a no-op. If an operation stops after a partial apply, the client can safely re-PUT the same target; missing OpenFGA grants behind existing SQL rows are treated as recovery work. A successful recovery writes a new completed event with the actual remaining counts for that request.
- Audit: each non-no-op attempt that mutates state writes one best-effort `permissions_updated` event, including partial applies. Metadata is `{ prev_mode, new_mode_requested, mode_applied, granted_requested, granted_applied, revoked_requested, revoked_applied, completed, failed_stage }`; `failed_stage` is `"mode"`, `"revoke"`, or `"grant"` for an incomplete apply and `null` when `completed` is `true`. Requested fields describe planned work for that attempt; applied fields describe confirmed work. Audit failure never masks the operation error or changes its status code. A no-op writes no audit event.

### `POST /workspaces/{workspace_id}/documents/{document_id}/shares/{user_id}`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `201` empty body.
- Errors: `400 USER_NOT_WORKSPACE_MEMBER`; authz `403`; `404 RESOURCE_NOT_FOUND`; `500 AUTHZ_ERROR` on fail-closed membership check; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: verifies SQL `workspace_members` **and** OpenFGA `member` (admin inherits member), then inserts `document_shares`, writes `explicit_viewer`, and audit. No mutation when either membership check fails.
- Security notes: this only grants viewer access; it does not change `documents.access_mode`. FGA-only membership without SQL row is also denied under current business contract.

### `DELETE /workspaces/{workspace_id}/documents/{document_id}/shares/{user_id}`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `204` empty body.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND`; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: deletes the matching `document_shares` row, removes the `explicit_viewer` OpenFGA tuple, and writes a metadata-only `audit_events` row.
- Security notes: revoking a share that does not exist is not specifically guarded against and currently succeeds as a no-op-equivalent deletion; verify this against `gmrag_api/src/auth/document_acl.rs::revoke_document_explicit_viewer` if idempotency guarantees matter for your use case.

### `GET /workspaces/{workspace_id}/documents/{document_id}/preview`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with `{ content, chunks }`, where `chunks` is ordered by `chunk_index`.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND`; `409 CONFLICT` when the document is not `COMPLETED`; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: none.
- Security notes: preview checks document-level ACL viewer permissions.

### `GET /workspaces/{workspace_id}/chunks/{chunk_id}`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with `{ id, original_text }`.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND`; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: none.
- Security notes: chunk lookup checks document-level ACL viewer permissions.

## Chat

### `POST /workspaces/{workspace_id}/chat`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`, then chat-session ownership inside the session helpers.
- Request body: JSON `{ "session_id": "uuid", "message": "..." }`.
- Success: `200` `text/event-stream`.
- Errors before SSE begins: `400 INVALID_REQUEST`, `403 FORBIDDEN`, `502 GENERATION_SERVICE_UNAVAILABLE`, or `500 INTERNAL_ERROR` JSON envelopes. After SSE starts, the protocol uses a sanitized `error` event. Upstream response bodies and credentials are never exposed.
- Side effects: ensures the chat session, persists the user message, builds RAG context, streams model tokens, resolves citations, and persists the assistant message after streaming.
- Security notes: retrieval is ACL-scoped and uses Qdrant filter-then-search to ensure only permitted chunks are processed, backed by OpenFGA viewer permission re-checks.
- Timeout/cancellation notes: DeepSeek request establishment is bounded by `DEEPSEEK_REQUEST_TIMEOUT_SECS`; stream idle gaps are bounded by `DEEPSEEK_STREAM_IDLE_TIMEOUT_SECS`; when the SSE client disconnects, the upstream stream is dropped/cancelled by connection teardown.

Current SSE events:

- default event: token text chunks,
- `error`: stream-processing failure,
- `done`: final session id.

### `GET /workspaces/{workspace_id}/chat/history`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}` plus chat-session ownership.
- Request body: none.
- Query: `session_id=<uuid>`.
- Success: `200` with ordered chat messages containing `id`, `role`, `content`, `citations`, and `created_at`.
- Errors: `403 FORBIDDEN` or `500 INTERNAL_ERROR` JSON envelopes.
- Side effects: none.
- Security notes: a missing session currently returns `200 []` instead of `404`.

### `GET /workspaces/{workspace_id}/chat/sessions`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with session summaries `{ id, title, created_at }` owned by the caller.
- Errors: authz `403` or `500 INTERNAL_ERROR`, both JSON envelopes.
- Side effects: none.
- Security notes: callers only see their own chat sessions, not other members' sessions.

### `GET /workspaces/{workspace_id}/chat/sessions/{session_id}/messages`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}` plus chat-session ownership.
- Request body: none.
- Success: `200` with ordered message rows containing structured `citations`.
- Errors: `403 FORBIDDEN` or `500 INTERNAL_ERROR` JSON envelopes.
- Side effects: none.
- Security notes: a missing session currently returns `200 []` instead of `404`.

### `DELETE /workspaces/{workspace_id}/chat/sessions/{session_id}`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}` plus chat-session ownership.
- Request body: none.
- Success: `204` empty body.
- Errors: `403 FORBIDDEN`, `404 RESOURCE_NOT_FOUND`, or `500 INTERNAL_ERROR` JSON envelopes.
- Side effects: deletes the chat session row and cascades chat messages.
- Security notes: only the owner can delete a session.

## Graph

### `GET /workspaces/{workspace_id}/graph`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with `{ nodes, links }`, where nodes expose entity metadata and links expose `source`, `target`, `relationship`, and `description`.
- Errors: authz `403` or `500 INTERNAL_ERROR`, both JSON envelopes.
- Side effects: none.

- Security notes: graph nodes and edges are filtered to only return sources the user has document-level ACL access to.

## Phase 4 visibility policy

| Boundary | Unauthorized behavior |
| --- | --- |
| Workspace membership | `403` envelope theo workspace boundary contract. |
| Restricted document, preview, chunk, citation | `404 RESOURCE_NOT_FOUND`, giống resource không tồn tại. |
| Document list, graph, retrieval, chat citations | Omit resource; không placeholder. |
| Chat session không thuộc caller | Không phân biệt session của người khác với session không tồn tại khi helper ownership trả hidden result. |
| Admin mutation | Check OpenFGA workspace relation trước resource mutation; deny không tạo SQL/FGA/outbox side effect. |
