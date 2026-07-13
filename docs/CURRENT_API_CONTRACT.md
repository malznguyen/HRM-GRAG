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

- Upload stores original PDFs in MinIO/S3 through the storage module.
- Retry reads from MinIO/S3 and does not depend on local-file existence.
- Delete performs SQL cleanup first, then best-effort object deletion.
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
| `POST` | `/workspaces/{workspace_id}/documents/{document_id}/shares/{user_id}` | `admin` on `workspace:{workspace_id}` |
| `DELETE` | `/workspaces/{workspace_id}/documents/{document_id}/shares/{user_id}` | `admin` on `workspace:{workspace_id}` |
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

## Tenant And Workspace Lifecycle

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

### Tenant delete and Qdrant orphans

There is no `DELETE /tenants/{tenant_id}` route. If a tenant is removed via SQL
cascade (or any path that does not go through `DELETE /workspaces/{id}` per
workspace), Qdrant vectors are not cleaned automatically.

Remediation (library / ops, not HTTP):

- `RetrievalClient::delete_points_by_tenant(pool, tenant_id)` while workspaces still exist
- `RetrievalClient::delete_points_by_workspaces(&[...])` with ids captured before cascade
- Operator: `cleanup-qdrant-orphans --tenant-id ...` or outbox replay via `process-qdrant-outbox`

Payload limitation: points do not carry `tenant_id` (workspace list only). See
`docs/CURRENT_ARCHITECTURE.md` and `docs/RUNBOOK.md` §7.

### `DELETE /workspaces/{workspace_id}`

- Auth: bearer JWT.
- Authorization: `can_assign_role` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `204` empty body.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND`; `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: deletes the workspace row and cascades workspace data in the same PostgreSQL transaction as inserting `qdrant_outbox` (`delete_by_workspace`) and `storage_outbox` (`delete_prefix` with canonical `tenants/{tenant_id}/workspaces/{workspace_id}/` prefix, SQL-trusted tenant/workspace IDs, and configured storage bucket — not client-supplied). Then best-effort deletes the `tenant -> workspace` tuple from OpenFGA and best-effort deletes Qdrant points filtered by `workspace_id` (`wait=true` + short request timeout `QDRANT_DELETE_REQUEST_TIMEOUT_SECS`). Qdrant failure/timeout does not fail HTTP once SQL has committed; worker recovery uses longer `QDRANT_DELETE_WORKER_TIMEOUT_SECS`. **No** S3 prefix delete runs on the HTTP request path; prefix recovery is via `process-storage-outbox` (unattended scheduling: OPS-003).
- Security notes: Qdrant cleanup runs after SQL commit; failures are logged and reflected in audit metadata (`qdrant_workspace_delete_succeeded`) and do not fail the HTTP delete once SQL has committed. Storage prefix cleanup is durable via `storage_outbox` after SQL commit (LIFE-004); missing workspace leaves no outbox row. Tenant-wide cascade strategy remains LIFE-005.

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
- Request body: none.
- Success: `200` with document rows containing `id`, `filename`, `status`, `processing_stage`, optional `failure_code`/`failure_message`, and `created_at`.
- Errors: authz `403` or `500 INTERNAL_ERROR`, both JSON envelopes.
- Side effects: none.
- Security notes: visibility is ACL-scoped; users only see documents they have explicit or bypass viewer access to.

### `POST /workspaces/{workspace_id}/documents/upload`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: `multipart/form-data` with one or more `file` parts, and an optional text field `access_mode` (`workspace_default` | `restricted`). When omitted, `access_mode` defaults to `workspace_default`. The same `access_mode` applies to every accepted file in the request.
- Success: `202` with `{ documents: [{ document_id, filename }] }` for the accepted files.
- Errors: `400 INVALID_REQUEST` when no acceptable PDF is accepted; `400 INVALID_ACCESS_MODE`; authz `403`; `404 RESOURCE_NOT_FOUND`; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: uploads original bytes to MinIO/S3, then atomically inserts `documents` metadata (including `access_mode`) and one durable `ingestion_jobs` row. Processing is performed by the separate ingestion worker after commit.
- Security notes: filename and client MIME type are stored as metadata only. Acceptance requires a `%PDF-` signature and successful structural validation (`lopdf`) of the submitted bytes before object-key generation or S3/MinIO upload; a filename suffix alone is never enough. Responses do not expose raw object keys. Uploading as `restricted` does not auto-create `document_shares` or `explicit_viewer` tuples for the uploader; visibility follows the same restricted-document rules as after `PATCH .../access-mode`.

### `DELETE /workspaces/{workspace_id}/documents/{document_id}`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `204` empty body.
- Errors: authz `403`; `404 RESOURCE_NOT_FOUND`; or `500 INTERNAL_ERROR`, all JSON envelopes.
- Side effects: removes graph provenance, the document row, and inserts `qdrant_outbox` (`delete_by_document`) plus `storage_outbox` (`delete_object` with SQL-captured `object_key`/`bucket`) in one SQL transaction, then best-effort deletes the storage object and Qdrant points for that `document_id` (filtered with `workspace_id` + `document_id`). HTTP remains `204` on storage/Qdrant failure after commit.
- Security notes: storage and Qdrant deletes happen after SQL commit; Qdrant uses `wait=true` + short request timeout (`QDRANT_DELETE_REQUEST_TIMEOUT_SECS`). Qdrant recovery row is committed with the SQL delete (LIFE-001); storage recovery row is also committed with the SQL delete (LIFE-003). Worker retries: `process-qdrant-outbox` / `process-storage-outbox`. Cleanup failures are logged and reflected in audit metadata (`storage_delete_succeeded`, `qdrant_delete_succeeded`) for later remediation. They do not fail the HTTP delete once SQL has committed. Object keys are not exposed to the client.

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
| `EMBEDDING_PROVIDER_UNAVAILABLE` | Embeddings failed | Yes (until max attempts) |
| `GRAPH_EXTRACTION_FAILED` | Graph extraction failed | Yes (until max attempts) |
| `QDRANT_INDEX_FAILED` | Vector index failed | Yes (until max attempts) |
| `DATABASE_SAVE_FAILED` | SQL persistence failed | Yes (until max attempts) |
| `INTERNAL_INGESTION_ERROR` | Other internal ingestion error | Yes (until max attempts) |
| `INGESTION_MAX_ATTEMPTS_EXCEEDED` | Retryable failure exhausted `max_attempts` | No (terminal after retries) |

`NEEDS_OCR` is non-retryable for the worker while no OCR provider is configured,
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
