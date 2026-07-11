# Current API Contract

This document reflects the routes registered by `gmrag_api/src/lib.rs` after Phase 2
and Phase 3A hardening.
Historical audit snapshots are archived under `docs/archive/v1/`.

Phase 3A hardening is internal operational work (workers, cleanup, audit
hardening) and does not introduce new public HTTP endpoints. Operator-only
binaries (including `backfill-graph-node-embeddings`) are documented in
`docs/RUNBOOK.md`, not as HTTP routes.

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

### Error Response Format

The API is partially standardized today.

- Authz denials use the JSON envelope shown above.
- `POST /workspaces/{workspace_id}/documents/{document_id}/retry` also returns a JSON envelope for `410 DOCUMENT_OBJECT_MISSING`.
- `POST /workspaces/{workspace_id}/members` returns a JSON envelope for `404 USER_NOT_FOUND_IN_IDENTITY` when the target email is not a verified Keycloak user (see Workspace Members).
- `DELETE /workspaces/{workspace_id}/members/{member_id}` returns JSON envelopes for `500 AUTHZ_REVOKE_FAILED` and `500 MEMBER_REMOVE_FAILED` (fail-closed revoke; see Workspace Members).
- Many validation, not-found, and internal errors still return plain text or an empty body.

### Current Storage Contract

- Upload stores original PDFs in MinIO/S3 through the storage module.
- Retry reads from MinIO/S3 and does not depend on local-file existence.
- Delete performs SQL cleanup first, then best-effort object deletion.
- Public API responses do not expose raw storage object keys.

## Endpoint Inventory

| Method | Path | Authorization |
| --- | --- | --- |
| `GET` | `/health` | public |
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
- Errors: `500` empty body if `SELECT 1` fails.
- Side effects: none.
- Security notes: safe public liveness check only.

## User Endpoints

### `GET /users/me`

- Auth: bearer JWT.
- Authorization: any authenticated user.
- Request body: none.
- Success: `200` with the current SQL user row plus a boolean platform-admin flag derived from OpenFGA.
- Errors: `404` plain text `User not found`; `500` empty body on DB failure.
- Side effects: none (invite reconciliation was removed from this path).
- Security notes: platform-admin status is derived from OpenFGA, not a SQL column.

### `POST /users/sync`

- Auth: bearer JWT.
- Authorization: any authenticated user.
- Request body: none.
- Success: `200` empty body.
- Errors: `400` JSON `IDENTITY_EMAIL_REQUIRED` or `IDENTITY_EMAIL_UNVERIFIED`; `409` JSON `IDENTITY_EMAIL_CONFLICT`; `500` empty body on upsert failure.
- Side effects: upserts the SQL `users` row for verified JWT `sub` + verified `email` claim. It never accepts an email from the request body and never reconciles identities.

## Tenant And Workspace Lifecycle

### `POST /tenants`

- Auth: bearer JWT.
- Authorization: `admin` on `platform:system`.
- Request body: JSON `{ "name": "Tenant Name" }`.
- Success: `201` with `{ id, name, created_at }`.
- Errors: `400` plain text `Tenant name is required`; `403` JSON authz envelope; `500` empty body on SQL or OpenFGA write failure.
- Side effects: inserts the tenant row and writes the `platform -> tenant` tuple in OpenFGA.
- Security notes: creating a tenant does not grant the caller implicit business-data access inside its workspaces.

### `POST /tenants/{tenant_id}/owners`

- Auth: bearer JWT.
- Authorization: `admin` on `platform:system`.
- Request body: JSON `{ "email": "owner@example.com" }`.
- Success: `204` empty body.
- Errors: `400` plain text for invalid email or unverified/missing Keycloak user; `403` JSON authz envelope; `500` empty body on lookup, SQL, or OpenFGA failure.
- Side effects: ensures a SQL `users` row exists, inserts `tenant_members`, and writes the `user owner tenant` tuple.
- Security notes: only verified users from Keycloak are accepted as tenant owners.

### `POST /tenants/{tenant_id}/workspaces`

- Auth: bearer JWT.
- Authorization: `owner` on `tenant:{tenant_id}`.
- Request body: JSON `{ "name": "Workspace Name" }`.
- Success: `201` with `{ id, name, created_at }`.
- Errors: `400` plain text `Workspace name is required`; `403` JSON authz envelope; `500` empty body on SQL or OpenFGA failure.
- Side effects: inserts the workspace row, inserts a SQL `workspace_members` admin row for the creator, writes the `tenant -> workspace` tuple, and writes the workspace-admin tuple for the creator.
- Security notes: current workspace creation is tenant-owner only.

### `GET /workspaces`

- Auth: bearer JWT.
- Authorization: any authenticated user; each SQL candidate is then checked for OpenFGA `member` on `workspace:{id}` (admin and tenant owner inherit `member` via the model).
- Request body: none.
- Success: `200` with an array of `{ id, name, created_at }` that pass **both** SQL candidate membership (workspace_members or tenant OWNER) **and** OpenFGA `member`. Deterministic order by `created_at DESC`; duplicates removed.
- Errors: `500` empty body on SQL failure; `500` JSON `AUTHZ_ERROR` if OpenFGA check fails (fail-closed — never returns the SQL-only candidate list).
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
- Errors: `403` JSON authz envelope; `404` empty body if the workspace row does not exist; `500` empty body on SQL failure.
- Side effects: deletes the workspace row, cascades workspace data, best-effort deletes the `tenant -> workspace` tuple from OpenFGA, and best-effort deletes Qdrant points filtered by `workspace_id` (`wait=true` + short request timeout `QDRANT_DELETE_REQUEST_TIMEOUT_SECS`). On Qdrant failure/timeout, enqueues `qdrant_outbox` (`delete_by_workspace`) for operator recovery (worker uses longer `QDRANT_DELETE_WORKER_TIMEOUT_SECS`).
- Security notes: Qdrant cleanup runs after SQL commit; failures are logged and reflected in audit metadata (`qdrant_workspace_delete_succeeded`) and do not fail the HTTP delete once SQL has committed. Full object-storage prefix cleanup is not implemented yet.

## Workspace Members

### `GET /workspaces/{workspace_id}/members`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with an array of member rows containing user id, email, role, and join time.
- Errors: `403` JSON authz envelope; `500` empty body on SQL failure.
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
  | `500` | empty body | Keycloak lookup, SQL, or unexpected failure |
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
- Errors: `400` JSON `INVALID_MEMBER_ROLE`; `403` JSON authz envelope; `404` empty body if member missing; `409` JSON `LAST_WORKSPACE_ADMIN` when demoting the last valid management path (see below); `500` on SQL/OpenFGA failure.
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
  - `404` empty body if the member row is not found
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
- Errors: `403` JSON authz envelope; `500` empty body on SQL failure.
- Side effects: none.
- Security notes: visibility is ACL-scoped; users only see documents they have explicit or bypass viewer access to.

### `POST /workspaces/{workspace_id}/documents/upload`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: `multipart/form-data` with one or more `file` parts, and an optional text field `access_mode` (`workspace_default` | `restricted`). When omitted, `access_mode` defaults to `workspace_default`. The same `access_mode` applies to every accepted file in the request.
- Success: `202` with `{ documents: [{ document_id, filename }] }` for the accepted files.
- Errors: `400` empty body if no acceptable PDF was accepted; `400` JSON `{"error":{"code":"INVALID_ACCESS_MODE","message":"access_mode must be workspace_default or restricted"}}` if `access_mode` is present but not one of the two allowed values; `403` JSON authz envelope; `404` empty body if the workspace cannot be resolved to a tenant; `500` empty body on tenant lookup failure.
- Side effects: uploads original bytes to MinIO/S3, then atomically inserts `documents` metadata (including `access_mode`) and one durable `ingestion_jobs` row. Processing is performed by the separate ingestion worker after commit.
- Security notes: filename and client MIME type are stored as metadata only. Acceptance requires a `%PDF-` signature and successful structural validation (`lopdf`) of the submitted bytes before object-key generation or S3/MinIO upload; a filename suffix alone is never enough. Responses do not expose raw object keys. Uploading as `restricted` does not auto-create `document_shares` or `explicit_viewer` tuples for the uploader; visibility follows the same restricted-document rules as after `PATCH .../access-mode`.

### `DELETE /workspaces/{workspace_id}/documents/{document_id}`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `204` empty body.
- Errors: `403` JSON authz envelope; `404` empty body if the document row does not exist in the workspace; `500` empty body on SQL failure.
- Side effects: removes graph provenance and the document row in a SQL transaction, then best-effort deletes the storage object and Qdrant points for that `document_id` (filtered with `workspace_id` + `document_id`).
- Security notes: storage and Qdrant deletes happen after SQL commit; Qdrant uses `wait=true` + short request timeout (`QDRANT_DELETE_REQUEST_TIMEOUT_SECS`) then enqueues `qdrant_outbox` on failure (worker retries with `QDRANT_DELETE_WORKER_TIMEOUT_SECS`). Cleanup failures are logged and reflected in audit metadata (`storage_delete_succeeded`, `qdrant_delete_succeeded`) for later remediation. They do not fail the HTTP delete once SQL has committed.

### `POST /workspaces/{workspace_id}/documents/{document_id}/retry`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `202` empty body.
- Errors: `403` JSON authz envelope; `404` empty body if the document is missing; `409` JSON `DOCUMENT_NOT_RETRYABLE` when it is not a failed document with no active job; `410` JSON `{ "error": { "code": "DOCUMENT_OBJECT_MISSING", "message": "Original document object is missing" } }`; `500` empty body on SQL or storage lookup failure.
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

### `PATCH /workspaces/{workspace_id}/documents/{document_id}/access-mode`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: JSON `{ "access_mode": "workspace_default" | "restricted" }`.
- Success: `204` empty body.
- Errors: `400` JSON `{"error":{"code":"INVALID_ACCESS_MODE","message":"access_mode must be workspace_default or restricted"}}`; `403` JSON authz envelope; `404` empty body if the document does not exist in the workspace; `500` empty body on SQL or OpenFGA failure.
- Side effects: updates `documents.access_mode`; when the target mode is `workspace_default`, deletes all `document_shares` rows for the document and removes the corresponding `explicit_viewer` OpenFGA tuples (OpenFGA first, then SQL); writes a metadata-only `audit_events` row recording the previous mode, new mode, and `shares_cleaned` count.
- Security notes: mode is updated before share cleanup so a mid-cleanup failure cannot leave the document `restricted` without its prior explicit viewers. Cleanup also runs on re-apply of `workspace_default` so a partial previous cleanup can be retried safely. Setting `restricted` does not create shares.

### `POST /workspaces/{workspace_id}/documents/{document_id}/shares/{user_id}`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `201` empty body.
- Errors: `400` JSON `USER_NOT_WORKSPACE_MEMBER` when target fails **SQL membership or OpenFGA `member`** (including stale SQL after FGA revoke); `403` JSON authz envelope; `404` empty body if the document does not exist in the workspace; `500` JSON `AUTHZ_ERROR` if OpenFGA check fails (fail-closed, no share/tuple write); `500` empty body on other SQL/OpenFGA grant failures.
- Side effects: verifies SQL `workspace_members` **and** OpenFGA `member` (admin inherits member), then inserts `document_shares`, writes `explicit_viewer`, and audit. No mutation when either membership check fails.
- Security notes: this only grants viewer access; it does not change `documents.access_mode`. FGA-only membership without SQL row is also denied under current business contract.

### `DELETE /workspaces/{workspace_id}/documents/{document_id}/shares/{user_id}`

- Auth: bearer JWT.
- Authorization: `admin` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `204` empty body.
- Errors: `403` JSON authz envelope; `404` empty body if the document does not exist in the workspace; `500` empty body on SQL or OpenFGA failure.
- Side effects: deletes the matching `document_shares` row, removes the `explicit_viewer` OpenFGA tuple, and writes a metadata-only `audit_events` row.
- Security notes: revoking a share that does not exist is not specifically guarded against and currently succeeds as a no-op-equivalent deletion; verify this against `gmrag_api/src/auth/document_acl.rs::revoke_document_explicit_viewer` if idempotency guarantees matter for your use case.

### `GET /workspaces/{workspace_id}/documents/{document_id}/preview`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with `{ content, chunks }`, where `chunks` is ordered by `chunk_index`.
- Errors: `403` JSON authz envelope; `404` empty body if the document is missing; `409` empty body if the document is not `COMPLETED`; `500` empty body on SQL failure.
- Side effects: none.
- Security notes: preview checks document-level ACL viewer permissions.

### `GET /workspaces/{workspace_id}/chunks/{chunk_id}`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with `{ id, original_text }`.
- Errors: `403` JSON authz envelope; `404` empty body if the chunk is missing; `500` empty body on SQL failure.
- Side effects: none.
- Security notes: chunk lookup checks document-level ACL viewer permissions.

## Chat

### `POST /workspaces/{workspace_id}/chat`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`, then chat-session ownership inside the session helpers.
- Request body: JSON `{ "session_id": "uuid", "message": "..." }`.
- Success: `200` `text/event-stream`.
- Errors: `400` plain text `Message is required`; `403` JSON authz envelope or plain text `Chat session not accessible`; `502` plain text for embedding or generation failures; `500` plain text or empty body on DB failure.
- Side effects: ensures the chat session, persists the user message, builds RAG context, streams model tokens, resolves citations, and persists the assistant message after streaming.
- Security notes: retrieval is ACL-scoped and uses Qdrant filter-then-search to ensure only permitted chunks are processed, backed by OpenFGA viewer permission re-checks.

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
- Errors: `403` JSON authz envelope or plain text `Chat session not accessible`; `500` empty body on SQL failure.
- Side effects: none.
- Security notes: a missing session currently returns `200 []` instead of `404`.

### `GET /workspaces/{workspace_id}/chat/sessions`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with session summaries `{ id, title, created_at }` owned by the caller.
- Errors: `403` JSON authz envelope; `500` empty body on SQL failure.
- Side effects: none.
- Security notes: callers only see their own chat sessions, not other members' sessions.

### `GET /workspaces/{workspace_id}/chat/sessions/{session_id}/messages`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}` plus chat-session ownership.
- Request body: none.
- Success: `200` with ordered message rows containing structured `citations`.
- Errors: `403` JSON authz envelope or plain text `Chat session not accessible`; `500` empty body on SQL failure.
- Side effects: none.
- Security notes: a missing session currently returns `200 []` instead of `404`.

### `DELETE /workspaces/{workspace_id}/chat/sessions/{session_id}`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}` plus chat-session ownership.
- Request body: none.
- Success: `204` empty body.
- Errors: `403` JSON authz envelope or plain text `Chat session not accessible`; `404` empty body if the session does not exist; `500` empty body on SQL failure.
- Side effects: deletes the chat session row and cascades chat messages.
- Security notes: only the owner can delete a session.

## Graph

### `GET /workspaces/{workspace_id}/graph`

- Auth: bearer JWT.
- Authorization: `member` on `workspace:{workspace_id}`.
- Request body: none.
- Success: `200` with `{ nodes, links }`, where nodes expose entity metadata and links expose `source`, `target`, `relationship`, and `description`.
- Errors: `403` JSON authz envelope; `500` empty body on SQL failure.
- Side effects: none.
- Security notes: graph nodes and edges are filtered to only return sources the user has document-level ACL access to.
