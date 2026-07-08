# GMRAG

**GMRAG** is a high-performance, multi-tenant GraphRAG platform inspired by LightRAG and EdgeQuake. A Rust API handles document ingestion, vector search, knowledge-graph extraction, and streaming chat; a Next.js dashboard provides workspace management, document uploads, graph visualization, and RAG Q&A. PostgreSQL with `pgvector` stores embeddings and graph data; Clerk provides authentication and workspace-scoped RBAC.

| Layer | Stack |
|-------|--------|
| Backend (`gmrag_api`) | Rust, Axum, SQLx, Tokio |
| Frontend (`gmrag_ui`) | Next.js 16 (App Router), Tailwind CSS, shadcn/ui, Clerk |
| Database | PostgreSQL 16 + `pgvector` |
| Chat & graph extraction | DeepSeek `deepseek-v4-flash` API |
| Embeddings | Ollama `nomic-embed-text` (batched, multi-file) |
| Auth | Clerk JWT validation + optional webhooks |

---

## 1. Architecture and Core Features

### Multi-tenant workspace isolation

Every API request is authenticated with a Clerk JWT. Workspace membership is enforced in PostgreSQL via `workspace_members` with roles `ADMIN` and `USER`:

- **Super admin** (`users.is_super_admin = true`): create and delete workspaces.
- **ADMIN** (workspace role): upload/delete documents, invite/remove members, view chat/graph/document data within the workspace.
- **USER** (workspace role): list documents, query chat, view graph, read chat history; cannot upload, delete documents, or manage members.

All queries scope data by `workspace_id`, so tenants never cross workspace boundaries.

### Staged ingestion pipeline

PDF uploads run asynchronously with a global document concurrency limit (`GMRAG_INGESTION_DOCUMENT_CONCURRENCY`). Each document progresses through explicit stages persisted on the row:

| Stage | Description |
|-------|-------------|
| `QUEUED` | Accepted and scheduled |
| `PARSING` | PDF text extraction (with optional vision OCR fallback) |
| `EMBEDDING` | Tiktoken chunking + concurrent Ollama batch embeddings |
| `GRAPH_EXTRACTION` | Bounded concurrent DeepSeek graph node/edge extraction per chunk |
| `SAVING` | Transactional persist of chunks, vectors, and graph elements |
| `DONE` | Final stage when status becomes `COMPLETED` |

Status transitions: `PROCESSING` → `COMPLETED` or `FAILED`. Failed documents can be manually re-queued via `POST /workspaces/{workspace_id}/documents/{document_id}/retry` (admin-only, source PDF must still exist on disk).

Note: current OCR fallback is a placeholder (`mock_ocr_text`) for low-text pages.

### Idempotent database transactions

- Migrations run automatically on API boot via `sqlx::migrate!("./migrations")`.
- Unique indexes on `(workspace_id, document_id, chunk_index)` and graph entity keys support safe re-ingestion semantics.
- Document completion and failure updates run inside transactions where required.
- **Document deletion** runs in a single transaction: remove `graph_*_sources` for the document, delete graph edges/nodes that no longer have any source or edge references (orphan cleanup), then delete the document row and on-disk PDF.

### Academic citation mapping

The LLM is instructed to cite using compact index markers (`[chunk:1]`, `[chunk:2]`) tied to retrieved chunk order—not raw UUIDs in the prompt. After streaming completes, the API resolves indices to real `document_chunks.id` UUIDs via `resolve_chunk_index_citations`, stores deduplicated citation UUIDs on `chat_messages`, and serves secure chunk lookups to the UI without bloating tokens during generation.

---

## 2. Project Directory Layout

```text
GMRAG/
├── docker-compose.yml          # PostgreSQL 16 + pgvector
├── gmrag_api/
│   ├── Cargo.toml
│   ├── migrations/             # SQLx migrations (applied on boot)
│   ├── data/uploads/           # PDF storage (gitignored)
│   └── src/
│       ├── main.rs             # Axum router, health, CORS
│       ├── auth/               # JWT validation, RBAC extractors
│       ├── chat/               # RAG retrieval, DeepSeek streaming, citations
│       ├── ingestion/          # PDF parse, chunk, embed, graph extract
│       ├── routes/             # HTTP handlers (workspaces, docs, chat, …)
│       ├── state.rs            # AppState (pool, JWT, upload dir, limiter)
│       └── webhooks/           # Clerk webhook (Svix verification)
└── gmrag_ui/
    ├── package.json
    ├── public/
    └── src/
        ├── app/                # App Router pages (dashboard, sign-in, …)
        ├── components/         # UI + dashboard (chat, graph, upload, …)
        ├── context/            # Workspace and chat session providers
        └── lib/                # API client helpers (api, chat, documents, …)
```

---

## 3. Prerequisites

Install the following on your development machine:

| Tool | Version / notes |
|------|------------------|
| **Docker** | Docker Desktop or Engine + Compose v2 |
| **Rust** | Stable toolchain with **Cargo edition 2024** (`rustup default stable`) |
| **Node.js** | **v20+** (LTS recommended) |
| **npm** | Bundled with Node.js |
| **Ollama** | Local daemon for `nomic-embed-text` embeddings |
| **Clerk** | Application with JWT issuer and API keys |
| **DeepSeek** | API key for chat and graph extraction |

Optional: `RUST_LOG` or `tracing` env filter (defaults to `info,lopdf=error`).

---

## 4. Environment Variables Configuration

Copy each template to the path shown, then fill in secrets. Never commit real `.env` files.

### Backend — `gmrag_api/.env.example`

Create `gmrag_api/.env` from this template:

```env
# -----------------------------------------------------------------------------
# Database (required)
# -----------------------------------------------------------------------------
DATABASE_URL=postgres://gmrag_user:change_me@127.0.0.1:5432/gmrag
DATABASE_POOL_SIZE=32

# -----------------------------------------------------------------------------
# Clerk authentication (required for API)
# -----------------------------------------------------------------------------
# Issuer URL from Clerk Dashboard → API Keys (no trailing slash)
CLERK_ISSUER=https://your-instance.clerk.accounts.dev

# Optional: Svix signing secret for POST /api/webhooks/clerk
CLERK_WEBHOOK_SECRET=whsec_your_webhook_signing_secret

# -----------------------------------------------------------------------------
# DeepSeek — chat streaming and graph extraction (required)
# -----------------------------------------------------------------------------
DEEPSEEK_API_KEY=sk-your-deepseek-api-key
DEEPSEEK_API_URL=https://api.deepseek.com/chat/completions
DEEPSEEK_MODEL=deepseek-v4-flash
DEEPSEEK_GRAPH_MAX_TOKENS=32768

# -----------------------------------------------------------------------------
# Ollama — batch embeddings (required for ingestion and chat retrieval)
# -----------------------------------------------------------------------------
OLLAMA_EMBED_URL=http://127.0.0.1:11434/api/embed
# Legacy alias (optional): OLLAMA_EMBEDDINGS_URL=http://127.0.0.1:11434/api/embeddings
OLLAMA_EMBED_MODEL=nomic-embed-text

# -----------------------------------------------------------------------------
# Storage and HTTP
# -----------------------------------------------------------------------------
UPLOAD_DIR=./data/uploads
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000

# -----------------------------------------------------------------------------
# Ingestion — document-level concurrency
# -----------------------------------------------------------------------------
GMRAG_INGESTION_DOCUMENT_CONCURRENCY=2

# -----------------------------------------------------------------------------
# Ingestion — PDF parsing
# -----------------------------------------------------------------------------
GMRAG_PDF_PARSE_TIMEOUT_SECS=120

# -----------------------------------------------------------------------------
# Ingestion — Ollama embedding batches
# -----------------------------------------------------------------------------
GMRAG_EMBEDDING_BATCH_SIZE=32
GMRAG_EMBEDDING_CONCURRENCY=2
GMRAG_EMBEDDING_TIMEOUT_SECS=120
GMRAG_EMBEDDING_RETRIES=1
GMRAG_EMBEDDING_RETRY_BACKOFF_MS=250

# -----------------------------------------------------------------------------
# Ingestion — DeepSeek graph extraction (per chunk, bounded concurrency)
# -----------------------------------------------------------------------------
GMRAG_GRAPH_EXTRACTION_ENABLED=true
GMRAG_GRAPH_EXTRACTION_CONCURRENCY=12
GMRAG_GRAPH_EXTRACTION_TIMEOUT_SECS=20
GMRAG_GRAPH_EXTRACTION_RETRIES=0
GMRAG_GRAPH_EXTRACTION_STAGE_TIMEOUT_SECS=30
GMRAG_GRAPH_EXTRACTION_RETRY_BACKOFF_MS=250

# -----------------------------------------------------------------------------
# Logging (optional)
# -----------------------------------------------------------------------------
RUST_LOG=info,lopdf=error
```

### Frontend — `gmrag_ui/.env.local.example`

Create `gmrag_ui/.env.local` from this template:

```env
# -----------------------------------------------------------------------------
# Clerk (required) — https://dashboard.clerk.com
# -----------------------------------------------------------------------------
NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY=pk_test_your_publishable_key
CLERK_SECRET_KEY=sk_test_your_secret_key

# Optional Clerk route overrides (defaults work for App Router)
# NEXT_PUBLIC_CLERK_SIGN_IN_URL=/sign-in
# NEXT_PUBLIC_CLERK_SIGN_UP_URL=/sign-up

# -----------------------------------------------------------------------------
# GMRAG API (required)
# -----------------------------------------------------------------------------
NEXT_PUBLIC_API_URL=http://127.0.0.1:8080
```

**Clerk setup:** Use the same Clerk application as the backend. Set `CLERK_ISSUER` to your Clerk Frontend API issuer (e.g. `https://<slug>.clerk.accounts.dev`). The UI sends `Authorization: Bearer <session_token>` to the Rust API on protected routes.

---

## 5. Step-by-Step Local Setup Guide

Run these commands from the repository root unless noted otherwise.

### 5.1 Clone and enter the project

```bash
git clone <your-repo-url> GMRAG
cd GMRAG
```

### 5.2 Start infrastructure (PostgreSQL + pgvector)

```bash
docker compose up -d
```

Default connection (matches `docker-compose.yml`):

- Host: `127.0.0.1:5432`
- User: `gmrag_user`
- Password: `change_me`
- Database: `gmrag`

Data persists under `.docker_data/postgres/` (gitignored).

### 5.3 Install and run Ollama embeddings

Install [Ollama](https://ollama.com), start the daemon, then pull the embedding model:

```bash
ollama pull nomic-embed-text
```

Verify the embed endpoint (default):

```bash
curl http://127.0.0.1:11434/api/embed -d "{\"model\":\"nomic-embed-text\",\"input\":[\"test\"]}"
```

### 5.4 Configure and run the backend

```bash
cd gmrag_api
cp .env.example .env
# Edit .env: DATABASE_URL, CLERK_ISSUER, DEEPSEEK_API_KEY, etc.

cargo run
```

On startup the API will:

1. Connect to PostgreSQL using `DATABASE_URL`
2. Run all migrations in `gmrag_api/migrations/` via `sqlx::migrate!`
3. Create `UPLOAD_DIR` if missing
4. Listen on **`http://127.0.0.1:8080`**

Health check:

```bash
curl http://127.0.0.1:8080/health
```

### 5.5 Configure and run the frontend

In a second terminal:

```bash
cd gmrag_ui
cp .env.local.example .env.local
# Edit .env.local: Clerk keys and NEXT_PUBLIC_API_URL

npm install
npm run dev
```

Open **`http://localhost:3000`**, sign in with Clerk, sync the user (`POST /users/sync` is triggered from the app), create a workspace, upload PDFs (admin), and use chat/graph views.

Workspace creation/deletion are gated by `users.is_super_admin`. For local bootstrapping, promote your user after first sign-in/sync:

```bash
docker compose exec postgres psql -U gmrag_user -d gmrag -c "UPDATE users SET is_super_admin = TRUE WHERE email = 'you@example.com';"
```

Adjust user/database names if you changed `POSTGRES_USER` / `POSTGRES_DB`.

### 5.6 First-time verification checklist

| Step | Expected result |
|------|-----------------|
| `docker compose ps` | `postgres` is `Up` on port 5432 |
| `curl :8080/health` | `{"status":"ok","db":"connected"}` |
| Ollama model | `nomic-embed-text` listed in `ollama list` |
| Upload PDF (admin) | Document status moves through stages → `COMPLETED` |
| Chat message | SSE stream; citations resolve to chunk UUIDs in history |

---

## 6. API Contract Summary

Base URL: `http://127.0.0.1:8080` (configurable in production).

**Authentication:** `Authorization: Bearer <clerk_session_jwt>` on all routes except `/health` and `/api/webhooks/clerk`.

### Core routes

| Method | Path | Access | Description |
|--------|------|--------|-------------|
| `GET` | `/health` | Public | Liveness and DB connectivity |
| `POST` | `/api/webhooks/clerk` | Svix secret | Clerk user lifecycle webhooks |
| `GET` | `/users/me` | Auth | Current user profile |
| `POST` | `/users/sync` | Auth | Upsert/reconcile user from request email |
| `GET` | `/workspaces` | Auth | List workspaces for current user |
| `POST` | `/workspaces` | Super admin | Create workspace (creator becomes `ADMIN`) |
| `DELETE` | `/workspaces/{workspace_id}` | Super admin | Delete workspace |
| `GET` | `/workspaces/{workspace_id}/members` | Member | List members |
| `POST` | `/workspaces/{workspace_id}/members` | Admin | Invite/add member (`role`: `admin`/`owner` → `ADMIN`, `user`/`member` → `USER`) |
| `DELETE` | `/workspaces/{workspace_id}/members/{member_id}` | Admin | Remove member |
| `GET` | `/workspaces/{workspace_id}/documents` | Member | List documents and processing status |
| `POST` | `/workspaces/{workspace_id}/documents/upload` | Admin | Multipart PDF upload (`file` field, 50 MiB total request limit) |
| `DELETE` | `/workspaces/{workspace_id}/documents/{document_id}` | Admin | Delete document + orphan graph cleanup |
| `POST` | `/workspaces/{workspace_id}/documents/{document_id}/retry` | Admin | Retry ingestion for `FAILED` document |
| `GET` | `/workspaces/{workspace_id}/documents/{document_id}/preview` | Member | Aggregated chunk text preview |
| `GET` | `/workspaces/{workspace_id}/chunks/{chunk_id}` | Member | Single chunk text (citation target) |
| `POST` | `/workspaces/{workspace_id}/chat` | Member | **SSE** streaming RAG chat |
| `GET` | `/workspaces/{workspace_id}/chat/history` | Member | Messages for `session_id` query parameter |
| `GET` | `/workspaces/{workspace_id}/chat/sessions` | Member | List chat sessions |
| `GET` | `/workspaces/{workspace_id}/chat/sessions/{session_id}/messages` | Member | Messages with citation UUIDs |
| `DELETE` | `/workspaces/{workspace_id}/chat/sessions/{session_id}` | Member | Delete session |
| `GET` | `/workspaces/{workspace_id}/graph` | Member | Knowledge graph nodes/edges for visualization |

### Chat streaming (`POST /workspaces/{workspace_id}/chat`)

- **Content-Type:** `application/json` body with required `session_id` and `message`.
- **Response:** `text/event-stream` (SSE) with token deltas; final assistant message persisted with resolved citation UUIDs.
- **RAG flow:** embed query (Ollama) → top-5 chunk vector search → graph context → DeepSeek stream with index-based citations.

---

## 7. UI/UX Enforcement Rules (For Developers)

These rules apply to all contributions in `gmrag_ui/`:

1. **No emojis** in UI copy, component labels, toasts, placeholders, or styling. Use clear text and consistent terminology only.
2. **Icons:** Use **`lucide-react`** for every visual affordance (actions, status, navigation, empty states). Do not substitute emoji or ad-hoc Unicode symbols.
3. **Auth:** Protected routes use `@clerk/nextjs` middleware; API calls attach the Clerk session token via shared helpers in `src/lib/api.ts`.
4. **API base URL:** Always read `NEXT_PUBLIC_API_URL` (default `http://localhost:8080`); never hardcode production hosts in components.
5. **Accessibility:** Prefer semantic HTML and Radix/shadcn primitives already in the design system; pair icons with visible text or `aria-label` where needed.

---

## Production Notes

- Put the API behind TLS and restrict `CORS_ALLOWED_ORIGINS` to your deployed UI origin.
- Rotate `DEEPSEEK_API_KEY`, `CLERK_WEBHOOK_SECRET`, and database credentials via your secrets manager.
- Scale embedding throughput by tuning `GMRAG_EMBEDDING_*` and running Ollama on GPU-backed hardware.
- Tune `GMRAG_GRAPH_EXTRACTION_CONCURRENCY` against DeepSeek rate limits.
- Back up PostgreSQL volume and `UPLOAD_DIR` (or object storage if you migrate files off disk).

---

## License

See repository license file if present. Otherwise, all rights reserved by the project maintainers.
