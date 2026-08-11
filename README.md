# GMRAG

GMRAG is a multi-tenant Vietnamese retrieval-augmented generation platform. The
repository contains a Rust/Axum API, a Next.js/React frontend, PostgreSQL with
pgvector for application and graph data, OpenFGA for authorization, Qdrant for
live vector retrieval, MinIO/S3-compatible object storage, Ollama for the
`AITeamVN/Vietnamese_Embedding` model, and DeepSeek configuration for chat and
graph extraction. The OpenFGA model defines platform, tenant, workspace, and
document relations; the local Ollama model is imported from `model-q8_0.gguf`.

Sources: `gmrag_api/Cargo.toml:1-35`, `gmrag_ui/package.json:1-32`,
`docker-compose.yml:3-10,15-55,57-118,133-182`,
`gmrag_api/openfga/model.fga:4-30`, `docker/ollama/Modelfile:1-2`.

## Repository layout

- `gmrag_api/` — Rust API, migrations, OpenFGA integration, ingestion, chat,
  retrieval, storage, routes, and operator binaries.
- `gmrag_ui/` — Next.js frontend and its React components.
- `docker/` — local container build files, Keycloak configuration, and the
  Ollama `Modelfile`.
- `scripts/` — PowerShell and shell bring-up, smoke, reset, seed, and identity
  harness scripts.
- `docs/` — the three current contract/schema/architecture documents listed
  below.
- `gmrag_api/migrations/` — SQL migrations applied by the API.

Sources: `gmrag_api/src/lib.rs:1-22`, `gmrag_api/src/bin/backfill-document-workspace-tuples.rs:1-5`,
`gmrag_ui/package.json:1-10`, `docker-compose.yml:1-258`,
`gmrag_api/migrations/20260708000000_initial_schema.sql:1-5`.

There is no separate top-level `migrations/` directory in the current tree;
migrations are under `gmrag_api/migrations/`.

## Local bring-up

The repository-provided bring-up helper is `scripts/p0-bringup-migrate.ps1`.
Without `-Execute` it is preview-only: it checks migration state and prints the
planned Compose handoff without changing the Qdrant collection or database.
`-Execute` removes the configured Qdrant collection and runs `sqlx migrate run`,
then prints the manual handoff. Review that reset boundary before using it.

Source: `scripts/p0-bringup-migrate.ps1:1-13,64-71,83-107`.

The handoff starts the local infrastructure and checks the ingestion worker:

```powershell
docker compose -p hrm-rag up -d --build
docker compose -p hrm-rag ps
docker compose -p hrm-rag logs --tail 100 ingestion-worker
```

The default Compose file runs Ollama on CPU and therefore works on machines
without a GPU. On a host with the NVIDIA Container Toolkit and a usable NVIDIA
GPU, add the GPU override explicitly:

```powershell
# CPU-only (default)
docker compose -p hrm-rag up -d

# NVIDIA GPU
docker compose -p hrm-rag -f docker-compose.yml -f docker-compose.gpu.yml up -d
```

The override keeps the GPU reservation in version control without making it a
requirement for the default local environment. See
`docs/OLLAMA_MODEL_SETUP.md` for model import and verification.

Compose starts the application Postgres, OpenFGA Postgres/migration/server,
MinIO and bucket initialization, Qdrant, Ollama, Keycloak, the durable
`ingestion-worker`, the authz outbox worker, the Qdrant outbox worker, and the
dry-run storage orphan scanner. The API and frontend remain host-run in this
topology.

Sources: `scripts/p0-bringup-migrate.ps1:40-46`,
`docker-compose.yml:1-258`.

The Ollama `Modelfile` requires a local `docker/ollama/model-q8_0.gguf` input.
Import and verification commands are documented in
`docs/OLLAMA_MODEL_SETUP.md`.

Source: `docker/ollama/Modelfile:1-2`; the Compose mount and Ollama service are
defined at `docker-compose.yml:97-118`.

Run the API on the host using the values printed by the bring-up helper:

```powershell
Set-Location .
$env:QDRANT_URL='http://127.0.0.1:16333'
$env:QDRANT_VECTOR_SIZE='1024'
$env:OLLAMA_EMBED_URL='http://127.0.0.1:11435/api/embed'
$env:OLLAMA_EMBED_MODEL='AITeamVN/Vietnamese_Embedding'
cargo run --manifest-path .\gmrag_api\Cargo.toml --locked
```

The HRM API binds to `127.0.0.1:18083`; OpenFGA is published at `18081`, Qdrant at
`16333`, Ollama at `11435`, MinIO at `19000`, and Keycloak at `18080`. The frontend package provides `npm run
dev` and its API client defaults to that API base URL. Configure the remaining
API and OIDC values from `gmrag_api/.env.example` and the frontend’s existing
environment contract before testing authenticated flows.

Sources: `scripts/p0-bringup-migrate.ps1:47-52`,
`gmrag_api/src/main.rs:37-56,83-93`, `gmrag_ui/package.json:5-10`,
`gmrag_ui/src/lib/config/env.ts:1-24`, `gmrag_api/.env.example:5-16,22-24,37-49,58-65,111-127`.

Start the frontend from `gmrag_ui/` with `npm run dev`. After signing in,
provide a workspace id and bearer token to the local ingestion smoke:

```powershell
Set-Location .\gmrag_ui
npm run dev

$env:GMRAG_SMOKE_WORKSPACE_ID='<workspace-uuid>'
$env:GMRAG_SMOKE_BEARER_TOKEN='<bearer-token>'
Set-Location ..
.\scripts\run-local-ingestion-smoke.ps1
```

The smoke uploads generated PDF, DOCX, TXT, and MD fixtures, waits for
`COMPLETED/DONE`, checks 1024-dimensional Qdrant vectors, previews each
document, checks retrieval content, and prints `Local ingestion smoke: PASS`.

Sources: `scripts/p0-bringup-migrate.ps1:54-60`,
`scripts/run-local-ingestion-smoke.ps1:15-29,222-261`.

## Fast verification (no Docker required)

These checks need no running stack and complete in seconds. Run them before
pushing — CI (`.github/workflows/ci.yml`) runs exactly this set.

```powershell
Set-Location .\gmrag_api
cargo fmt --check
cargo clippy --all-targets
cargo test --lib          # ~2s; 5 infra-dependent tests are #[ignore]d

Set-Location ..\gmrag_ui
npm run lint
npm run typecheck
```

The full integration suite requires a provisioned isolated environment — run it
with `.\scripts\run-isolated-integration-tests.ps1` after `docker compose up -d`.
The five ignored unit tests belong to that tier and run there via
`cargo test --lib -- --ignored`.

Sources: `rust-toolchain.toml`, `.github/workflows/ci.yml`,
`scripts/run-isolated-integration-tests.ps1`.

## Mandatory pre-deploy ordering

Run the `backfill-document-workspace-tuples` binary before enabling strict
document ACL checks. The binary backfills missing `workspace` relations for
existing documents and reports inserted versus existing relations. Restricted
document checks use the document’s `bypass_viewer` relation, and the OpenFGA
model derives that relation through the document’s workspace and tenant owner.

```powershell
Set-Location .\gmrag_api
cargo run --bin backfill-document-workspace-tuples
```

Sources: `gmrag_api/src/bin/backfill-document-workspace-tuples.rs:18-55`,
`gmrag_api/src/auth/document_acl.rs:254-320`,
`gmrag_api/openfga/model.fga:25-30`.

## Current documentation

- [API contract](docs/CURRENT_API_CONTRACT.md)
- [Architecture](docs/CURRENT_ARCHITECTURE.md)
- [Database schema](docs/CURRENT_DATABASE_SCHEMA.md)
