# HRM GRAG API

The Vietnamese retrieval-augmented generation API that the HRM system calls.
A Rust/Axum service backed by PostgreSQL with pgvector for application and graph
data, OpenFGA for authorization, Qdrant for live vector retrieval, MinIO/S3 for
object storage, Ollama for the `AITeamVN/Vietnamese_Embedding` model, and
DeepSeek for chat and graph extraction. The OpenFGA model defines platform,
tenant, workspace, and document relations.

This repository contains the API only. There is no frontend here; HRM is the
client.

Sources: `gmrag_api/Cargo.toml`, `docker-compose.yml`,
`gmrag_api/openfga/model.fga:4-30`, `docker/ollama/Modelfile:1-2`.

## Integrating with HRM — start here

Read [`docs/api/HANDOVER.md`](docs/api/HANDOVER.md) first. It gives the base URL,
the Swagger UI the API serves itself at `/docs`, the auth model, and the reading
order for the rest.

- [`docs/api/HANDOVER.md`](docs/api/HANDOVER.md) — handover note for the HRM team.
- [`docs/api/INTEGRATION_GUIDE.md`](docs/api/INTEGRATION_GUIDE.md) — the current
  contract, endpoint by endpoint, including the chat SSE flow.
- [`docs/api/openapi.yaml`](docs/api/openapi.yaml) — the OpenAPI spec. It is
  compiled into the binary via `include_str!` and served at `/openapi.yaml`, so
  the spec and the running server cannot drift apart.
- [`docs/api/examples/`](docs/api/examples) — a `.http` collection and
  `smoke.sh`, an end-to-end shell smoke against a live deployment.
- [`docs/CURRENT_API_CONTRACT.md`](docs/CURRENT_API_CONTRACT.md) — the full
  registered route surface, including the admin/tenant routes that are not
  part of the HRM-facing OpenAPI spec. `phase4_api_contract.rs` embeds this
  file and fails if a registered route is missing from it, so it cannot go
  stale silently.

Sources: `gmrag_api/src/api_docs.rs:12`,
`gmrag_api/tests/phase4_api_contract.rs:214-229`.

## Repository layout

- `gmrag_api/` — the API crate: routes, chat, retrieval, ingestion, storage,
  auth/OpenFGA integration, migrations, and operator binaries.
- `docs/api/` — the integration contract handed to HRM.
- `docs/` — deployment and recovery runbooks.
- `docker/` — Dockerfiles for the API and the outbox workers, plus the Ollama
  `Modelfile`.
- `scripts/` — bring-up, seed, smoke, integration-test, and server operations
  scripts.

Migrations live under `gmrag_api/migrations/` and are applied by the API. There
is no top-level `migrations/` directory.

## Local bring-up

`scripts/p0-bringup-migrate.ps1` is preview-only without `-Execute`: it checks
migration state and prints the planned Compose handoff without touching the
Qdrant collection or the database. `-Execute` removes the configured Qdrant
collection and runs `sqlx migrate run`. Review that reset boundary before using
it.

```powershell
docker compose -p hrm-rag up -d --build
docker compose -p hrm-rag ps
docker compose -p hrm-rag logs --tail 100 ingestion-worker
```

The default Compose file runs Ollama on CPU, so it works without a GPU. On a
host with the NVIDIA Container Toolkit, add the override explicitly:

```powershell
docker compose -p hrm-rag -f docker-compose.yml -f docker-compose.gpu.yml up -d
```

Compose starts the application Postgres, OpenFGA Postgres/migration/server,
MinIO and bucket initialization, Qdrant, Ollama, the durable `ingestion-worker`,
the authz outbox worker, the Qdrant outbox worker, the storage outbox worker,
and the dry-run storage orphan scanner. The API itself stays host-run in this
topology.

The Ollama `Modelfile` needs a local `docker/ollama/model-q8_0.gguf`, which is
not in Git. Import and verification commands are in
[`docs/OLLAMA_MODEL_SETUP.md`](docs/OLLAMA_MODEL_SETUP.md).

Run the API on the host:

```powershell
$env:QDRANT_URL='http://127.0.0.1:16333'
$env:QDRANT_VECTOR_SIZE='1024'
$env:OLLAMA_EMBED_URL='http://127.0.0.1:11435/api/embed'
$env:OLLAMA_EMBED_MODEL='AITeamVN/Vietnamese_Embedding'
cargo run --manifest-path .\gmrag_api\Cargo.toml --locked
```

The API binds to `127.0.0.1:18083`. Compose publishes OpenFGA at `18081`, Qdrant
at `16333`, Ollama at `11435`, MinIO at `19000`, and Postgres at `15432` — all
on loopback only. Fill in the remaining values from `gmrag_api/.env.example`.

Sources: `scripts/p0-bringup-migrate.ps1`, `gmrag_api/src/main.rs:94-97`,
`docker-compose.yml`.

With a workspace id and a bearer token, run the ingestion smoke:

```powershell
$env:GMRAG_SMOKE_WORKSPACE_ID='<workspace-uuid>'
$env:GMRAG_SMOKE_BEARER_TOKEN='<bearer-token>'
.\scripts\run-local-ingestion-smoke.ps1
```

It uploads generated PDF, DOCX, TXT, and MD fixtures, waits for `COMPLETED/DONE`,
checks 1024-dimensional Qdrant vectors, previews each document, checks retrieval
content, and prints `Local ingestion smoke: PASS`.

Source: `scripts/run-local-ingestion-smoke.ps1`.

## Fast verification (no Docker required)

These need no running stack and finish in seconds. CI
(`.github/workflows/ci.yml`) runs exactly this set.

```powershell
Set-Location .\gmrag_api
cargo fmt --check
cargo clippy --all-targets
cargo test --lib          # 5 infra-dependent tests are #[ignore]d
```

The full integration suite needs a provisioned isolated environment. Run it with
`.\scripts\run-isolated-integration-tests.ps1` after `docker compose up -d`;
the five ignored tests belong to that tier and run there via
`cargo test --lib -- --ignored`.

## Mandatory pre-deploy ordering

Run the `backfill-document-workspace-tuples` binary before enabling strict
document ACL checks. It backfills missing `workspace` relations for existing
documents and reports inserted versus existing relations. Restricted-document
checks use the document's `bypass_viewer` relation, which the OpenFGA model
derives through the document's workspace and tenant owner.

```powershell
Set-Location .\gmrag_api
cargo run --bin backfill-document-workspace-tuples
```

Sources: `gmrag_api/src/bin/backfill-document-workspace-tuples.rs:18-55`,
`gmrag_api/src/auth/document_acl.rs:254-320`, `gmrag_api/openfga/model.fga:25-30`.

## Deployment and recovery

- [`docs/DEPLOY_SERVER.md`](docs/DEPLOY_SERVER.md) — deploying the stack to the
  LAN server, including the resource-monitoring evidence path.
- [`docs/RECOVERY_RUNBOOK.md`](docs/RECOVERY_RUNBOOK.md) — rebuilding the
  environment on a fresh machine, and what is deliberately not in Git.
- [`docs/OLLAMA_MODEL_SETUP.md`](docs/OLLAMA_MODEL_SETUP.md) — importing and
  verifying the embedding model.

Production runs with `HRM_MODE=true`: HRM signs the JWT, the API verifies it, and
no Keycloak is deployed. The `KEYCLOAK_*` variables in `.env.example` only apply
to the `HRM_MODE=false` path and to integration tests, which run with
`TEST_BYPASS_KEYCLOAK=1` and never contact a live Keycloak.

Source: `scripts/generate-prod-env.sh:12`, `gmrag_api/src/auth/hrm.rs:33-45`.
