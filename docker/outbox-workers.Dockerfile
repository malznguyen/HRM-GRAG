# Image cho các worker/outbox operator chạy trong Docker Compose.
# Chỉ build binary Rust hiện có; không nhúng credential. Không phải bằng chứng production.
#
# Build context là REPO ROOT (không phải ./gmrag_api). Lý do: worker link vào lib
# `gmrag_api`, mà `mod api_docs` (src/lib.rs:1) nhúng docs/api/openapi.yaml bằng
# include_str!("../../docs/api/openapi.yaml") — file này nằm ngoài gmrag_api/.
#
# Build:  docker build -f docker/outbox-workers.Dockerfile -t hrm-rag/outbox-workers:local .

# Ghim theo baseline host (rustc 1.95) — Cargo.lock cần rustc ≥ 1.94 cho aws/sqlx.
FROM rust:1.95-bookworm AS builder
WORKDIR /src

# Phải có mặt trước khi compile, nếu không include_str! fail lúc build lib.
COPY docs/api/openapi.yaml ./docs/api/openapi.yaml

COPY gmrag_api/Cargo.toml gmrag_api/Cargo.lock ./gmrag_api/
COPY gmrag_api/migrations ./gmrag_api/migrations
COPY gmrag_api/src ./gmrag_api/src

WORKDIR /src/gmrag_api
RUN cargo build --release --locked \
    --bin ingestion-worker \
    --bin process-authz-outbox \
    --bin process-qdrant-outbox \
    --bin process-storage-outbox \
    --bin cleanup-qdrant-orphans \
    --bin cleanup-storage-objects

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 gmrag
COPY --from=builder /src/gmrag_api/target/release/process-authz-outbox /usr/local/bin/process-authz-outbox
COPY --from=builder /src/gmrag_api/target/release/ingestion-worker /usr/local/bin/ingestion-worker
COPY --from=builder /src/gmrag_api/target/release/process-qdrant-outbox /usr/local/bin/process-qdrant-outbox
COPY --from=builder /src/gmrag_api/target/release/process-storage-outbox /usr/local/bin/process-storage-outbox
COPY --from=builder /src/gmrag_api/target/release/cleanup-qdrant-orphans /usr/local/bin/cleanup-qdrant-orphans
COPY --from=builder /src/gmrag_api/target/release/cleanup-storage-objects /usr/local/bin/cleanup-storage-objects
USER gmrag
# Entrypoint do root docker-compose.yml chọn theo service.
