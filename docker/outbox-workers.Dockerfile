# Image local-demo cho các worker/outbox operator chạy trong Docker Compose.
# Chỉ build binary Rust hiện có; không nhúng credential. Không phải bằng chứng production.

# Ghim theo baseline host (rustc 1.95) — Cargo.lock cần rustc ≥ 1.94 cho aws/sqlx.
FROM rust:1.95-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN cargo build --release \
    --bin process-authz-outbox \
    --bin process-qdrant-outbox \
    --bin cleanup-storage-objects

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 gmrag
COPY --from=builder /src/target/release/process-authz-outbox /usr/local/bin/process-authz-outbox
COPY --from=builder /src/target/release/process-qdrant-outbox /usr/local/bin/process-qdrant-outbox
COPY --from=builder /src/target/release/cleanup-storage-objects /usr/local/bin/cleanup-storage-objects
USER gmrag
# Entrypoint do root docker-compose.yml chọn theo service.
