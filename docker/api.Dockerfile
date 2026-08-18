# Image cho gmrag_api (HTTP API). Build context là REPO ROOT, không phải
# ./gmrag_api như outbox-workers.Dockerfile — vì src/api_docs.rs:12 nhúng
# docs/api/openapi.yaml bằng include_str!("../../docs/api/openapi.yaml").
#
# Build:  docker build -f docker/api.Dockerfile -t hrm-rag/api:local .
#
# Không nhúng credential. API tự chạy sqlx::migrate! lúc khởi động
# (src/main.rs:50) nên không cần bước migrate riêng.

# Ghim theo rust-toolchain.toml (1.95.0); Cargo.lock cần rustc ≥ 1.94 cho aws/sqlx.
FROM rust:1.95-bookworm AS builder
WORKDIR /src

# Spec phải nằm đúng vị trí tương đối trước khi compile, nếu không include_str! fail.
COPY docs/api/openapi.yaml ./docs/api/openapi.yaml

COPY gmrag_api/Cargo.toml gmrag_api/Cargo.lock ./gmrag_api/
COPY gmrag_api/migrations ./gmrag_api/migrations
COPY gmrag_api/src ./gmrag_api/src

WORKDIR /src/gmrag_api
RUN cargo build --release --locked --bin gmrag_api

FROM debian:bookworm-slim
# curl để healthcheck gọi /health; ca-certificates để gọi https://api.deepseek.com.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 gmrag
# Target dir nằm cạnh Cargo.toml (/src/gmrag_api), khác outbox-workers.Dockerfile
# vì file đó dùng context ./gmrag_api nên crate root của nó là /src.
COPY --from=builder /src/gmrag_api/target/release/gmrag_api /usr/local/bin/gmrag_api
USER gmrag
EXPOSE 18083
CMD ["/usr/local/bin/gmrag_api"]
