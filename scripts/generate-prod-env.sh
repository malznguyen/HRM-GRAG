#!/usr/bin/env bash
# Sinh .env.prod cho deploy server từ .env dev.
#
# Nguyên tắc:
#   - JWT_HMAC_SECRET và DEEPSEEK_API_KEY được GIỮ NGUYÊN. JWT_HMAC_SECRET là
#     khóa ký của HRM (họ ký, mình verify) — đổi đơn phương sẽ làm hỏng token
#     HRM đang dùng. Muốn xoay khóa phải hẹn lịch với team HRM.
#   - Mật khẩu nội bộ (postgres, openfga, minio) được SINH MỚI. Chúng chỉ dùng
#     trong compose network nên không cần trùng với dev.
#   - Cờ test (ALLOW_INSECURE_DEFAULTS, ALLOW_LOCAL_TEST_SEED, ALLOW_IDENTITY_E2E)
#     bị loại bỏ; APP_ENV=production.
#   - Toàn bộ cấu hình Keycloak bị loại bỏ (HRM_MODE=true không dùng tới).
#
# Chạy:  bash scripts/generate-prod-env.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/.env"
OUT="$ROOT/.env.prod"

[ -f "$SRC" ] || { echo "Không tìm thấy $SRC" >&2; exit 1; }
if [ -f "$OUT" ]; then
  echo "$OUT đã tồn tại. Xóa thủ công trước nếu thực sự muốn sinh lại" >&2
  echo "(sinh lại sẽ đổi mật khẩu DB — stack đang chạy sẽ không kết nối được)." >&2
  exit 1
fi

# Đọc một biến từ .env, bỏ CR của line ending Windows.
get() { grep -E "^$1=" "$SRC" | head -1 | cut -d= -f2- | tr -d '\r\n'; }
# Chỉ sinh ký tự alphanumeric: tránh ký tự làm hỏng parse của --env-file và URI postgres.
gen() { LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c "${1:-40}"; }

require() {
  local name="$1" value="$2"
  [ -n "$value" ] || { echo "Thiếu $name trong $SRC" >&2; exit 1; }
}

DEEPSEEK_API_KEY="$(get DEEPSEEK_API_KEY)";   require DEEPSEEK_API_KEY "$DEEPSEEK_API_KEY"
JWT_HMAC_SECRET="$(get JWT_HMAC_SECRET)";     require JWT_HMAC_SECRET "$JWT_HMAC_SECRET"
HRM_TENANT_ID="$(get HRM_TENANT_ID)";         require HRM_TENANT_ID "$HRM_TENANT_ID"
HRM_WORKSPACE_ID="$(get HRM_WORKSPACE_ID)";   require HRM_WORKSPACE_ID "$HRM_WORKSPACE_ID"

umask 077
cat > "$OUT" <<EOF
# ĐƯỢC SINH BỞI scripts/generate-prod-env.sh — KHÔNG COMMIT.
# Dùng với: docker compose -p hrm-rag -f docker-compose.prod.yml --env-file .env.prod up -d

APP_ENV=production
DOCS_ENABLED=true
RUST_LOG=info,lopdf=error

# --- Postgres ứng dụng -------------------------------------------------------
POSTGRES_USER=$(get POSTGRES_USER)
POSTGRES_PASSWORD=$(gen 40)
POSTGRES_DB=$(get POSTGRES_DB)
DATABASE_POOL_SIZE=16

# --- Postgres của OpenFGA ----------------------------------------------------
OPENFGA_DB_USER=$(get OPENFGA_DB_USER)
OPENFGA_DB_PASSWORD=$(gen 40)
OPENFGA_DB_NAME=$(get OPENFGA_DB_NAME)

# --- OpenFGA -----------------------------------------------------------------
# STORE_ID/MODEL_ID được điền sau khi chạy scripts/bootstrap-openfga.sh trên server.
OPENFGA_API_TOKEN=$(gen 48)
OPENFGA_STORE_ID=
OPENFGA_MODEL_ID=
AUTHZ_OUTBOX_BATCH_SIZE=50
AUTHZ_OUTBOX_MAX_RETRIES=5

# --- MinIO -------------------------------------------------------------------
MINIO_ROOT_USER=$(get MINIO_ROOT_USER)
MINIO_ROOT_PASSWORD=$(gen 40)
S3_BUCKET=$(get S3_BUCKET)
S3_REGION=$(get S3_REGION)
S3_FORCE_PATH_STYLE=true
S3_PRESIGN_EXPIRY_SECS=900

# --- Qdrant ------------------------------------------------------------------
QDRANT_COLLECTION=$(get QDRANT_COLLECTION)
QDRANT_VECTOR_SIZE=$(get QDRANT_VECTOR_SIZE)
QDRANT_TOP_K=5

# --- Ollama ------------------------------------------------------------------
OLLAMA_EMBED_MODEL=$(get OLLAMA_EMBED_MODEL)

# --- DeepSeek (giữ nguyên từ .env) -------------------------------------------
DEEPSEEK_API_KEY=$DEEPSEEK_API_KEY
DEEPSEEK_API_URL=$(get DEEPSEEK_API_URL)
DEEPSEEK_MODEL=$(get DEEPSEEK_MODEL)
GMRAG_GRAPH_EXTRACTION_ENABLED=false

# --- HRM mode / JWT (giữ nguyên: HRM ký, mình verify) ------------------------
HRM_MODE=true
HRM_TENANT_ID=$HRM_TENANT_ID
HRM_WORKSPACE_ID=$HRM_WORKSPACE_ID
JWT_ALG=$(get JWT_ALG)
JWT_ISSUER=$(get JWT_ISSUER)
JWT_SUBJECT_CLAIM=$(get JWT_SUBJECT_CLAIM)
JWT_AUDIENCE=$(get JWT_AUDIENCE)
JWT_VERIFY_AUDIENCE=$(get JWT_VERIFY_AUDIENCE)
JWT_HMAC_SECRET=$JWT_HMAC_SECRET

# --- Ingestion: hạ concurrency cho máy 1 vCPU --------------------------------
# Không ghi GMRAG_INGESTION_DOCUMENT_CONCURRENCY: worker không đọc biến này;
# API tạo semaphore nhưng không route nào .acquire() — NO-OP.
# Worker tuần tự nhờ INGESTION_JOB_BATCH_SIZE=1.
GMRAG_EMBEDDING_BATCH_SIZE=8
GMRAG_EMBEDDING_CONCURRENCY=1
GMRAG_EMBEDDING_TIMEOUT_SECS=300
INGESTION_JOB_LEASE_SECS=600

# --- Mạng --------------------------------------------------------------------
API_PUBLIC_PORT=18083
# HRM gọi server-to-server nên không cần CORS. Điền origin nếu có frontend gọi thẳng.
CORS_ALLOWED_ORIGINS=
EOF

chmod 600 "$OUT"
echo "Đã sinh $OUT (chmod 600)."
echo "Còn thiếu: OPENFGA_STORE_ID và OPENFGA_MODEL_ID — điền sau khi bootstrap OpenFGA."
