#!/usr/bin/env bash
# Bản shell của scripts/seed-hrm-workspace.ps1 — dùng khi deploy trên Linux server.
#
# Tạo tenant + workspace của HRM trong Postgres và hai tuple CẤU TRÚC trong
# OpenFGA. Không có bước này, mọi request tới /workspaces/hrm/... trả
# 404 RESOURCE_NOT_FOUND vì handler không resolve được tenant_id từ workspace_id
# (src/routes/documents.rs:1254).
#
# KHÔNG seed tuple admin/member cho user: HRM provisioning tự suy ra từ claim
# `role` đã ký trong token và giữ đồng bộ khi role đổi (src/auth/hrm.rs).
#
# Chạy (trên server, sau khi postgres + openfga đã up và đã bootstrap store):
#   bash scripts/seed-hrm-workspace.sh --env-file .env.prod

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT/.env.prod"
COMPOSE_FILE="$ROOT/docker-compose.prod.yml"
PROJECT="hrm-rag"
TENANT_NAME="HRM"
WORKSPACE_NAME="HRM"
API_URL=""

while [ $# -gt 0 ]; do
  case "$1" in
    --env-file)       ENV_FILE="$2";       shift 2 ;;
    --compose-file)   COMPOSE_FILE="$2";   shift 2 ;;
    --project)        PROJECT="$2";        shift 2 ;;
    --tenant-name)    TENANT_NAME="$2";    shift 2 ;;
    --workspace-name) WORKSPACE_NAME="$2"; shift 2 ;;
    --api-url)        API_URL="$2";        shift 2 ;;
    *) echo "Tham số lạ: $1" >&2; exit 2 ;;
  esac
done

[ -f "$ENV_FILE" ] || { echo "Không tìm thấy $ENV_FILE" >&2; exit 1; }
# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a

PY="$(command -v python3 || command -v python || true)"
[ -n "$PY" ] || { echo "Cần python để parse JSON." >&2; exit 1; }

: "${HRM_TENANT_ID:?Thiếu HRM_TENANT_ID}"
: "${HRM_WORKSPACE_ID:?Thiếu HRM_WORKSPACE_ID}"
: "${OPENFGA_STORE_ID:?Thiếu OPENFGA_STORE_ID — chạy bootstrap-openfga.sh trước}"
: "${OPENFGA_MODEL_ID:?Thiếu OPENFGA_MODEL_ID — chạy bootstrap-openfga.sh trước}"
: "${OPENFGA_API_TOKEN:?Thiếu OPENFGA_API_TOKEN}"

UUID_RE='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
[[ "$HRM_TENANT_ID"    =~ $UUID_RE ]] || { echo "HRM_TENANT_ID không phải UUID." >&2; exit 1; }
[[ "$HRM_WORKSPACE_ID" =~ $UUID_RE ]] || { echo "HRM_WORKSPACE_ID không phải UUID." >&2; exit 1; }

[ -n "$API_URL" ] || API_URL="${OPENFGA_API_URL:-http://127.0.0.1:18081}"
API_URL="${API_URL%/}"
AUTH="Authorization: Bearer $OPENFGA_API_TOKEN"

DC=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --env-file "$ENV_FILE")

# --- 1. Postgres -------------------------------------------------------------
echo "== Seed Postgres =="
SQL="BEGIN;
INSERT INTO tenants (id, name) VALUES ('$HRM_TENANT_ID', '${TENANT_NAME//\'/\'\'}')
  ON CONFLICT (id) DO NOTHING;
INSERT INTO workspaces (id, tenant_id, name) VALUES ('$HRM_WORKSPACE_ID', '$HRM_TENANT_ID', '${WORKSPACE_NAME//\'/\'\'}')
  ON CONFLICT (id) DO NOTHING;
COMMIT;
SELECT (SELECT COUNT(*) FROM tenants WHERE id = '$HRM_TENANT_ID')::text || '|' ||
       (SELECT COUNT(*) FROM workspaces WHERE id = '$HRM_WORKSPACE_ID' AND tenant_id = '$HRM_TENANT_ID')::text;"

VERIFY="$("${DC[@]}" exec -T postgres \
  psql -v ON_ERROR_STOP=1 -qAt -U "${POSTGRES_USER:-hrm_rag_user}" -d "${POSTGRES_DB:-hrm_rag}" -c "$SQL" \
  | tr -d '\r' | grep -E '^[0-9]+\|[0-9]+$' | tail -1)"

if [ "$VERIFY" != "1|1" ]; then
  echo "Seed SQL không đạt (tenant|workspace=$VERIFY)." >&2
  echo "Nhiều khả năng workspace id đã thuộc về một tenant khác." >&2
  exit 1
fi
echo "tenants=1 workspaces=1"

# --- 2. Tuple cấu trúc trong OpenFGA ----------------------------------------
echo "== Seed tuple OpenFGA =="

read_tuples() {
  local token="" body out
  : > /tmp/fga_keys.$$
  while :; do
    if [ -n "$token" ]; then body="{\"page_size\":100,\"continuation_token\":\"$token\"}"
    else body='{"page_size":100}'; fi
    out="$(printf '%s' "$body" | curl -fsS -X POST -H "$AUTH" -H 'Content-Type: application/json' \
      --data-binary @- "$API_URL/stores/$OPENFGA_STORE_ID/read")"
    printf '%s' "$out" | "$PY" -c 'import json,sys
d=json.load(sys.stdin)
for t in d.get("tuples") or []:
    k=t["key"]; print("%s|%s|%s" % (k["user"],k["relation"],k["object"]))' >> /tmp/fga_keys.$$
    token="$(printf '%s' "$out" | "$PY" -c 'import json,sys; print(json.load(sys.stdin).get("continuation_token") or "")')"
    [ -n "$token" ] || break
  done
  cat /tmp/fga_keys.$$
  rm -f /tmp/fga_keys.$$
}

WANT_1="platform:system|platform|tenant:$HRM_TENANT_ID"
WANT_2="tenant:$HRM_TENANT_ID|tenant|workspace:$HRM_WORKSPACE_ID"

EXISTING="$(read_tuples)"
WRITES=""
grep -qxF "$WANT_1" <<< "$EXISTING" || WRITES="$WRITES{\"user\":\"platform:system\",\"relation\":\"platform\",\"object\":\"tenant:$HRM_TENANT_ID\"},"
grep -qxF "$WANT_2" <<< "$EXISTING" || WRITES="$WRITES{\"user\":\"tenant:$HRM_TENANT_ID\",\"relation\":\"tenant\",\"object\":\"workspace:$HRM_WORKSPACE_ID\"},"

if [ -n "$WRITES" ]; then
  printf '{"writes":{"tuple_keys":[%s]}}' "${WRITES%,}" \
    | curl -fsS -X POST -H "$AUTH" -H 'Content-Type: application/json' --data-binary @- \
      "$API_URL/stores/$OPENFGA_STORE_ID/write" >/dev/null
  echo "đã ghi tuple còn thiếu"
else
  echo "hai tuple đã tồn tại"
fi

ACTUAL="$(read_tuples)"
for want in "$WANT_1" "$WANT_2"; do
  grep -qxF "$want" <<< "$ACTUAL" || { echo "Verify tuple thất bại: $want" >&2; exit 1; }
done

echo "HRM_TENANT_ID=$HRM_TENANT_ID"
echo "HRM_WORKSPACE_ID=$HRM_WORKSPACE_ID"
echo "OPENFGA_STRUCTURAL_TUPLES=2"
