#!/usr/bin/env bash
# Bản shell của scripts/bootstrap-openfga.ps1 — dùng khi deploy trên Linux
# server (không có PowerShell). Tạo OpenFGA store mới và write authorization
# model từ gmrag_api/openfga/model.fga, rồi in ra STORE_ID + MODEL_ID.
#
# STORE_ID/MODEL_ID gắn với từng instance OpenFGA. ID trong .env dev KHÔNG dùng
# lại được trên server mới — OpenFGA sẽ trả 404 khi check tuple.
#
# Chạy (trên server, sau khi openfga container đã up):
#   bash scripts/bootstrap-openfga.sh --store-name hrm-rag-prod --env-file .env.prod
#
# Mặc định script chỉ IN ra ID. Thêm --write để tự ghi vào --env-file.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODEL_PATH="$ROOT/gmrag_api/openfga/model.fga"
CLI_IMAGE="openfga/cli:v0.7.15"

STORE_NAME="hrm-rag-prod"
ENV_FILE="$ROOT/.env.prod"
API_URL=""
WRITE_BACK=0

while [ $# -gt 0 ]; do
  case "$1" in
    --store-name) STORE_NAME="$2"; shift 2 ;;
    --env-file)   ENV_FILE="$2";   shift 2 ;;
    --api-url)    API_URL="$2";    shift 2 ;;
    --write)      WRITE_BACK=1;    shift ;;
    *) echo "Tham số lạ: $1" >&2; exit 2 ;;
  esac
done

command -v docker >/dev/null || { echo "Cần docker trên PATH." >&2; exit 1; }
command -v curl   >/dev/null || { echo "Cần curl trên PATH." >&2; exit 1; }
[ -f "$MODEL_PATH" ] || { echo "Không tìm thấy model: $MODEL_PATH" >&2; exit 1; }

PY="$(command -v python3 || command -v python || true)"
[ -n "$PY" ] || { echo "Cần python để parse JSON." >&2; exit 1; }

case "$STORE_NAME" in
  *[!a-zA-Z0-9._-]*) echo "Store name chỉ được chứa chữ, số, dấu chấm, gạch dưới, gạch ngang." >&2; exit 1 ;;
esac

# Nạp OPENFGA_API_URL / OPENFGA_API_TOKEN từ env-file nếu chưa có trong shell.
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  set -a; . "$ENV_FILE"; set +a
fi
# OPENFGA_API_URL trong .env.prod không tồn tại (compose tự đặt http://openfga:8080).
# Từ host, OpenFGA nằm ở loopback theo port compose publish.
[ -n "$API_URL" ] || API_URL="${OPENFGA_API_URL:-http://127.0.0.1:18081}"
API_URL="${API_URL%/}"

: "${OPENFGA_API_TOKEN:?Cần OPENFGA_API_TOKEN (trong $ENV_FILE hoặc biến môi trường)}"
AUTH="Authorization: Bearer $OPENFGA_API_TOKEN"

# --- Từ chối tạo trùng store -------------------------------------------------
existing="$(curl -fsS -H "$AUTH" "$API_URL/stores?page_size=100" \
  | "$PY" -c 'import json,sys
name=sys.argv[1]
data=json.load(sys.stdin)
print(",".join(s["id"] for s in data.get("stores") or [] if s.get("name")==name))' "$STORE_NAME")"

if [ -n "$existing" ]; then
  echo "Store '$STORE_NAME' đã tồn tại (id: $existing)." >&2
  echo "Không tạo trùng. Dùng lại ID đó, hoặc chọn --store-name khác." >&2
  exit 1
fi

# --- Tạo store ---------------------------------------------------------------
STORE_ID="$(curl -fsS -X POST -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"name\":\"$STORE_NAME\"}" "$API_URL/stores" \
  | "$PY" -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
echo "Đã tạo store: $STORE_ID" >&2

cleanup_store() {
  echo "Bootstrap lỗi — xóa store $STORE_ID vừa tạo." >&2
  curl -fsS -X DELETE -H "$AUTH" "$API_URL/stores/$STORE_ID" >/dev/null 2>&1 \
    || echo "Cảnh báo: xóa store $STORE_ID thất bại, cần dọn thủ công." >&2
}
trap cleanup_store ERR

# --- Compile model.fga (DSL) sang JSON --------------------------------------
MODEL_JSON="$(docker run --rm \
  --mount "type=bind,source=$ROOT,target=/work,readonly" \
  -w /work "$CLI_IMAGE" \
  model transform --file /work/gmrag_api/openfga/model.fga \
  --input-format fga --output-format json)"

# CLI trả kèm field thừa (vd id); API chỉ nhận schema_version/type_definitions/conditions.
BODY="$(printf '%s' "$MODEL_JSON" | "$PY" -c 'import json,sys
m=json.load(sys.stdin)
out={"schema_version":m["schema_version"],"type_definitions":m["type_definitions"]}
if m.get("conditions"): out["conditions"]=m["conditions"]
sys.stdout.write(json.dumps(out))')"

MODEL_ID="$(printf '%s' "$BODY" | curl -fsS -X POST -H "$AUTH" \
  -H 'Content-Type: application/json' --data-binary @- \
  "$API_URL/stores/$STORE_ID/authorization-models" \
  | "$PY" -c 'import json,sys; print(json.load(sys.stdin)["authorization_model_id"])')"

trap - ERR

echo "OPENFGA_STORE_ID=$STORE_ID"
echo "OPENFGA_MODEL_ID=$MODEL_ID"

if [ "$WRITE_BACK" = "1" ]; then
  [ -f "$ENV_FILE" ] || { echo "Không tìm thấy $ENV_FILE để ghi." >&2; exit 1; }
  sed -i -e "s|^OPENFGA_STORE_ID=.*|OPENFGA_STORE_ID=$STORE_ID|" \
         -e "s|^OPENFGA_MODEL_ID=.*|OPENFGA_MODEL_ID=$MODEL_ID|" "$ENV_FILE"
  echo "Đã ghi hai ID vào $ENV_FILE." >&2
fi
