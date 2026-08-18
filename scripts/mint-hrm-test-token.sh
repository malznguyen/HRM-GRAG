#!/usr/bin/env bash
# Tạo token HS512 để smoke test API ở chế độ HRM_MODE.
#
# CHỈ dùng cho kiểm thử hệ thống của chính mình. Token thật trong vận hành do
# HRM ký và cấp; script này ký bằng cùng JWT_HMAC_SECRET để mô phỏng một caller
# hợp lệ khi HRM chưa sẵn sàng.
#
# Token in ra stdout là một credential — đừng dán vào chat, ticket hay log.
# Cách dùng an toàn:
#   export RAG_TOKEN="$(bash scripts/mint-hrm-test-token.sh --role ADMIN)"
#
# Tham số:
#   --role      ADMIN | HR | MANAGER | EMPLOYEE   (mặc định ADMIN)
#   --userid    id của caller                     (mặc định smoke-test-admin)
#   --ttl       số giây hiệu lực                  (mặc định 3600)
#   --env-file  file chứa JWT_*                   (mặc định .env)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROLE="ADMIN"
USERID="smoke-test-admin"
TTL=3600
ENV_FILE="$ROOT/.env"

while [ $# -gt 0 ]; do
  case "$1" in
    --role)     ROLE="$2";     shift 2 ;;
    --userid)   USERID="$2";   shift 2 ;;
    --ttl)      TTL="$2";      shift 2 ;;
    --env-file) ENV_FILE="$2"; shift 2 ;;
    *) echo "Tham số lạ: $1" >&2; exit 2 ;;
  esac
done

[ -f "$ENV_FILE" ] || { echo "Không tìm thấy $ENV_FILE" >&2; exit 1; }

get() { grep -E "^$1=" "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d '\r\n'; }

SECRET="$(get JWT_HMAC_SECRET)"
ISSUER="$(get JWT_ISSUER)"
SUBJECT_CLAIM="$(get JWT_SUBJECT_CLAIM)"
AUDIENCE="$(get JWT_AUDIENCE)"
[ -n "$SECRET" ] || { echo "Thiếu JWT_HMAC_SECRET trong $ENV_FILE" >&2; exit 1; }
[ -n "$ISSUER" ] || { echo "Thiếu JWT_ISSUER trong $ENV_FILE" >&2; exit 1; }
SUBJECT_CLAIM="${SUBJECT_CLAIM:-userid}"

if command -v python3 >/dev/null 2>&1 && python3 -c 'import sys; assert sys.version_info.major==3' 2>/dev/null; then
  PY=python3
elif command -v python >/dev/null 2>&1 && python -c 'import sys; assert sys.version_info.major==3' 2>/dev/null; then
  PY=python
else
  echo "Cần Python 3." >&2; exit 1
fi

# ADMIN/HR có quyền upload; MANAGER/EMPLOYEE chỉ đọc và chat (xem docs/api/HANDOVER.md).
case "$ROLE" in
  ADMIN|HR) PERMS='["CHATBOT_USE","CHATBOT_UPLOAD_DOCUMENT"]' ;;
  *)        PERMS='["CHATBOT_USE"]' ;;
esac

SECRET="$SECRET" ISSUER="$ISSUER" SUBJECT_CLAIM="$SUBJECT_CLAIM" AUDIENCE="$AUDIENCE" \
ROLE="$ROLE" USERID="$USERID" TTL="$TTL" PERMS="$PERMS" "$PY" <<'PYEOF'
import base64, hashlib, hmac, json, os, time, uuid

def b64(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=")

secret = os.environ["SECRET"].encode()
now = int(time.time())

payload = {
    os.environ["SUBJECT_CLAIM"]: os.environ["USERID"],
    "sub": os.environ["USERID"],
    "role": os.environ["ROLE"],
    "permissions": json.loads(os.environ["PERMS"]),
    "iss": os.environ["ISSUER"],
    "iat": now,
    "exp": now + int(os.environ["TTL"]),
    "jti": str(uuid.uuid4()),
}
if os.environ.get("AUDIENCE"):
    payload["aud"] = os.environ["AUDIENCE"]

header = {"alg": "HS512", "typ": "JWT"}
signing_input = b".".join([
    b64(json.dumps(header, separators=(",", ":")).encode()),
    b64(json.dumps(payload, separators=(",", ":")).encode()),
])
signature = hmac.new(secret, signing_input, hashlib.sha512).digest()
print((signing_input + b"." + b64(signature)).decode())
PYEOF
