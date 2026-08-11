#!/usr/bin/env bash
#
# Smoke test end-to-end cho phần API mà HRM tích hợp.
# Chạy end-to-end: health -> upload -> poll -> chat -> 4 route history -> cleanup.
#
# Cách dùng:
#   export RAG_BASE_URL="<RAG_BASE_URL>"
#   export RAG_TOKEN="<token ADMIN/HR có CHATBOT_USE + CHATBOT_UPLOAD_DOCUMENT>"
#   # MANAGER/EMPLOYEE không dùng được cho smoke: upload=403, DELETE=404.
#   # Script luôn dùng alias workspace "hrm".
#   ./smoke.sh [đường-dẫn-file-để-upload]
#
# Không có file truyền vào thì script tự tạo một file .md tạm.
#
# KHÔNG hard-code token vào script này.
#
# Yêu cầu: bash, curl, Python 3 qua lệnh `python3` hoặc `python`.

set -uo pipefail

BASE_URL="${RAG_BASE_URL:-}"
BASE_URL="${BASE_URL%/}"
WORKSPACE_ID="hrm"
TOKEN="${RAG_TOKEN:-}"
UPLOAD_FILE="${1:-}"

POLL_TIMEOUT_SECS="${RAG_POLL_TIMEOUT_SECS:-900}"   # 15 phút, xem mục 4.5 của guide

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[34m%s\033[0m\n' "$*"; }
step()  { printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }

die() { red "FAIL: $*"; exit 1; }

# Git Bash trên Windows có thể thấy App Execution Alias `python3` nhưng alias đó
# không chạy. Probe thực sự rồi fallback sang `python`.
if command -v python3 >/dev/null 2>&1 && python3 -c 'import sys; assert sys.version_info.major == 3' >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1 && python -c 'import sys; assert sys.version_info.major == 3' >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  die "cần Python 3 qua lệnh python3 hoặc python để đọc JSON"
fi

# Đọc một field từ JSON trên stdin. Không cần jq.
json_get() {
  "$PYTHON_BIN" -c "
import json,sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for k in sys.argv[1].split('.'):
    if isinstance(d, list):
        d = d[int(k)]
    else:
        d = d.get(k) if isinstance(d, dict) else None
    if d is None:
        print('')
        sys.exit(0)
print(d)
" "$1" 2>/dev/null
}

[ -n "$BASE_URL" ]     || die "Chưa đặt RAG_BASE_URL."
[ -n "$TOKEN" ]        || die "Chưa đặt RAG_TOKEN."

command -v curl    >/dev/null || die "cần curl"

WS="${BASE_URL}/workspaces/${WORKSPACE_ID}"
AUTH="Authorization: Bearer ${TOKEN}"

# ---------------------------------------------------------------- 1. health
step "1/9  Health check"

health_body="$(curl -sS --max-time 10 "${BASE_URL}/health")" \
  || die "không gọi được ${BASE_URL}/health — API có đang chạy và có gọi được từ máy này không?"

echo "$health_body"
[ "$(printf '%s' "$health_body" | json_get status)" = "ok" ] \
  || die "health không trả status=ok"
green "OK — API sống, database kết nối được"

# ---------------------------------------------------------------- 2. upload
step "2/9  Upload tài liệu"

CLEANUP_FILE=""
if [ -z "$UPLOAD_FILE" ]; then
  UPLOAD_FILE="$(mktemp -t rag-smoke-XXXXXX).md"
  CLEANUP_FILE="$UPLOAD_FILE"
  cat > "$UPLOAD_FILE" <<'EOF'
# NỘI QUY CÔNG TY — TÀI LIỆU SMOKE TEST

## Điều 1. Giờ làm việc
Giờ làm việc chính thức là từ 08:00 đến 17:00, từ thứ Hai đến thứ Sáu.
Nghỉ trưa từ 12:00 đến 13:00.

## Điều 2. Ngày nghỉ
Thứ Bảy và Chủ Nhật là ngày nghỉ.
EOF
  blue "Không truyền file — dùng file tạm: $UPLOAD_FILE"
fi

[ -f "$UPLOAD_FILE" ] || die "không thấy file: $UPLOAD_FILE"

upload_response="$(mktemp)"
upload_code="$(curl -sS -o "$upload_response" -w '%{http_code}' \
  -X POST "${WS}/documents/upload" \
  -H "$AUTH" \
  -F "file=@${UPLOAD_FILE}")"

cat "$upload_response"; echo

if [ "$upload_code" != "202" ]; then
  code="$(json_get error.code < "$upload_response")"
  rm -f "$upload_response" "$CLEANUP_FILE"
  die "upload trả HTTP ${upload_code} (code=${code:-?}) — mong đợi 202"
fi

DOCUMENT_ID="$(json_get documents.0.document_id < "$upload_response")"
rm -f "$upload_response"
[ -n "$DOCUMENT_ID" ] || die "response 202 nhưng không có document_id"

green "OK — document_id = ${DOCUMENT_ID}"
blue  "Nhắc lại: HRM PHẢI lưu document_id này vào database của mình (mục 5.2)."

# ---------------------------------------------------------------- 3. poll
step "3/9  Poll trạng thái (tối đa ${POLL_TIMEOUT_SECS}s)"

started=$(date +%s)
final_status=""

while :; do
  now=$(date +%s); elapsed=$(( now - started ))
  if [ "$elapsed" -ge "$POLL_TIMEOUT_SECS" ]; then
    red "Quá ${POLL_TIMEOUT_SECS}s vẫn chưa xong — coi như treo. Báo team RAG kèm document_id."
    break
  fi

  status_body="$(curl -sS "${WS}/documents/${DOCUMENT_ID}" -H "$AUTH")"
  st="$(printf '%s' "$status_body" | json_get status)"
  stage="$(printf '%s' "$status_body" | json_get processing_stage)"
  chunks="$(printf '%s' "$status_body" | json_get chunk_count)"

  printf '  [%3ds] status=%-11s stage=%-17s chunks=%s\n' \
    "$elapsed" "${st:-?}" "${stage:-?}" "${chunks:-?}"

  case "$st" in
    COMPLETED)
      final_status="COMPLETED"
      green "OK — đã index xong"
      if [ "${chunks:-0}" = "0" ]; then
        red "CẢNH BÁO: COMPLETED nhưng chunk_count=0 — tài liệu sẽ không bao giờ được trích dẫn (mục 4.2)."
      fi
      break
      ;;
    FAILED)
      final_status="FAILED"
      fcode="$(printf '%s' "$status_body" | json_get failure_code)"
      fmsg="$(printf '%s' "$status_body" | json_get failure_message)"
      red "THẤT BẠI — failure_code=${fcode}"
      red "           failure_message=${fmsg}"
      case "$fcode" in
        NEEDS_OCR)
          red "  -> PDF scan/ảnh. Lỗi vĩnh viễn, upload lại vô ích (mục 8.3)." ;;
        PDF_PARSE_FAILED|DOCX_PARSE_FAILED|TEXT_DECODE_FAILED)
          red "  -> Lỗi của file. Người dùng phải sửa file rồi upload lại." ;;
        *)
          red "  -> Lỗi hệ thống. Upload lại có thể thành công." ;;
      esac
      break
      ;;
    PROCESSING) : ;;
    *)
      red "status lạ: '${st}' — response: ${status_body}"
      break
      ;;
  esac

  # Chu kỳ tăng dần, theo mục 4.5 của guide
  if   [ "$elapsed" -lt 30  ]; then sleep 2
  elif [ "$elapsed" -lt 300 ]; then sleep 5
  else                              sleep 30
  fi
done

# ---------------------------------------------------------------- 4. chat
step "4/9  Chat (SSE)"

SESSION_ID=""
if [ "$final_status" != "COMPLETED" ]; then
  red "Bỏ qua chat: tài liệu chưa COMPLETED nên chat không thấy nội dung này."
else
  SESSION_ID="$("$PYTHON_BIN" -c 'import uuid;print(uuid.uuid4())')"
  blue "session_id (client tự sinh) = ${SESSION_ID}"

  sse_raw="$(mktemp)"
  chat_body="$(mktemp)"

  # Body đi qua file chứ không qua `-d "..."`: trên Git Bash/Windows, chuỗi tiếng
  # Việt truyền thẳng làm tham số bị chuyển mã trước khi tới curl, server nhận
  # được JSON hỏng và trả 400 INVALID_REQUEST. Ghi file rồi --data-binary thì
  # bytes tới nguyên vẹn trên mọi nền tảng.
  printf '%s' "{\"session_id\":\"${SESSION_ID}\",\"message\":\"Giờ làm việc của công ty là mấy giờ?\"}" > "$chat_body"

  # -N (--no-buffer) là bắt buộc, nếu không curl sẽ đệm cả stream.
  curl -sS -N \
    -X POST "${WS}/chat" \
    -H "$AUTH" \
    -H 'Content-Type: application/json' \
    -H 'Accept: text/event-stream' \
    --data-binary "@${chat_body}" \
    > "$sse_raw"

  rm -f "$chat_body"

  echo "--- stream nguyên văn ---"
  cat "$sse_raw"
  echo "--- hết stream ---"

  # Ráp lại đúng cách: gom TOÀN BỘ text rồi mới parse marker (mục 6.4).
  # PYTHONIOENCODING cần thiết trên Git Bash/Windows, nơi stdout mặc định là
  # cp1252 và sẽ ném UnicodeEncodeError khi in tiếng Việt.
  PYTHONIOENCODING=utf-8 "$PYTHON_BIN" - "$sse_raw" <<'PY'
import json, re, sys

raw = open(sys.argv[1], encoding="utf-8").read()

answer, citations, session_id, stream_error = [], [], None, None

# Tách theo block, mỗi event cách nhau bằng một dòng trống.
for block in raw.split("\n\n"):
    name, data = None, []
    for line in block.splitlines():
        if line.startswith(":"):           # comment keep-alive -> bỏ qua
            continue
        if line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            # Đúng chuẩn SSE: sau "data:" chỉ bỏ **một** dấu cách phân cách.
            # Dùng lstrip() sẽ nuốt luôn dấu cách thật của câu trả lời — server gửi
            # "data:  làm" (một cách phân cách + một cách thật) và text ráp lại sẽ
            # dính liền thành "Giờlàmviệc".
            chunk = line[5:]
            data.append(chunk[1:] if chunk.startswith(" ") else chunk)
    if not data:
        continue
    payload = "\n".join(data)

    if name in (None, "message"):
        answer.append(payload)             # CHỈ GOM, không parse ở đây
    elif name == "citations":
        citations = json.loads(payload).get("citations", [])
    elif name == "done":
        session_id = payload.strip()
    elif name == "error":
        stream_error = payload

text = "".join(answer)

print()
print("text đã gom  :", repr(text))
print("số citation  :", len(citations))
print("session_id   :", session_id)
if stream_error:
    print("LỖI GIỮA CHỪNG:", stream_error, "(HTTP vẫn 200 — mục 6.10)")

ok = True
if session_id is None:
    print("FAIL: không nhận được event: done — stream chưa kết thúc bình thường")
    ok = False

# Map theo GIÁ TRỊ index, không theo vị trí trong mảng (mục 6.7)
by_index = {c["index"]: c for c in citations}
markers  = [int(n) for n in re.findall(r"\[chunk:(\d+)\]", text)]
print("marker tìm thấy:", markers)

def render(m):
    c = by_index.get(int(m.group(1)))
    return "" if c is None else f" [nguồn: {c['document_name']}]"

print("hiển thị cuối  :", re.sub(r"\[chunk:(\d+)\]", render, text))

unmapped = [n for n in markers if n not in by_index]
if unmapped:
    print(f"Lưu ý: marker không map được {unmapped} — đã xóa khỏi text (mục 6.7)")

if markers and not citations:
    print("FAIL: có marker nhưng citations rỗng")
    ok = False

sys.exit(0 if ok else 1)
PY
  chat_rc=$?
  rm -f "$sse_raw"
  [ "$chat_rc" -eq 0 ] && green "OK — chat stream hợp lệ" || red "Chat stream có vấn đề"
fi

# ------------------------------------------------------- 5-8. chat history
if [ -n "$SESSION_ID" ]; then
  step "5/9  Đọc history bằng query session_id"
  history_body="$(mktemp)"
  history_code="$(curl -sS -o "$history_body" -w '%{http_code}' \
    "${WS}/chat/history?session_id=${SESSION_ID}" -H "$AUTH")"
  cat "$history_body"; echo
  [ "$history_code" = "200" ] || die "chat/history trả HTTP ${history_code} — mong đợi 200"
  history_count="$("$PYTHON_BIN" -c 'import json,sys; print(len(json.load(sys.stdin)))' < "$history_body")"
  rm -f "$history_body"
  [ "$history_count" -ge 1 ] || die "chat/history không có message của session vừa chat"
  green "OK — history có ${history_count} message"

  step "6/9  Liệt kê session của user hiện tại"
  sessions_body="$(mktemp)"
  sessions_code="$(curl -sS -o "$sessions_body" -w '%{http_code}' \
    "${WS}/chat/sessions" -H "$AUTH")"
  cat "$sessions_body"; echo
  [ "$sessions_code" = "200" ] || die "chat/sessions trả HTTP ${sessions_code} — mong đợi 200"
  "$PYTHON_BIN" -c 'import json,sys; sid=sys.argv[1]; data=json.load(sys.stdin); sys.exit(0 if any(row.get("id") == sid for row in data) else 1)' \
    "$SESSION_ID" < "$sessions_body" \
    || die "chat/sessions không chứa session vừa chat"
  rm -f "$sessions_body"
  green "OK — tìm thấy session hiện tại trong danh sách"

  step "7/9  Đọc messages bằng path session_id"
  messages_body="$(mktemp)"
  messages_code="$(curl -sS -o "$messages_body" -w '%{http_code}' \
    "${WS}/chat/sessions/${SESSION_ID}/messages" -H "$AUTH")"
  cat "$messages_body"; echo
  [ "$messages_code" = "200" ] || die "session messages trả HTTP ${messages_code} — mong đợi 200"
  messages_count="$("$PYTHON_BIN" -c 'import json,sys; print(len(json.load(sys.stdin)))' < "$messages_body")"
  rm -f "$messages_body"
  [ "$messages_count" -ge 1 ] || die "session messages không có message vừa chat"
  green "OK — messages path có ${messages_count} message"

  step "8/9  Xóa session và xác nhận CASCADE messages"
  session_delete_code="$(curl -sS -o /dev/null -w '%{http_code}' \
    -X DELETE "${WS}/chat/sessions/${SESSION_ID}" -H "$AUTH")"
  [ "$session_delete_code" = "204" ] \
    || die "delete chat session trả HTTP ${session_delete_code} — mong đợi 204"

  after_session_body="$(mktemp)"
  after_session_code="$(curl -sS -o "$after_session_body" -w '%{http_code}' \
    "${WS}/chat/sessions/${SESSION_ID}/messages" -H "$AUTH")"
  [ "$after_session_code" = "200" ] \
    || die "đọc session đã xóa trả HTTP ${after_session_code} — mong đợi 200 []"
  after_session_count="$("$PYTHON_BIN" -c 'import json,sys; print(len(json.load(sys.stdin)))' < "$after_session_body")"
  rm -f "$after_session_body"
  [ "$after_session_count" -eq 0 ] || die "session đã xóa nhưng vẫn còn messages"
  green "OK — 204 và đọc lại trả []; session/messages đã sạch"
fi

# ---------------------------------------------------------------- 9. delete document
step "9/9  Xóa tài liệu"

del_code="$(curl -sS -o /dev/null -w '%{http_code}' \
  -X DELETE "${WS}/documents/${DOCUMENT_ID}" -H "$AUTH")"

[ "$del_code" = "204" ] || die "delete trả HTTP ${del_code} — mong đợi 204"
green "OK — 204 No Content"

after_code="$(curl -sS -o /dev/null -w '%{http_code}' \
  "${WS}/documents/${DOCUMENT_ID}" -H "$AUTH")"

[ "$after_code" = "404" ] || die "sau khi xóa vẫn trả HTTP ${after_code} — mong đợi 404"
green "OK — xác nhận đã xóa (404)"

rm -f "$CLEANUP_FILE"

step "Xong"
green "Luồng document/chat/history đều chạy được và dữ liệu test đã được xóa."
