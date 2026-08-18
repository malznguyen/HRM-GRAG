#!/usr/bin/env bash
# Đo tải đường CHAT (không phải ingest) để trả lời: bao nhiêu người hỏi cùng lúc
# thì hệ thống còn dùng được?
#
# Hai pha tách bạch, vì chúng nghẽn ở chỗ khác nhau:
#   PHA A — embed: bắn thẳng vào Ollama. Đây là bước CPU nặng nhất của mỗi câu
#           hỏi và dùng chung đúng một model với ingestion. MIỄN PHÍ.
#   PHA B — chat đầy đủ: qua API thật (authz + embed + Qdrant + SQL + DeepSeek).
#           CÓ TỐN TIỀN DeepSeek — mỗi virtual user gửi 1 câu hỏi.
#
# Mỗi virtual user dùng một `userid` riêng để không dính rate limit
# (30 chat/60s/user, rate_limit.rs:79-82).
#
# Chạy:
#   bash scripts/server/loadtest-chat.sh --phase a
#   bash scripts/server/loadtest-chat.sh --phase b --users 30

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="$ROOT/.env.prod"
BASE_URL="http://127.0.0.1:18083"
OLLAMA_URL="http://127.0.0.1:11435/api/embed"
PHASE="a"
USERS=30
QUESTION="Giờ làm việc của công ty là mấy giờ?"

while [ $# -gt 0 ]; do
  case "$1" in
    --phase)    PHASE="$2";    shift 2 ;;
    --users)    USERS="$2";    shift 2 ;;
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --base-url) BASE_URL="$2"; shift 2 ;;
    *) echo "Tham số lạ: $1" >&2; exit 2 ;;
  esac
done

# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a
MODEL="${OLLAMA_EMBED_MODEL:-AITeamVN/Vietnamese_Embedding}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# In p50/p95/max từ danh sách thời gian (giây, mỗi dòng một số).
stats() {
  sort -n "$1" | awk -v label="$2" '
    {a[NR]=$1; s+=$1}
    END{
      if(NR==0){printf "%-28s (khong co mau)\n", label; exit}
      p50=a[int(NR*0.50)+((NR*0.50)==int(NR*0.50)?0:1)]
      p95=a[int(NR*0.95)+((NR*0.95)==int(NR*0.95)?0:1)]
      printf "%-28s n=%-3d  tb=%6.2fs  p50=%6.2fs  p95=%6.2fs  max=%6.2fs\n",
             label, NR, s/NR, p50, p95, a[NR]
    }'
}

sample_load() { awk '{printf "load=%s", $1}' /proc/loadavg; free -m | awk '/^Mem:/{printf "  ram_avail=%sMi\n", $7}'; }

# ---------------------------------------------------------------- PHA A: embed
if [ "$PHASE" = "a" ]; then
  echo "### PHA A — embed đồng thời (Ollama, không tốn tiền)"
  echo "model=$MODEL"
  echo ""
  # Warm-up để model nằm sẵn trong RAM, nếu không mẫu đầu tiên bị tính cả thời gian nạp.
  curl -s -o /dev/null --max-time 120 "$OLLAMA_URL" -H 'Content-Type: application/json' \
    -d "{\"model\":\"$MODEL\",\"input\":\"warmup\"}"

  for c in 1 5 10 20 40; do
    : > "$WORK/t_$c"
    start=$(date +%s.%N)
    for i in $(seq 1 "$c"); do
      (
        t=$(curl -s -o /dev/null -w '%{time_total}' --max-time 600 "$OLLAMA_URL" \
          -H 'Content-Type: application/json' \
          -d "{\"model\":\"$MODEL\",\"input\":\"Cau hoi so $i ve quy dinh nghi phep va gio lam viec\"}")
        echo "$t" >> "$WORK/t_$c"
      ) &
    done
    wait
    end=$(date +%s.%N)
    wall=$(awk -v a="$start" -v b="$end" 'BEGIN{printf "%.2f", b-a}')
    printf "concurrency=%-3d wall=%6.2fs  " "$c" "$wall"
    stats "$WORK/t_$c" ""
    sample_load
    echo ""
  done
  exit 0
fi

# ----------------------------------------------------------- PHA B: chat thật
echo "### PHA B — $USERS user hỏi đồng thời qua API (CÓ tốn tiền DeepSeek)"
: > "$WORK/chat_times"
: > "$WORK/chat_codes"

start=$(date +%s.%N)
for i in $(seq 1 "$USERS"); do
  (
    tok="$(bash "$ROOT/scripts/mint-hrm-test-token.sh" --role EMPLOYEE \
            --userid "loadtest-user-$i" --ttl 3600 --env-file "$ENV_FILE")"
    sid="$(cat /proc/sys/kernel/random/uuid)"
    t=$(curl -s -o "$WORK/out_$i" -D "$WORK/hdr_$i" -w '%{time_total}|%{http_code}' --max-time 600 \
      -X POST "$BASE_URL/workspaces/hrm/chat" \
      -H "Authorization: Bearer $tok" -H 'Content-Type: application/json' \
      -H 'Accept: text/event-stream' \
      -d "{\"session_id\":\"$sid\",\"message\":\"$QUESTION\"}")
    echo "${t%%|*}" >> "$WORK/chat_times"
    echo "${t##*|}" >> "$WORK/chat_codes"
    # Tách riêng thời gian của request ĐƯỢC PHỤC VỤ: gộp chung với request bị
    # từ chối sẽ làm p50 đẹp lên một cách giả tạo (503 trả về gần như tức thì).
    if [ "${t##*|}" = "200" ]; then echo "${t%%|*}" >> "$WORK/ok_times"; fi
  ) &
done
wait
end=$(date +%s.%N)

echo ""
printf "wall clock toan bo: %.2fs\n" "$(awk -v a="$start" -v b="$end" 'BEGIN{print b-a}')"
stats "$WORK/chat_times" "tat ca request"
[ -s "$WORK/ok_times" ] && stats "$WORK/ok_times" "chi request duoc phuc vu"
echo ""
echo "--- phan bo HTTP code ---"
sort "$WORK/chat_codes" | uniq -c | sort -rn
echo ""
served=$(grep -l "event: citations" "$WORK"/out_* 2>/dev/null | wc -l)
errored=$(grep -l "event: error" "$WORK"/out_* 2>/dev/null | wc -l)
# `grep -l | wc -l` đếm SỐ FILE khớp. Bản trước dùng `grep -lc | grep -c ":1"`
# và luôn ra 0 vì -l và -c loại trừ nhau: -l thắng, output không có ":1".
busy=$(grep -l "CHAT_BUSY" "$WORK"/out_* 2>/dev/null | wc -l)
echo "tra loi HOAN CHINH (co event: citations) : $served / $USERS"
echo "stream bi dut giua chung (event: error)  : $errored"
echo "bi tu choi lich su (503 CHAT_BUSY)       : $busy"
echo ""
echo "--- header Retry-After tren cac response 503 ---"
grep -ih "^retry-after:" "$WORK"/hdr_* 2>/dev/null | sort | uniq -c || echo "(khong co)"
echo ""
sample_load
