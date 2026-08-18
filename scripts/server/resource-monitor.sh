#!/usr/bin/env bash
# Ghi log tài nguyên mỗi 30s để có bằng chứng định lượng khi stack chết vì thiếu RAM.
#
# Không có script này thì lúc sập chỉ biết "nó chết"; có nó thì biết chính xác
# container nào ăn bao nhiêu RAM tại thời điểm nào, và OOM killer giết cái gì.
#
# Cài:  bash scripts/server/install-monitor.sh
# Đọc:  tail -f /var/log/hrm-rag-resource.log
#       grep OOM /var/log/hrm-rag-resource.log

set -uo pipefail

LOG=/var/log/hrm-rag-resource.log
INTERVAL="${MONITOR_INTERVAL_SECS:-30}"
MAX_BYTES=$((64 * 1024 * 1024))

log() { printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$LOG"; }

rotate_if_needed() {
  [ -f "$LOG" ] || return 0
  local size
  size=$(stat -c %s "$LOG" 2>/dev/null || echo 0)
  if [ "$size" -gt "$MAX_BYTES" ]; then
    mv -f "$LOG" "$LOG.1"
    log "ROTATE log trước đó chuyển sang $LOG.1"
  fi
}

# Con trỏ dmesg đã đọc tới đâu, để không log lặp cùng một sự kiện OOM.
LAST_OOM=""

check_oom() {
  local hits
  hits="$(dmesg 2>/dev/null | grep -iE 'out of memory|oom-killer|oom_reaper' | tail -5)"
  [ -n "$hits" ] || return 0
  [ "$hits" = "$LAST_OOM" ] && return 0
  LAST_OOM="$hits"
  while IFS= read -r line; do
    [ -n "$line" ] && log "OOM $line"
  done <<< "$hits"
}

log "MONITOR START interval=${INTERVAL}s"

while true; do
  rotate_if_needed

  # MemAvailable là con số đáng tin, không phải MemFree.
  read -r mem_total mem_avail swap_total swap_free <<< "$(
    awk '/^MemTotal:/{t=$2} /^MemAvailable:/{a=$2} /^SwapTotal:/{st=$2} /^SwapFree:/{sf=$2}
         END{printf "%d %d %d %d", t/1024, a/1024, st/1024, sf/1024}' /proc/meminfo
  )"
  log "MEM total=${mem_total}Mi available=${mem_avail}Mi swap_total=${swap_total}Mi swap_free=${swap_free}Mi"

  # docker stats một lần (--no-stream); bỏ qua nếu daemon chưa sẵn sàng.
  if docker info >/dev/null 2>&1; then
    docker stats --no-stream --format '{{.Name}} mem={{.MemUsage}} mem%={{.MemPerc}} cpu={{.CPUPerc}}' 2>/dev/null \
      | while IFS= read -r line; do [ -n "$line" ] && log "CTR $line"; done

    # Container đã chết và mã thoát — 137 = SIGKILL, dấu hiệu điển hình của OOM.
    docker ps -a --filter 'status=exited' --format '{{.Names}} exit={{.Status}}' 2>/dev/null \
      | while IFS= read -r line; do [ -n "$line" ] && log "EXITED $line"; done
  else
    log "DOCKER daemon không phản hồi"
  fi

  check_oom
  sleep "$INTERVAL"
done
