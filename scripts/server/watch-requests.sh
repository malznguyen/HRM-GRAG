#!/usr/bin/env bash
# Xem request HTTP tới API theo thời gian thực.
#
# Vì sao cần script này: API KHÔNG ghi access log cho request thành công
# (tower-http chỉ bật feature `cors`, không có TraceLayer), nên
# `docker compose logs api` chỉ hiện warning/error. Muốn biết ai đang gọi gì
# thì phải bắt gói.
#
# Chỉ đọc được vì endpoint đang chạy HTTP thuần. Khi nào bọc TLS thì script này
# hết tác dụng — lúc đó phải thêm access log ở tầng API hoặc reverse proxy.
#
# Dùng:
#   bash watch-requests.sh                    # tất cả
#   bash watch-requests.sh 192.168.168.88     # lọc theo một IP (vd server HRM)
#
# Ctrl-C để dừng.

set -uo pipefail

PORT="${API_PORT:-18083}"
FILTER_IP="${1:-}"

command -v tcpdump >/dev/null || { echo "Cần tcpdump: yum install -y tcpdump" >&2; exit 1; }

BPF="tcp port $PORT"
[ -n "$FILTER_IP" ] && BPF="$BPF and host $FILTER_IP"

echo "Đang theo dõi cổng $PORT${FILTER_IP:+ (chỉ $FILTER_IP)} — Ctrl-C để dừng"
printf '%-12s %-18s %s\n' "GIỜ" "CLIENT" "VIỆC"
echo "---------------------------------------------------------------------------"

# -A in payload dạng ASCII, -l tắt đệm, -s0 lấy trọn gói.
stdbuf -oL tcpdump -i any -nn -A -s0 -l "$BPF" 2>/dev/null | awk -v port="$PORT" '
  # --- Dòng header của tcpdump: lấy thời gian, IP nguồn và IP đích ---------
  # Vị trí cột đổi theo phiên bản tcpdump (-i any thêm tên interface), nên dò
  # bằng regex thay vì đếm cột.
  match($0, /IP [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+ > [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:/) {
    hdr = substr($0, RSTART + 3, RLENGTH - 4)
    split(hdr, ends, " > ")
    src = ends[1]; dst = ends[2]
    sub(/\.[0-9]+$/, "", src)          # bỏ port, giữ IP
    sub(/:$/, "", ends[2]); dst = ends[2]
    dport = ends[2]; sub(/^.*\./, "", dport)
    sub(/\.[0-9]+$/, "", dst)

    tm = $1
    if (tm !~ /^[0-9][0-9]:/) { for (i = 1; i <= NF; i++) if ($i ~ /^[0-9][0-9]:[0-9][0-9]:/) { tm = $i; break } }
    split(tm, t, ":")
    now = t[1] * 3600 + t[2] * 60 + t[3]
    clock = substr(tm, 1, 8)
    next
  }

  # --- Bỏ trùng ------------------------------------------------------------
  # `-i any` bắt cùng một gói trên nhiều interface (ens192, bridge, veth).
  # Cùng client + cùng nội dung trong vòng 1 giây coi như một lần.
  function dedup(key,   last) {
    last = seen[key]
    if (last != "" && now - last < 1.0) return 0
    seen[key] = now
    return 1
  }

  # --- Dòng mở đầu HTTP request (nằm sau rác nhị phân trên cùng dòng) ------
  match($0, /(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS) \/[^ ]* HTTP\/1\.[01]/) {
    line = substr($0, RSTART, RLENGTH)
    split(line, f, " ")
    path = f[2]
    sub(/\?.*/, "", path)              # cắt query cho gọn
    # Client là bên GỬI request.
    if (dedup(src "|" f[1] "|" path)) {
      printf "%-12s %-18s %s %s\n", clock, src, f[1], path
      fflush()
    }
    next
  }

  # --- Dòng mở đầu HTTP response ------------------------------------------
  match($0, /HTTP\/1\.[01] [0-9][0-9][0-9]/) {
    line = substr($0, RSTART, RLENGTH)
    split(line, f, " ")
    # Client là bên NHẬN response.
    if (dedup(dst "|resp|" f[2] "|" clock)) {
      printf "%-12s %-18s     └─ %s\n", clock, dst, f[2]
      fflush()
    }
    next
  }
'
