#!/usr/bin/env bash
# Cài resource-monitor.sh thành systemd service, và bảo vệ sshd khỏi OOM killer.
#
# Bảo vệ sshd là bắt buộc: máy chỉ có 1.8GB RAM cho một stack cần ~4-6GB, nên
# OOM killer chắc chắn sẽ hoạt động. Nếu nó chọn sshd thì mất luôn đường vào
# máy và phải nhờ console VMware.

set -euo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/resource-monitor.sh"
[ -f "$SRC" ] || { echo "Không tìm thấy $SRC" >&2; exit 1; }
[ "$(id -u)" = "0" ] || { echo "Cần chạy bằng root." >&2; exit 1; }

install -m 0755 "$SRC" /usr/local/bin/hrm-rag-resource-monitor

cat > /etc/systemd/system/hrm-rag-monitor.service <<'UNIT'
[Unit]
Description=HRM RAG resource monitor (bằng chứng cho việc xin nâng hạ tầng)
After=docker.service

[Service]
Type=simple
ExecStart=/usr/local/bin/hrm-rag-resource-monitor
Restart=always
RestartSec=10
# Monitor phải sống sót qua OOM để còn ghi lại được chính sự kiện đó.
OOMScoreAdjust=-900

[Install]
WantedBy=multi-user.target
UNIT

# sshd: -1000 = miễn nhiễm OOM killer. Giữ đường vào máy bằng mọi giá.
mkdir -p /etc/systemd/system/sshd.service.d
cat > /etc/systemd/system/sshd.service.d/oom.conf <<'UNIT'
[Service]
OOMScoreAdjust=-1000
UNIT

systemctl daemon-reload
systemctl enable hrm-rag-monitor.service >/dev/null
systemctl restart hrm-rag-monitor.service
systemctl restart sshd

echo "Monitor: $(systemctl is-active hrm-rag-monitor.service)"
echo "Log: /var/log/hrm-rag-resource.log"
echo "sshd OOMScoreAdjust=-1000 đã áp dụng."
