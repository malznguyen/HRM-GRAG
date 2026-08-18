# Runbook: deploy HRM RAG lên server LAN

Máy đích: `192.168.168.89` (hostname `hrm-grag`, CentOS 7.9, VMware VM).
Endpoint bàn giao cho HRM: `http://192.168.168.89:18083`.

> **Cảnh báo tài nguyên.** Server có **1 vCPU / 1.8 GB RAM**. Stack cần khoảng
> **4–6 GB**. Lần deploy này được thực hiện có chủ đích để lấy số liệu chứng minh
> nhu cầu nâng hạ tầng — không phải cấu hình đã được xác nhận đủ tài nguyên.
> Xem [mục 7](#7-theo-dõi-tài-nguyên) để lấy bằng chứng.

---

## 1. Chuẩn bị máy chủ

### 1.1. Vá repo CentOS 7 (bắt buộc trước mọi lệnh `yum`)

CentOS 7 đã EOL, `mirror.centos.org` trả về rỗng — `yum repolist` ra **0 gói**.
Phải trỏ về `vault.centos.org`:

```bash
cp -an /etc/yum.repos.d /root/yum.repos.d.bak
sed -i -e 's|^mirrorlist=|#mirrorlist=|g' -e 's|^#\s*baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*.repo
yum clean all && yum repolist
```

Kết quả đúng: `repolist: 16,771`.

### 1.2. Cài Docker

```bash
yum install -y yum-utils
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
systemctl enable --now docker
```

Đã kiểm chứng: Docker `26.1.4`, Compose `v2.27.1`, storage driver `overlay2`.

> **Ghi chú `libseccomp`.** CentOS 7 chỉ có `libseccomp 2.3.1`. Với Docker cũ,
> bản này chặn syscall `clone3` khiến mọi image nền glibc ≥ 2.34 (debian
> bookworm, ubuntu 22.04) không chạy được. Docker 26.1.4 đã xử lý được — đã test
> `debian:bookworm-slim` chạy bình thường. Nếu sau này hạ cấp Docker mà container
> báo `Operation not permitted` lúc khởi động, đây là nguyên nhân; cách chữa là
> nâng `libseccomp` lên 2.5.x hoặc chạy với `--security-opt seccomp=unconfined`.

### 1.3. Thêm swap

**Cạm bẫy:** `/` là **XFS**. `fallocate` tạo file có extent chưa ghi và
`swapon` từ chối với `Invalid argument`. Bắt buộc dùng `dd`:

```bash
dd if=/dev/zero of=/swapfile2 bs=1M count=6144 status=none
chmod 600 /swapfile2
mkswap /swapfile2
swapon /swapfile2
echo '/swapfile2 none swap sw 0 0' >> /etc/fstab
```

Chỉ thêm dòng vào `/etc/fstab` **sau khi** `swapon` thành công, nếu không lần
boot sau sẽ lỗi. Kết quả đúng: `free -h` báo `Swap: 8.0G`.

### 1.4. Đồng bộ đồng hồ (BẮT BUỘC — ảnh hưởng trực tiếp tới xác thực)

Khi nhận máy, system clock của server **nhanh hơn giờ thật 5 giờ 55 phút**, dù
RTC (đồng hồ phần cứng) lại đúng. `chrony`/`ntp` chưa được cài và
`NTP synchronized: no`.

Hậu quả nếu bỏ qua: `JwtValidator` bật `validation.validate_exp` với leeway rất
nhỏ (`src/auth/jwt.rs:195-196`). Lệch 6 tiếng nghĩa là **mọi token HRM hợp lệ đều
bị từ chối** với log `JWT rejected: token expired` — trong khi chữ ký và issuer
hoàn toàn đúng. Rất dễ chẩn đoán nhầm thành "sai secret".

```bash
yum install -y chrony
systemctl enable --now chronyd
chronyc makestep
chronyc tracking
timedatectl | grep -i synchron      # phải ra: NTP synchronized: yes
```

NTP ra Internet (UDP 123) đã kiểm chứng là thông. VMware guest time sync đang
`Disabled` nên không xung đột với chrony.

Kiểm tra lệch giữa hai máy bất kỳ lúc nào:

```bash
echo "local : $(date +%s)"
ssh root@192.168.168.89 'echo "server: $(date +%s)"'
```

Hai số phải chênh nhau dưới vài giây.

---

## 2. Build image (làm ở máy dev, KHÔNG làm trên server)

`cargo build --release` cần > 2 GB RAM lúc link. Trên máy 1.8 GB nó sẽ OOM.
Build ở máy dev rồi chuyển image sang.

```bash
docker build -f docker/api.Dockerfile -t hrm-rag/api:local --provenance=false .
docker build -f docker/outbox-workers.Dockerfile -t hrm-rag/outbox-workers:local --provenance=false ./gmrag_api
```

Hai điểm khác nhau giữa hai Dockerfile, dễ nhầm:

| | `api.Dockerfile` | `outbox-workers.Dockerfile` |
|---|---|---|
| Build context | **repo root** (`.`) | `./gmrag_api` |
| Lý do | `src/api_docs.rs:12` nhúng `docs/api/openapi.yaml` bằng `include_str!` | không cần file ngoài crate |
| Target dir | `/src/gmrag_api/target/release` | `/src/target/release` |

`--provenance=false` là để `docker save` ra archive một manifest; nếu không,
buildx sinh manifest list kèm attestation và `docker load` trên Docker 26 có thể
báo không tìm thấy platform phù hợp.

Chuyển sang server:

```bash
docker save hrm-rag/api:local hrm-rag/outbox-workers:local | gzip -1 > /tmp/hrm-rag-images.tar.gz
scp /tmp/hrm-rag-images.tar.gz root@192.168.168.89:/opt/hrm-rag/
ssh root@192.168.168.89 'gunzip -c /opt/hrm-rag/hrm-rag-images.tar.gz | docker load'
```

---

## 3. Cấu hình

```bash
bash scripts/generate-prod-env.sh     # sinh .env.prod ở máy dev
scp .env.prod root@192.168.168.89:/opt/hrm-rag/
ssh root@192.168.168.89 'chmod 600 /opt/hrm-rag/.env.prod'
```

Script giữ nguyên `JWT_HMAC_SECRET` và `DEEPSEEK_API_KEY`, sinh mới mật khẩu
Postgres/MinIO/OpenFGA, và bỏ toàn bộ cờ test.

> **`JWT_HMAC_SECRET` không được tự ý đổi.** HRM ký token bằng khóa này, mình chỉ
> verify. Đổi đơn phương sẽ làm mọi token HRM đang lưu hành trở thành `401`.
> Muốn xoay khóa phải hẹn lịch cắt với team HRM.

Mọi file được chuyển từ Windows phải chuẩn hóa line ending, nếu không `bash` báo
`$'\r': command not found`:

```bash
sed -i 's/\r$//' /opt/hrm-rag/scripts/*.sh /opt/hrm-rag/scripts/server/*.sh /opt/hrm-rag/.env.prod
```

---

## 4. Model embedding

`docker/ollama/model-q8_0.gguf` (634 MB) không nằm trong git và không nằm trong
image — phải chuyển riêng.

```bash
scp docker/ollama/model-q8_0.gguf docker/ollama/Modelfile root@192.168.168.89:/opt/hrm-rag/docker/ollama/
ssh root@192.168.168.89 'md5sum /opt/hrm-rag/docker/ollama/model-q8_0.gguf'
```

md5 phải khớp `736464f067796ae32eee5e753ea04dfb`.

Import sau khi container `ollama` đã chạy:

```bash
docker compose -p hrm-rag -f docker-compose.prod.yml --env-file .env.prod up -d ollama
docker compose -p hrm-rag exec ollama ollama create AITeamVN/Vietnamese_Embedding -f /models/Modelfile
```

Tên import phải khớp **chính xác** `OLLAMA_EMBED_MODEL`. Model khác bản ghim sẽ
lệch số chiều vector và phải re-embed toàn bộ corpus.

---

## 5. Bootstrap OpenFGA

`OPENFGA_STORE_ID`/`OPENFGA_MODEL_ID` gắn với từng instance OpenFGA. **ID trong
`.env` dev không dùng lại được** — server mới sẽ trả 404 khi check tuple.

```bash
cd /opt/hrm-rag
docker compose -p hrm-rag -f docker-compose.prod.yml --env-file .env.prod up -d openfga
bash scripts/bootstrap-openfga.sh --store-name hrm-rag-prod --env-file .env.prod --write
```

`--write` ghi thẳng hai ID vào `.env.prod`. Không có `--write` thì chỉ in ra.

## 5b. Seed tenant + workspace của HRM

Database mới hoàn toàn trống. `HRM_TENANT_ID` và `HRM_WORKSPACE_ID` trong
`.env.prod` chỉ là *cấu hình*, không tự tạo ra row nào. Thiếu bước này thì mọi
request tới `/workspaces/hrm/...` trả **`404 RESOURCE_NOT_FOUND`** vì handler
không resolve được `tenant_id` từ `workspace_id`
(`src/routes/documents.rs:1254`) — rất dễ tưởng nhầm là sai alias `hrm`.

```bash
cd /opt/hrm-rag
bash scripts/seed-hrm-workspace.sh --env-file .env.prod
```

Script ghi 2 row Postgres (`tenants`, `workspaces`) và 2 tuple **cấu trúc** trong
OpenFGA:

| user | relation | object |
|---|---|---|
| `platform:system` | `platform` | `tenant:<HRM_TENANT_ID>` |
| `tenant:<HRM_TENANT_ID>` | `tenant` | `workspace:<HRM_WORKSPACE_ID>` |

Tuple `admin`/`member` cho từng user **không** được seed — HRM provisioning tự
suy ra từ claim `role` đã ký trong token và giữ đồng bộ khi role đổi.

Phải chạy **sau** khi bootstrap OpenFGA, vì script cần `OPENFGA_STORE_ID`.

---

## 6. Khởi động và mở firewall

```bash
cd /opt/hrm-rag
docker compose -p hrm-rag -f docker-compose.prod.yml --env-file .env.prod up -d
docker compose -p hrm-rag ps

firewall-cmd --zone=public --add-port=18083/tcp --permanent
firewall-cmd --reload
```

Chỉ mở **18083**. Postgres (15432/15433), OpenFGA (18081/18082), MinIO
(19000/19001), Qdrant (16333) và Ollama (11435) đều bind `127.0.0.1` trong
`docker-compose.prod.yml` — giữ nguyên. Ai gọi được OpenFGA là tự cấp được quyền
admin trên mọi workspace.

---

## 7. Theo dõi tài nguyên

```bash
bash scripts/server/install-monitor.sh
tail -f /var/log/hrm-rag-resource.log
```

Script ghi mỗi 30s: RAM/swap khả dụng, `docker stats` từng container, container
đã thoát kèm exit code, và sự kiện OOM từ `dmesg`.

Nó cũng đặt `OOMScoreAdjust=-1000` cho `sshd`. Đây là điều bắt buộc: với 1.8 GB
RAM cho một stack cần 4–6 GB, OOM killer chắc chắn sẽ chạy, và nếu nó chọn
`sshd` thì mất đường vào máy, phải mở console VMware.

Khi cần số liệu xin nâng hạ tầng:

```bash
grep OOM /var/log/hrm-rag-resource.log
grep 'exit=.*137' /var/log/hrm-rag-resource.log     # 137 = SIGKILL, dấu hiệu OOM
awk '/^.*MEM/ {print $1, $3}' /var/log/hrm-rag-resource.log | tail -100
```

---

## 7b. Nới timeout OpenFGA — biện pháp bù phần cứng

`AuthzClient` mặc định connect 2s / request 3s (`src/auth/authz.rs:19-23`). Con số
ngắn là **có chủ đích**: authz nằm trên hot path của mọi request, nên fail-closed
nhanh còn hơn treo và cạn connection pool.

Trên máy 1 vCPU này, lúc nhiều container khởi động cùng lúc, OpenFGA trả lời chậm
hơn 3s và API trả `500 AUTHZ_ERROR` — **dù OpenFGA hoàn toàn khỏe**. Bằng chứng
từ log lần deploy đầu:

```
api-1     | ERROR Failed to synchronize HRM authorization
          | error=HTTP request failed: error sending request for url
          | (http://openfga:8080/stores/01KZZ.../read)     ← API bỏ cuộc 03:13:47.963
openfga-1 | INFO grpc_req_complete ... "grpc_code": 0      ← OpenFGA trả OK 03:13:48.219
api-1     | WARN sqlx::pool::acquire: acquired connection, but time to acquire
          | exceeded slow threshold acquired_after_secs=5.697
```

Cách chữa hiện tại (đã đặt trong `docker-compose.prod.yml`):

```yaml
OPENFGA_CONNECT_TIMEOUT_SECS: 10
OPENFGA_REQUEST_TIMEOUT_SECS: 20
```

Giá trị bị clamp ở 30s (`auth/mod.rs:14`).

> Đây **không phải bản vá đúng**. Nó chỉ giấu triệu chứng của việc thiếu CPU.
> Khi server được nâng cấp, hãy bỏ hai biến này về mặc định — giữ nguyên timeout
> dài nghĩa là một sự cố OpenFGA thật sẽ giữ request treo 20s thay vì fail nhanh.

---

## 8. Smoke test

```bash
curl http://192.168.168.89:18083/health     # {"status":"ok","db":"connected"}
curl http://192.168.168.89:18083/ready      # postgres + openfga healthy
```

Sau đó mở `http://192.168.168.89:18083/docs`, bấm **Authorize**, dán access token
HRM (chỉ token, không thêm tiền tố `Bearer `), rồi thử upload và chat theo
[`docs/api/INTEGRATION_GUIDE.md`](api/INTEGRATION_GUIDE.md).

---

## 8b. Số liệu đo được (2026-08-14, deploy đầu tiên)

Toàn bộ 11/11 bước smoke test **PASS** trên `1 vCPU / 1.8 GB RAM`. Stack **không
sập**, không có sự kiện OOM, không container nào exit 137.

| Chỉ số | Giá trị |
|---|---|
| RAM khả dụng — trung bình | 788 Mi (41 mẫu) |
| RAM khả dụng — thấp nhất | 376 Mi |
| Sự kiện OOM | 0 |
| Container exit 137 | 0 |
| Swap đã dùng | 212 Mi / 8 GB |
| Load average (1 core) | 1.3 – 2.5 |

RAM theo container lúc idle:

| Container | RAM | % |
|---|---|---|
| ollama | 757 MiB | 41.2% |
| minio | 90 MiB | 4.9% |
| postgres | 45 MiB | 2.5% |
| openfga-postgres | 35 MiB | 1.9% |
| qdrant | 32 MiB | 1.8% |
| ingestion-worker | 32 MiB | 1.7% |
| openfga | 19 MiB | 1.0% |
| api | 18 MiB | 1.0% |
| 3 worker còn lại | ~7 MiB | 0.4% |

**Nút thắt thật là CPU, không phải RAM.** Bằng chứng:

- Embedding **1 chunk** mất **12,5 giây** (`elapsed_ms=12573`)
- Embedding query lần đầu 16,6s; lần sau (model đã nằm trong RAM) 1,4s
- `sqlx` cảnh báo `acquired_after_secs=5.69` khi các container khởi động cùng lúc
- Phải nới timeout OpenFGA từ 3s lên 20s mới qua được (mục 7b)

Kết luận cho đề xuất nâng cấp: máy đủ để **tích hợp và kiểm thử**, không đủ cho
nhiều người dùng đồng thời. Một tài liệu vài trăm chunk sẽ mất hàng chục phút để
ingest với tốc độ hiện tại.

Đề xuất: **8 vCPU / 16 GB RAM / 200 GB disk**.

> **Chưa kiểm chứng:** stack chưa được thử qua một lần reboot server. Các service
> đều đặt `restart: unless-stopped` và `docker.service` đã `enable`, swap đã vào
> `/etc/fstab`, nhưng nên hẹn một cửa sổ để reboot xác nhận.

---

## 8c. Sức chịu tải đường CHAT (đo 2026-08-14)

Đo trên chính server, mỗi virtual user một `userid` riêng để không dính rate limit.

### Kết quả

| User hỏi đồng thời | p50 | p95 | HTTP | Trả lời hoàn chỉnh |
|---|---|---|---|---|
| 1 | 15,1s\* | — | 200 | 1/1 |
| 5 | **4,93s** | 5,77s | 200 | 5/5 |
| 10 | **8,72s** | 10,7s | 200 | 10/10 |
| 20 | 43,1s | 45,3s | **502** | **0/20** |
| 30 | — | — | **500** | **0/30** |

\* Mốc 1 user còn trả nốt chi phí nạp model; độ trễ thật khi ấm là ~5s.

**Vách đứng nằm giữa 10 và 20 user đồng thời.** Không có vùng "chậm nhưng dùng
được" ở giữa — dưới ngưỡng thì mọi request thành công, trên ngưỡng thì **100%
thất bại**, không ai nhận được câu trả lời nào.

### Vì sao gãy

Chuỗi nhân quả, theo log chứ không phải suy đoán:

1. `OLLAMA_NUM_PARALLEL=1` — Ollama embed **đúng một request tại một thời điểm**,
   phần còn lại xếp hàng (`OLLAMA_MAX_QUEUE=512`).
2. Embed chiếm trọn 1 core duy nhất của máy.
3. Qdrant bị đói CPU và **đổ trước tiên**:
   `Qdrant HTTP request failed ... /points/search` → API trả `502`.
4. Ở mức cao hơn nữa, sập lan sang tầng hạ tầng:
   - `pool timed out while waiting for an open connection` (`DATABASE_POOL_SIZE=16`)
   - `Temporary failure in name resolution` — **DNS nội bộ của Docker chết**
   - Ollama: `Load failed: timed out waiting for llama-server to start`

Lưu ý cho người đọc log: nút thắt CPU là **embed**, nhưng thành phần báo lỗi
đầu tiên là **Qdrant**. Đừng đi tối ưu Qdrant — nó chỉ là nạn nhân.

### Congestion collapse

Đo riêng tầng embed (bắn thẳng Ollama), thông lượng **giảm tuyệt đối** khi tăng tải:

| Đồng thời | Thông lượng |
|---|---|
| 5 | **1,30 req/s** ← đỉnh |
| 10 | 0,59 req/s |
| 20 | 0,19 req/s |
| 40 | 0,044 req/s |

Từ 5 lên 40 request đồng thời, tổng số câu trả lời hoàn thành mỗi giây **giảm 30
lần**. Máy dành CPU để chuyển ngữ cảnh thay vì tính toán.

Hệ quả: **giới hạn số request đồng thời có giá trị hơn là để chúng tự do.** Cùng
phần cứng này, chặn ở mức 5–10 cho thông lượng cao gấp hàng chục lần so với thả
40 request vào cùng lúc.

### Kết luận cho vận hành

- **40 nhân viên dùng hệ thống: được** — miễn là không quá ~10 người bấm gửi
  cùng lúc. Với chatbot nội bộ, mẫu sử dụng thực tế hiếm khi vượt mức này.
- **Rủi ro thật là burst**, không phải tổng số người: 8h sáng thứ Hai, hoặc ngay
  sau khi có thông báo toàn công ty.
- **Ingest giành CPU với người đang chat.** Worker tuần tự nhờ
  `INGESTION_JOB_BATCH_SIZE` default 1 (`jobs.rs:51`). Biến
  `GMRAG_INGESTION_DOCUMENT_CONCURRENCY` không được worker đọc và không có
  route nào `.acquire()` semaphore đó — không phải nút giảm tải.
  Ingest vẫn đủ để làm chậm chat. Nên **hẹn job sync tài liệu chạy ngoài giờ làm việc**.

### Việc nên làm, theo thứ tự giá trị/chi phí

1. **Nâng CPU** — sửa tận gốc; embed song song được và vách đứng lùi xa.
2. ~~Chặn số chat đồng thời~~ — **đã làm**, xem mục 8e.
3. **Hẹn giờ job sync tài liệu ngoài giờ hành chính.**
4. ~~`OLLAMA_KEEP_ALIVE=-1`~~ — **đã làm**, xem mục 8d.

---

## 8d. `OLLAMA_KEEP_ALIVE` — đã sửa

Mặc định của Ollama là `5m`: sau 5 phút không ai hỏi, model bị đẩy khỏi RAM.
Câu hỏi kế tiếp phải trả **~15,6 giây** nạp lại. Với nhân viên hỏi rải rác trong
ngày thì gần như câu nào cũng dính — ảnh hưởng hàng ngày lớn hơn kịch bản burst.

Đã đặt `OLLAMA_KEEP_ALIVE: "-1"` trong `docker-compose.prod.yml`. Xác nhận:

```
$ docker compose -p hrm-rag exec ollama ollama ps
NAME                                   SIZE     PROCESSOR   UNTIL
AITeamVN/Vietnamese_Embedding:latest   695 MB   100% CPU    Forever
```

Độ trễ embed sau khi sửa, máy rảnh: **0,42 – 0,54s** (trước đó câu đầu sau mỗi
lần idle là 15,6s).

Đánh đổi: 757 MB RAM bị giữ thường trú. Chấp nhận được — model dù sao cũng phải
nằm trong RAM mới phục vụ được.

---

## 8e. Giới hạn chat đồng thời — đã làm

Cài đặt trong [`gmrag_api/src/admission.rs`](../gmrag_api/src/admission.rs), ba tầng:

| Biến | Giá trị | Ý nghĩa |
|---|---|---|
| `GMRAG_CHAT_CONCURRENCY` | **5** | Số chat được xử lý cùng lúc |
| `GMRAG_CHAT_QUEUE_DEPTH` | 20 | Số chỗ xếp hàng chờ |
| `GMRAG_CHAT_QUEUE_WAIT_SECS` | 15 | Chờ quá lâu thì bỏ cuộc |

Quá cả ba tầng → `503 CHAT_BUSY` kèm `Retry-After`. Request bị từ chối **trước
mọi thao tác ghi**, nên không để lại session rác và retry là an toàn.

### Kết quả đo (2026-08-14)

| | Không giới hạn | conc=10 | **conc=5** |
|---|---|---|---|
| 20 user — trả lời hoàn chỉnh | 0/20 | 5/20 | **16/20** |
| 20 user — lỗi cứng 502 | 20 | 5 | **0** |
| 20 user — p50 (được phục vụ) | — | 44,4s | **12,1s** |
| 40 user — trả lời hoàn chỉnh | 0/40 | 20/40 | 19/40 |
| 40 user — lỗi cứng 502 | 30/30 (ở mốc 30) | 0 | **0** |
| 40 user — p50 (được phục vụ) | — | 15,1s | **8,5s** |

Hai điều rút ra:

1. **`concurrency=10` vẫn quá cao cho 1 vCPU.** Ở mốc 20 user nó chỉ phục vụ
   được 5 người và vẫn sinh 5 lỗi cứng — đúng cơ chế cũ: 10 request cùng chạy
   làm Qdrant đói CPU rồi timeout. Đặt cao hơn sức máy **không** phục vụ được
   nhiều hơn.
2. **Không còn lỗi cứng nào ở conc=5.** Người không được phục vụ nhận `503` kèm
   `Retry-After`, biết chính xác phải làm gì — thay vì `502` giữa chừng.

### Khi nâng CPU thì chỉnh lại

`concurrency` phải theo số lõi thật. Cách chỉnh: tăng dần rồi chạy lại
`scripts/server/loadtest-chat.sh --phase b --users 20`, chọn giá trị lớn nhất
mà vẫn giữ **0 lỗi cứng 502**.

```bash
sed -i 's/^GMRAG_CHAT_CONCURRENCY=.*/GMRAG_CHAT_CONCURRENCY=15/' .env.prod
docker compose -p hrm-rag -f docker-compose.prod.yml --env-file .env.prod up -d api
```

---

## 9. Gỡ và làm lại

```bash
cd /opt/hrm-rag
docker compose -p hrm-rag -f docker-compose.prod.yml --env-file .env.prod down
```

Thêm `-v` sẽ **không** xóa dữ liệu vì stack dùng bind mount, không dùng named
volume. Muốn reset sạch phải xóa `/opt/hrm-rag/.docker_data/` — thao tác này
xóa toàn bộ tài liệu đã upload, vector và tuple phân quyền.
