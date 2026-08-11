# Recovery runbook

Runbook này dựng lại môi trường `hrm_rag` trên máy Windows mới mà không cần dữ
liệu runtime của máy cũ. Chạy tại thư mục gốc repository. Dừng khi một bước lỗi;
không tiếp tục với hạ tầng ở trạng thái nửa vời.

## Những thứ không nằm trong Git

| Thành phần | Nguồn | Ghi chú |
| --- | --- | --- |
| `.env` | Xin từ người quản lý secret/deployment hoặc backup mã hóa | Chứa secret thật. Không gửi qua kênh công khai và tuyệt đối không commit. |
| `docker/ollama/model-q8_0.gguf` | Hugging Face hoặc bản copy đã kiểm hash | Không commit; `.gitignore` đã có `docker/ollama/*.gguf`. |
| `.docker_data/` | Do Docker Compose tạo tại máy đích | Không cần copy cho môi trường trắng. Muốn khôi phục dữ liệu thật phải dùng backup database/object store đúng quy trình riêng. |

Artifact GGUF chuẩn:

- URL: `https://huggingface.co/doof-ferb/AITeamVN-Vietnamese_Embedding-gguf/resolve/main/model-q8_0.gguf?download=true`
- Kích thước: `634553568` byte
- SHA-256: `6672996fa3bf21ca11158b4e3429f5de6ce442ab50f37a8c889a357b399840ce`

Xem hướng dẫn online/offline đầy đủ tại `docs/OLLAMA_MODEL_SETUP.md`.

## 1. Công cụ máy host

Yêu cầu Git, Docker Desktop, Rust theo `rust-toolchain.toml` và Visual Studio
Build Tools 2022 với workload C++/Windows SDK. Kiểm tra linker trong Developer
PowerShell for VS:

```powershell
Get-Command link.exe
rustc --version
cargo --version
docker version
```

Nếu chưa có Build Tools và có quyền Administrator:

```powershell
winget install --id Microsoft.VisualStudio.2022.BuildTools -e --accept-package-agreements --accept-source-agreements --override "--wait --quiet --norestart --add Microsoft.VisualStudio.Workload.VCTools --add Microsoft.VisualStudio.Component.Windows11SDK.22621 --includeRecommended"
```

Sau khi cài phải mở Developer PowerShell mới. Không thêm package npm/pip global.

## 2. Khôi phục cấu hình ngoài Git

1. Chép `.env` đã được cấp vào root repository.
2. Chép hoặc tải GGUF vào `docker/ollama/model-q8_0.gguf`.
3. Kiểm tra file lớn không xuất hiện trong Git:

```powershell
(Get-Item .\docker\ollama\model-q8_0.gguf).Length
(Get-FileHash -Algorithm SHA256 .\docker\ollama\model-q8_0.gguf).Hash
git check-ignore -v -- docker/ollama/model-q8_0.gguf
git status --short
```

Đảm bảo `API_BIND_ADDR=127.0.0.1:18083`; phase local không được bind API hoặc
port Compose ra ngoài loopback.

## 3. Build và preflight

Chạy trong Developer PowerShell:

```powershell
cargo build --workspace
Set-Location .\gmrag_api
cargo test --lib
Set-Location ..
```

Baseline hiện tại: `173 passed; 0 failed; 5 ignored`.

## 4. Dựng hạ tầng và Ollama

CPU-only là mặc định:

```powershell
docker compose -p hrm-rag config --quiet
docker compose -p hrm-rag up -d
docker compose -p hrm-rag ps -a
```

Trên server có NVIDIA GPU và NVIDIA Container Toolkit:

```powershell
docker compose -p hrm-rag -f docker-compose.yml -f docker-compose.gpu.yml config --quiet
docker compose -p hrm-rag -f docker-compose.yml -f docker-compose.gpu.yml up -d
```

`openfga-migrate` và `minio-init` phải exit `0`; các service dài hạn phải
running/healthy. Import model:

```powershell
docker compose -p hrm-rag exec -T ollama ollama create "AITeamVN/Vietnamese_Embedding" -f /models/Modelfile
docker compose -p hrm-rag exec -T ollama ollama list
```

Kiểm tra `/api/embed` trả 1024 chiều theo `docs/OLLAMA_MODEL_SETUP.md`.

## 5. Bootstrap OpenFGA trắng

Mỗi OpenFGA database mới sinh store/model ULID mới; không tái sử dụng ID của
máy cũ. `.env` phải có `OPENFGA_API_URL` và `OPENFGA_API_TOKEN` hợp lệ trước khi
chạy:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\bootstrap-openfga.ps1
```

Script dùng `openfga/cli:v0.7.15` qua Docker để transform
`gmrag_api/openfga/model.fga`, sau đó tạo store/upload model qua HTTP có bearer
header. Chép hai dòng `OPENFGA_STORE_ID` và `OPENFGA_MODEL_ID` mà script in ra
vào `.env`. Script cố ý báo lỗi rõ nếu `hrm-rag-dev` đã tồn tại, tránh tạo
duplicate ngoài ý muốn.

Chạy lại Compose để worker authz nhận ID mới:

```powershell
docker compose -p hrm-rag up -d process-authz-outbox
```

## 6. Seed tenant/workspace HRM

Giữ UUID trong `.env` nếu cần tương thích với hệ HRM và tài liệu bàn giao:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\seed-hrm-workspace.ps1
```

Script insert ID tường minh, tạo hai tuple cấu trúc và chạy lại an toàn. User
role không seed tĩnh: middleware HRM đồng bộ `member`/`admin` từ JWT đã ký.

## 7. Chạy API và verify

Trong Developer PowerShell riêng:

```powershell
cargo run --manifest-path .\gmrag_api\Cargo.toml --locked --bin gmrag_api
```

Ở terminal khác:

```powershell
curl.exe -i http://127.0.0.1:18083/health
curl.exe -i http://127.0.0.1:18083/ready
```

Cả hai phải 200 sau bootstrap OpenFGA. Tạo token HS512 test trong bộ nhớ từ
`JWT_HMAC_SECRET`; không ghi token ra file hoặc report. Token cần issuer cấu
hình, claim `userid`, `role=HR`, và permissions `CHATBOT_USE`,
`CHATBOT_UPLOAD_DOCUMENT`.

Chạy smoke bằng Git Bash, không dùng WSL:

```bash
export RAG_BASE_URL='http://127.0.0.1:18083'
export RAG_TOKEN='<token chỉ giữ tạm trong terminal>'
export RAG_WORKSPACE_ID='hrm'
./docs/api/examples/smoke.sh
```

Kỳ vọng 5/5 PASS. Sau kiểm tra phải xóa document và chat session test; xác nhận
SQL không còn document/session, Qdrant không còn point và MinIO không còn object
của test.

Delete document tạo event `storage_outbox`. Topology Compose hiện không chạy
`process-storage-outbox` tự động (OPS-003); operator xử lý hàng đợi theo lệnh
binary đã có trong repo khi cần, sau khi kiểm tra target và cấu hình storage:

```powershell
cargo run --manifest-path .\gmrag_api\Cargo.toml --locked --bin process-storage-outbox
```

## Khi phải xin người khác

- Xin `.env`/secret từ người quản lý deployment; không tự đặt secret production.
- Nếu máy đích không có internet, xin GGUF đã kiểm SHA-256 và các Docker image
  được xuất bằng `docker save`.
- Xin quyền Administrator để cài MSVC Build Tools nếu máy chưa có; không lách UAC.
- Xin backup database/object store chính thức nếu mục tiêu là phục hồi dữ liệu
  thật. `.docker_data/` trắng chỉ dựng môi trường, không tái tạo dữ liệu nghiệp vụ.
- Xin API key DeepSeek hợp lệ nếu smoke chat không gọi được upstream.

Luôn backup `.env` và GGUF ở nơi an toàn ngoài máy làm việc sau khi recovery.
