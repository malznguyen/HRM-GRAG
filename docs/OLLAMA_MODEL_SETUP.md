# Ollama Vietnamese embedding model setup

## Artifact cố định

Ứng dụng dùng model `AITeamVN/Vietnamese_Embedding`, vector đầu ra 1024 chiều.
Không dùng được lệnh sau:

```powershell
ollama pull AITeamVN/Vietnamese_Embedding
```

Registry Ollama không còn manifest của model này; lỗi đã ghi nhận là
`Error: pull model manifest: file does not exist`. Vì vậy model phải được tạo
từ GGUF Q8_0 lưu trên Hugging Face.

- Repository nguồn: `https://huggingface.co/doof-ferb/AITeamVN-Vietnamese_Embedding-gguf`
- URL tải trực tiếp: `https://huggingface.co/doof-ferb/AITeamVN-Vietnamese_Embedding-gguf/resolve/main/model-q8_0.gguf?download=true`
- File trong repo: `docker/ollama/model-q8_0.gguf`
- Kích thước chính xác: `634553568` byte (`634.554 MB`, `605.157 MiB`)
- SHA-256: `6672996fa3bf21ca11158b4e3429f5de6ce442ab50f37a8c889a357b399840ce`

File GGUF lớn và không được commit. Rule `docker/ollama/*.gguf` trong
`.gitignore` phải tiếp tục được giữ.

## Modelfile và tên model

Nội dung chính xác của `docker/ollama/Modelfile`:

```dockerfile
FROM ./model-q8_0.gguf
PARAMETER num_ctx 2048
```

Tên truyền cho `ollama create` phải là
`AITeamVN/Vietnamese_Embedding`, khớp với `OLLAMA_EMBED_MODEL` trong `.env`.

## Tải, kiểm tra và import

Chạy tại thư mục gốc của repository trong PowerShell:

```powershell
$sourceUrl = 'https://huggingface.co/doof-ferb/AITeamVN-Vietnamese_Embedding-gguf/resolve/main/model-q8_0.gguf?download=true'
$ggufPath = Join-Path $PWD 'docker\ollama\model-q8_0.gguf'

Invoke-WebRequest -Uri $sourceUrl -OutFile $ggufPath

$file = Get-Item -LiteralPath $ggufPath
$hash = Get-FileHash -Algorithm SHA256 -LiteralPath $ggufPath
$file.Length
$hash.Hash

git check-ignore -v -- docker/ollama/model-q8_0.gguf
git status --short
```

Chỉ tiếp tục khi kích thước là `634553568` byte, SHA-256 đúng giá trị ở trên,
và `git status` không liệt kê GGUF. Khởi động stack CPU-only rồi import:

```powershell
docker compose -p hrm-rag config --quiet
docker compose -p hrm-rag up -d
docker compose -p hrm-rag exec -T ollama ollama create "AITeamVN/Vietnamese_Embedding" -f /models/Modelfile
```

Mount `./docker/ollama:/models:ro` trong Compose làm cho cả GGUF và Modelfile
có mặt trong container tại `/models`.

## Verify

```powershell
docker compose -p hrm-rag exec -T ollama ollama list

$body = @{
    model = 'AITeamVN/Vietnamese_Embedding'
    input = 'Kiểm tra embedding tiếng Việt.'
} | ConvertTo-Json -Compress

$response = Invoke-RestMethod `
    -Method Post `
    -Uri 'http://127.0.0.1:11435/api/embed' `
    -ContentType 'application/json' `
    -Body $body

@($response.embeddings[0]).Count
```

Kết quả mong đợi:

- `ollama list` có `AITeamVN/Vietnamese_Embedding:latest`;
- lệnh đếm trả `1024`.

Trong lần recovery ngày 2026-08-11, một request sau khi model đã được nạp mất
`0.505` giây trên CPU; `ollama ps` báo `100% CPU` và context `2048`. Đây chỉ là
số đo tham khảo trên máy recovery, không phải SLA. Khi ước lượng ingest, cần đo
thêm với độ dài và batch giống dữ liệu thật.

## CPU-only và NVIDIA GPU

File `docker-compose.yml` mặc định không yêu cầu GPU, vì vậy lệnh thông thường
chạy CPU-only và hoạt động trên máy không có GPU:

```powershell
docker compose -p hrm-rag up -d
```

GPU reservation được giữ riêng trong `docker-compose.gpu.yml`. Trên server có
NVIDIA GPU và NVIDIA Container Toolkit hoạt động, ghép override này vào sau
file chính:

```powershell
docker compose -p hrm-rag -f docker-compose.yml -f docker-compose.gpu.yml config --quiet
docker compose -p hrm-rag -f docker-compose.yml -f docker-compose.gpu.yml up -d
```

Có thể xác nhận backend đang dùng bằng:

```powershell
docker compose -p hrm-rag exec -T ollama ollama ps
```

Nếu máy không có adapter NVIDIA mà dùng override GPU, Docker sẽ lỗi kiểu
`nvidia-container-cli: WSL environment detected but no adapters were found`.
Trong trường hợp đó, quay về đúng lệnh CPU-only mặc định; không sửa hoặc xóa
file override.

## Deploy lên máy không có internet

Chuẩn bị trên máy có internet:

1. Tải `model-q8_0.gguf` theo URL ở trên.
2. Xác nhận kích thước và SHA-256.
3. Mang repository cùng file GGUF qua USB hoặc share nội bộ. Không đưa GGUF
   vào Git.

Trên máy đích không có internet:

1. Chép file vào đúng `docker/ollama/model-q8_0.gguf`.
2. Chạy `Get-FileHash -Algorithm SHA256 docker/ollama/model-q8_0.gguf` và xác
   nhận hash `6672996fa3bf21ca11158b4e3429f5de6ce442ab50f37a8c889a357b399840ce`.
3. Đảm bảo các Docker image cần thiết đã tồn tại trên máy đích. Nếu chưa có,
   chuyển các image bằng `docker save`/`docker load` từ một máy tương thích.
4. Khởi động Compose bằng lệnh CPU-only hoặc GPU tương ứng ở mục trên.
5. Chạy lại `ollama create` từ `/models/Modelfile`:

   ```powershell
   docker compose -p hrm-rag exec -T ollama ollama create "AITeamVN/Vietnamese_Embedding" -f /models/Modelfile
   ```

6. Chạy cả hai bước verify `ollama list` và `/api/embed`; chỉ hoàn tất khi
   vector có đúng 1024 chiều.

Volume `./.docker_data/hrm_rag/ollama:/root/.ollama` giữ model đã import qua
các lần recreate container. Vẫn nên lưu riêng GGUF đã kiểm hash để có thể dựng
lại volume trên máy mới.
