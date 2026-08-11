# Bàn giao tích hợp HRM RAG API

## Mở cái này trước

`http://<IP-máy-chạy-RAG>:18083/docs` — **mở cái này trước** để xem API và thử request
ngay trên Swagger UI. Bấm **Authorize**, dán access token HRM (chỉ token, không tự thêm
tiền tố `Bearer `), rồi dùng **Try it out**. Workspace mẫu đã điền sẵn alias `hrm`.

Swagger UI và spec đều do chính API phục vụ, không tải CDN hay asset Internet:

- UI: `http://<IP-máy-chạy-RAG>:18083/docs`
- OpenAPI YAML: `http://<IP-máy-chạy-RAG>:18083/openapi.yaml`

Swagger UI phù hợp để thử auth, upload, list, status, chat history và delete. Riêng chat
SSE, UI có thể gửi request nhưng không hiển thị đúng luồng/event theo thời gian thực; dùng ví dụ trong
[`INTEGRATION_GUIDE.md`](./INTEGRATION_GUIDE.md#6-chat--phần-quan-trọng-nhất) để test chat.

## Địa chỉ API

- Ngay trên máy chạy RAG: `http://127.0.0.1:18083`
- Từ máy khác trong LAN: `http://<RAG_HOST>:18083`

Deployment bàn giao hiện dùng `API_BIND_ADDR=0.0.0.0:18083`; firewall đã mở inbound
TCP port `18083`, nên team HRM có thể mở Swagger và gọi API từ máy khác trong LAN.
Đây chưa phải endpoint production: URL hiện dùng HTTP và chưa có TLS/reverse proxy;
khi có server chính thức, team RAG sẽ cung cấp base URL cố định.

## Đọc theo thứ tự này

1. Swagger UI ở `/docs` — mở trước để xem và thử API.
2. [`INTEGRATION_GUIDE.md`](./INTEGRATION_GUIDE.md) — đọc kỹ phần xử lý SSE và citation.
3. [`openapi.yaml`](./openapi.yaml) — spec nguồn để import vào công cụ khác khi cần.
4. [`examples/`](./examples/) — có file `.http` và `smoke.sh` để chạy thử ngay.

## Bốn điều cần biết trước khi bắt đầu

1. Corpus đang **rỗng có chủ đích**. Phải upload tài liệu công ty đã được phê
   duyệt trước khi cho nhân viên sử dụng. Khi chưa có tài liệu, bot trả lời rằng
   không tìm thấy thông tin và không trả citation.
2. Upload yêu cầu exact permission `CHATBOT_UPLOAD_DOCUMENT`. Theo seed hiện tại,
   chỉ `HR` và `ADMIN` có permission này. `MANAGER` và `EMPLOYEE` mặc định nhận
   `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED` khi upload.
3. `MANAGER` và `EMPLOYEE` không được xóa. DELETE trả `404 RESOURCE_NOT_FOUND`
   thay vì `403` là có chủ đích để không tiết lộ tài liệu có tồn tại hay không.
4. Lịch sử chat đã có API list/read/delete. Mỗi token chỉ thấy và xóa session gắn
   với chính `userid` của token; kể cả `ADMIN`/`HR` cũng không đọc/xóa thay user khác.
   Ba route GET dùng `limit`/`offset` (mặc định `20/0`, `limit` tối đa `100`) và trả
   page object có `sessions` hoặc `messages`, cùng `total`, `limit`, `offset`. Xóa
   session xóa luôn messages nhờ `ON DELETE CASCADE`.

| Role | Đọc/Chat | Upload | Xóa |
|---|---|---|---|
| `ADMIN` | có | có | có |
| `HR` | có | có | có |
| `MANAGER` | có | không (`403`) | không (`404`) |
| `EMPLOYEE` | có | không (`403`) | không (`404`) |

MANAGER/EMPLOYEE mặc định nhận `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED` khi upload và
`404 RESOURCE_NOT_FOUND` khi DELETE. Không được hiểu response DELETE `404` là bằng chứng
tài liệu không tồn tại; hãy ẩn thao tác upload/xóa theo role ngay tại HRM.

## Test nhanh nhất

```bash
curl http://<RAG_HOST>:18083/health
```

Kết quả mong đợi:

```json
{"status":"ok","db":"connected"}
```

## Một câu cần team HRM trả lời

1. Branch HRM nào đang deploy? Snapshot source đã đọc còn hardcode issuer
   `restaurant-access`, trong khi token production đã kiểm chứng dùng
   `hrm-gm-group-access`.

## Giới hạn hiện tại

- Chưa có idempotency cho upload.
- PDF scan chưa OCR được.
- Chưa có prefix `/v1`.
- Retrieval tối đa 5 đoạn/câu hỏi, chưa có reranker hoặc score threshold cứng.
- Lịch sử chat giữ vĩnh viễn tới khi owner tự xóa; không có admin-delete. Nhân viên
  đã nghỉ không còn token tự xóa nên cần quy trình dọn dẹp riêng ở phase sau.
- Endpoint tạm dùng HTTP, chưa có TLS.

Chi tiết xem [mục 8 trong `INTEGRATION_GUIDE.md`](./INTEGRATION_GUIDE.md#8-giới-hạn-hiện-tại).
