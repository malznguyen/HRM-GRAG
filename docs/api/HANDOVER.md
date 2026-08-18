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

**Base URL: `http://192.168.168.89:18083`** (server `hrm-grag`, LAN nội bộ).

- Swagger UI: <http://192.168.168.89:18083/docs>
- OpenAPI YAML: <http://192.168.168.89:18083/openapi.yaml>
- Health: <http://192.168.168.89:18083/health>

Deployment dùng `API_BIND_ADDR=0.0.0.0:18083`; firewalld đã mở inbound TCP `18083`,
nên team HRM gọi được từ máy khác trong LAN. Toàn bộ dịch vụ nội bộ (Postgres,
OpenFGA, MinIO, Qdrant, Ollama) chỉ bind loopback và **không** mở ra mạng.

Đây chưa phải endpoint production: hiện dùng HTTP, chưa có TLS/reverse proxy.

> **Giới hạn tài nguyên hiện tại.** Server đang chạy với **1 vCPU / 1.8 GB RAM**
> trong khi stack cần khoảng 4–6 GB. Toàn bộ 11 bước smoke test đã pass, nhưng
> throughput rất thấp: embedding một chunk mất ~12 giây, load average ~2.5 trên
> 1 core. Dùng để tích hợp và kiểm thử thì được; **chưa chịu được tải nhiều người
> dùng đồng thời**. Việc nâng cấp đang được đề xuất.

## Đọc theo thứ tự này

1. Swagger UI ở `/docs` — mở trước để xem và thử API.
2. [`INTEGRATION_GUIDE.md`](./INTEGRATION_GUIDE.md) — đọc kỹ phần xử lý SSE và citation.
3. [`openapi.yaml`](./openapi.yaml) — spec nguồn để import vào công cụ khác khi cần.
4. [`examples/`](./examples/) — có file `.http` và `smoke.sh` để chạy thử ngay.

## Năm điều cần biết trước khi bắt đầu

1. Corpus đang **rỗng có chủ đích**. Phải upload tài liệu công ty đã được phê
   duyệt trước khi cho nhân viên sử dụng. Khi chưa có tài liệu, bot trả lời rằng
   không tìm thấy thông tin và không trả citation.
2. Upload yêu cầu exact permission `CHATBOT_UPLOAD_DOCUMENT`. Theo seed hiện tại,
   chỉ `HR` và `ADMIN` có permission này. `MANAGER` và `EMPLOYEE` mặc định nhận
   `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED` khi upload.
3. `MANAGER` và `EMPLOYEE` không được xóa. DELETE trả `404 RESOURCE_NOT_FOUND`
   thay vì `403` là có chủ đích để không tiết lộ tài liệu có tồn tại hay không.
4. Lịch sử chat đã có API list/read/rename/delete. Mỗi token chỉ thấy, đổi title và
   xóa session gắn với chính `userid` của token; kể cả `ADMIN`/`HR` cũng không đọc,
   sửa hoặc xóa thay user khác. Ba route GET dùng `limit`/`offset` (mặc định `20/0`,
   `limit` tối đa `100`) và trả page object có `sessions` hoặc `messages`, cùng
   `total`, `limit`, `offset`. History trả citation object gồm `chunk_id`,
   `document_id`, `document_name`, `snippet` và không cần gọi `/citations/resolve`;
   xóa session xóa luôn messages nhờ `ON DELETE CASCADE`.
5. Cảnh báo citation: `event: citations` là toàn bộ retrieved set (tối đa 5), không
   phải các nguồn đã được câu trả lời dùng; client phải lọc theo marker `[chunk:N]`
   sau khi ráp toàn bộ text SSE rồi mới hiển thị nguồn.
6. Từ Phase 17, có ba route để mở tài liệu từ citation: `GET .../file` (bytes PDF
   gốc, proxy qua API, không presigned URL), `GET .../preview` (toàn văn + chunk
   để cuộn/highlight) và `GET .../chunks/{chunk_id}` (toàn văn một chunk). Xem
   [mục 10 trong `INTEGRATION_GUIDE.md`](./INTEGRATION_GUIDE.md#10-xem-tài-liệu-gốc-và-chunk-trích-dẫn-phase-17).
7. **`POST .../chat` có thể trả `503 CHAT_BUSY` — client BẮT BUỘC xử lý.**
   Xem mục ngay dưới đây.

| Role | Đọc/Chat | Upload | Xóa |
|---|---|---|---|
| `ADMIN` | có | có | có |
| `HR` | có | có | có |
| `MANAGER` | có | không (`403`) | không (`404`) |
| `EMPLOYEE` | có | không (`403`) | không (`404`) |

MANAGER/EMPLOYEE mặc định nhận `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED` khi upload và
`404 RESOURCE_NOT_FOUND` khi DELETE. Không được hiểu response DELETE `404` là bằng chứng
tài liệu không tồn tại; hãy ẩn thao tác upload/xóa theo role ngay tại HRM.

## Bắt buộc xử lý: `503 CHAT_BUSY` khi server quá tải

Server giới hạn số câu hỏi được xử lý đồng thời. Vượt ngưỡng thì request được
**xếp hàng chờ**; nếu hàng đợi cũng đầy hoặc chờ quá lâu, API trả:

```
HTTP/1.1 503 Service Unavailable
Retry-After: 15
Content-Type: application/json

{"error":{"code":"CHAT_BUSY","message":"Server is at capacity. Retry after the number of seconds in the Retry-After header."}}
```

**Client phải làm gì:** đợi đúng số giây trong header `Retry-After` rồi gửi lại
**nguyên văn** request cũ — giữ nguyên `session_id`, không sinh mới. Request bị
từ chối **không** để lại dấu vết nào ở server: không tạo session, không ghi câu
hỏi vào lịch sử. Gửi lại là an toàn, không sợ trùng lặp.

Nếu retry vẫn 503 thì nên hiển thị cho nhân viên một thông báo kiểu "hệ thống
đang bận, vui lòng thử lại sau" thay vì báo lỗi kỹ thuật.

Phân biệt với `429 RATE_LIMITED`:

| | `429 RATE_LIMITED` | `503 CHAT_BUSY` |
|---|---|---|
| Nghĩa | **Bạn** gửi quá nhiều (>30 chat/60s cho một `userid`) | **Server** đang bận, bạn không làm gì sai |
| Cách xử lý | Giảm tần suất gửi của user đó | Thử lại sau `Retry-After` giây |

Ngưỡng hiện tại: **5 câu hỏi xử lý đồng thời**, thêm **20 chỗ xếp hàng**, chờ
tối đa **15 giây**. Các con số này sẽ được nới sau khi server được nâng cấu hình.

Thực đo với 20 người hỏi cùng lúc: **16/20 nhận được câu trả lời** (p50 12 giây),
4 nhận `503`. Với 40 người cùng lúc: 19 nhận câu trả lời, 21 nhận `503`. Không
có trường hợp nào lỗi cứng. Nếu client retry đúng theo `Retry-After` thì cuối
cùng mọi người đều được phục vụ.

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
