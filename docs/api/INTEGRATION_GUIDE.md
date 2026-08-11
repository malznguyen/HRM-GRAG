# Hướng dẫn tích hợp RAG API cho backend HRM

Tài liệu này dành cho dev backend HRM (Java/Spring). Bạn **không cần** biết gì về
codebase RAG, không cần đọc Rust.

Mọi thứ trong tài liệu được đọc trực tiếp từ source. Chỗ nào chưa xác nhận được
đều ghi rõ `TODO`, không suy đoán.

Spec máy đọc được: [`openapi.yaml`](./openapi.yaml) (OpenAPI 3.1).
Ví dụ chạy được: [`examples/`](./examples/).

---

## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Xác thực](#2-xác-thực)
3. [Upload tài liệu](#3-upload-tài-liệu)
4. [Trạng thái tài liệu](#4-trạng-thái-tài-liệu)
5. [Xóa tài liệu](#5-xóa-tài-liệu)
6. [Chat — phần quan trọng nhất](#6-chat--phần-quan-trọng-nhất)
7. [Lỗi](#7-lỗi)
8. [Giới hạn hiện tại](#8-giới-hạn-hiện-tại)
9. [Checklist tích hợp](#9-checklist-tích-hợp)

---

## 1. Tổng quan

### 1.1 Service này làm gì

Nhận tài liệu nội bộ (nội quy, quy trình, chính sách...), cắt nhỏ, tạo embedding,
lưu vào vector store. Khi có câu hỏi, nó tìm các đoạn liên quan, đưa vào prompt
của LLM, rồi **stream** câu trả lời kèm trích dẫn nguồn.

### 1.2 Service này KHÔNG làm gì

- **Không phát hành token.** Không có endpoint login/register. Nó chỉ verify
  token do HRM cấp.
- **Không quản lý user.** User row được tạo tự động từ claim trong token.
- **Không có OCR.** PDF scan (ảnh) sẽ fail với `NEEDS_OCR`.
- **Không lưu file để tải về.** HRM vẫn phải giữ bản gốc của mình.
- **Không đảm bảo câu trả lời đúng.** Đây là LLM. Luôn hiển thị citation để
  người dùng tự kiểm chứng.
- **Không xử lý đồng bộ.** Upload xong không có nghĩa là đã tìm kiếm được.

### 1.3 Sơ đồ

```
┌───────────────┐
│  HRM frontend │
└───────┬───────┘
        │  (token của HRM)
        ▼
┌────────────────────┐        Authorization: Bearer <HRM access token>
│   HRM backend      │ ─────────────────────────────────────────────┐
│   (Java/Spring)    │                                              │
└────────────────────┘                                              │
                                                                    ▼
                                                    ┌───────────────────────────┐
                                                    │       RAG API             │
                                                    │  (Rust / Axum, HTTP)      │
                                                    │                           │
                                                    │  • verify JWT của HRM     │
                                                    │  • ánh xạ role → quyền    │
                                                    │  • upload / status / xóa  │
                                                    │  • chat SSE               │
                                                    └─────┬───────┬───────┬─────┘
                                                          │       │       │
                                     ┌────────────────────┘       │       └──────────────┐
                                     ▼                            ▼                      ▼
                            ┌────────────────┐          ┌──────────────────┐   ┌──────────────────┐
                            │  PostgreSQL    │          │     Qdrant       │   │  LLM + Embedding │
                            │  metadata,     │          │  vector search   │   │  (DeepSeek,      │
                            │  chunk, phiên  │          │                  │   │   Ollama)        │
                            └────────────────┘          └──────────────────┘   └──────────────────┘
                                     │
                                     ▼
                            ┌────────────────┐          ┌──────────────────┐
                            │  MinIO / S3    │          │    OpenFGA       │
                            │  file gốc      │          │  phân quyền      │
                            └────────────────┘          └──────────────────┘

              ┌──────────────────────────────────────────┐
              │  ingestion-worker (process riêng)        │
              │  chạy nền: parse → embed → index         │
              │  ⇒ lý do upload là BẤT ĐỒNG BỘ           │
              └──────────────────────────────────────────┘
```

Điểm cần nhớ từ sơ đồ: **`ingestion-worker` là một process riêng**. API chỉ ghi
một job vào hàng đợi rồi trả về ngay. Đó là lý do bắt buộc phải poll trạng thái.

### 1.4 Base URL và cấu hình

Địa chỉ bind lấy từ biến môi trường `API_BIND_ADDR`.

| Nguồn | Giá trị |
|---|---|
| Default trong code (`main.rs`) | `127.0.0.1:8083` |
| Giá trị trong `gmrag_api/.env.example` | `127.0.0.1:18083` |
| Deployment bàn giao LAN hiện tại | `0.0.0.0:18083` |
| Production chính thức cho HRM | **TODO: chưa quyết định** |

Deployment bàn giao hiện bind `0.0.0.0:18083`; firewall máy chủ đã mở inbound TCP
port `18083`, nên máy khác trong LAN có thể gọi qua
`http://<RAG_HOST>:18083`. Hai giá trị loopback trong bảng là default của source và
file cấu hình mẫu, **không phải** trạng thái process đang bàn giao.

Endpoint LAN hiện vẫn dùng HTTP, chưa có reverse proxy/TLS và chưa phải endpoint
production chính thức. Trước khi lên production, hai bên vẫn phải chốt base URL,
HTTPS và network được phép truy cập.

**Không có version prefix.** Đường dẫn là `/health`, không phải `/v1/health`.

Toàn bộ endpoint HRM cần:

| # | Việc | Method | Path |
|---|---|---|---|
| 1 | Upload tài liệu | `POST` | `/workspaces/hrm/documents/upload` |
| 2 | Xem trạng thái | `GET` | `/workspaces/hrm/documents/{document_id}` |
| 3 | Xóa tài liệu | `DELETE` | `/workspaces/hrm/documents/{document_id}` |
| 4 | Chat (SSE) | `POST` | `/workspaces/hrm/chat` |
| 5 | Health check | `GET` | `/health` |
| — | *(optional)* Liệt kê tài liệu để đối soát | `GET` | `/workspaces/hrm/documents` |
| 6 | Liệt kê session chat của user hiện tại | `GET` | `/workspaces/hrm/chat/sessions` |
| 7 | Đọc messages của một session | `GET` | `/workspaces/hrm/chat/sessions/{session_id}/messages` |
| 8 | Đọc messages qua route tương thích | `GET` | `/workspaces/hrm/chat/history?session_id={session_id}` |
| 9 | Xóa session và toàn bộ messages | `DELETE` | `/workspaces/hrm/chat/sessions/{session_id}` |

### 1.5 Thử nhanh bằng Swagger UI

Mở `http://<RAG_HOST>:18083/docs`. Swagger UI và `/openapi.yaml` được phục vụ từ cùng
RAG API, dùng URL same-origin và toàn bộ asset đã đóng gói trong binary; trình duyệt
không cần Internet và không cần mở rộng CORS.

1. Bấm **Authorize**.
2. Dán access token HRM vào ô bearer (chỉ token, không gõ thêm `Bearer `).
3. Chọn operation, bấm **Try it out** rồi **Execute**. Các path workspace mẫu dùng alias
   `hrm`, không cần tìm UUID.

Swagger UI phù hợp để thử auth, list, upload, status và delete. Với `POST
/workspaces/hrm/chat`, UI có thể kết nối và gửi bearer token nhưng thường buffer response;
nó không phải công cụ quan sát đúng từng SSE event theo thời gian thực. Để test chat, dùng
`examples/hrm-rag.http`, `examples/smoke.sh`, `curl -N` hoặc Spring `WebClient`, rồi làm
đúng [mục 6.3](#63-response-là-sse-không-phải-json) đến
[mục 6.5](#65-thứ-tự-event).

Đặc biệt, không regex marker `[chunk:N]` trên từng mảnh Swagger/curl vừa hiển thị. Phải
nối toàn bộ text event không tên, đợi `event: done`, rồi mới parse marker như
[mục 6.4](#64-️-cảnh-báo-quan-trọng-nhất-marker-chunkn-bị-cắt-vụn).

---

## 2. Xác thực

### 2.1 Cách gửi

Mọi request (trừ `/health`) phải có header:

```
Authorization: Bearer <ACCESS_TOKEN>
```

Đây là **access token của chính HRM**, không phải token do RAG service cấp.
RAG service không có endpoint đăng nhập.

Lưu ý cách server đọc header: nó chỉ chấp nhận đúng tiền tố `Bearer ` (chữ B
hoa, một dấu cách). Phần token sau đó không được rỗng. Sai định dạng → `401
UNAUTHORIZED`.

### 2.2 Claim bắt buộc

| Claim | Kiểu | Bắt buộc | Ý nghĩa |
|---|---|---|---|
| `userid` | string | **Có** | ID người dùng chuẩn. Rỗng/thiếu/không phải string → `401`. |
| `role` | string | **Có** | Quyết định quyền admin hay member. |
| `permissions` | array of string | Có, nếu dùng chat/upload | Chat cần `CHATBOT_USE`; upload cần `CHATBOT_UPLOAD_DOCUMENT`. |
| `email` | string | Không | Dùng để tạo user row. |
| `exp` | number | Có | Hết hạn → `401`. Cho phép lệch đồng hồ 60 giây. |
| `iss` | string | Có | Phải khớp `JWT_ISSUER` cấu hình phía RAG. |
| `aud` | string | Không | Chỉ verify khi `JWT_VERIFY_AUDIENCE=true`. Deployment HRM đặt `false`, nên token không có `aud` vẫn pass. |

> **Tên claim chủ thể là `userid`, không phải `sub`.** Server đọc claim theo tên
> cấu hình trong `JWT_SUBJECT_CLAIM`, và cho HRM giá trị đó là `userid`.
> Giá trị này trở thành ID người dùng duy nhất trong cả database lẫn hệ thống
> phân quyền. Nó phải **ổn định vĩnh viễn** cho một nhân viên — nếu HRM đổi
> `userid` của một người, phía RAG sẽ coi đó là người hoàn toàn khác và người đó
> mất sạch lịch sử chat.

Ví dụ payload token hợp lệ:

```json
{
  "userid": "employee-1",
  "role": "HR",
  "permissions": ["CHATBOT_USE", "CHATBOT_UPLOAD_DOCUMENT"],
  "email": "nhanvien@congty.vn",
  "iss": "<issuer HRM đã cấu hình>",
  "exp": 1786000000
}
```

### 2.3 Role → quyền

| Role | Đọc/Chat | Upload | Xóa |
|---|---|---|---|
| `ADMIN` | có | có | có |
| `HR` | có | có | có |
| `MANAGER` | có | không (`403`) | không (`404`) |
| `EMPLOYEE` | có | không (`403`) | không (`404`) |

Phía OpenFGA map `ADMIN`/`HR` → `admin` và `MANAGER`/`EMPLOYEE` → `member`.
Với seed HRM mặc định, MANAGER và EMPLOYEE upload nhận
`403 CHATBOT_UPLOAD_PERMISSION_REQUIRED`. Nếu được cấp riêng permission upload, họ vẫn là
workspace `member` và nhận `403 WORKSPACE_ADMIN_REQUIRED` ở ACL kế tiếp.

DELETE trả `404 RESOURCE_NOT_FOUND` thay vì `403` là có chủ đích, để không tiết lộ sự tồn tại
của tài liệu với người không có quyền. Vì vậy HRM phải chặn nút xóa theo role, không suy luận
"tài liệu không tồn tại" chỉ từ response `404`.

| `role` không hợp lệ | Kết quả |
|---|---|
| Bất kỳ giá trị nào ngoài 4 role trên | `403 HRM_ROLE_REQUIRED` |
| Thiếu / rỗng / không phải string | `403 HRM_ROLE_REQUIRED` |

So khớp **phân biệt hoa thường và chính xác tuyệt đối**. `"Admin"`, `"admin"`,
`"HR "` (có dấu cách thừa) đều **fail**. Không có role nào được suy diễn hay
gộp nhóm.

Quyền được đồng bộ **ở mỗi request**: server đọc `role` trong token hiện tại rồi
cập nhật quan hệ phân quyền cho khớp. Nghĩa là khi HRM đổi role của một nhân
viên, **token mới đầu tiên** sẽ tự động áp dụng role mới — không cần gọi API
đồng bộ nào cả. Không cần thao tác gì thêm phía HRM.

### 2.4 Quyền chat

Endpoint chat còn kiểm tra thêm: mảng `permissions` phải chứa **đúng chuỗi**
`CHATBOT_USE`. Thiếu → `403 CHATBOT_PERMISSION_REQUIRED`.

Kiểm tra này chạy **rất sớm**, trước cả khi server đọc body request. Hệ quả cần
biết khi debug: nếu bạn gửi body JSON sai *và* token thiếu `CHATBOT_USE`, bạn sẽ
nhận `403`, không phải `400`. Đừng tưởng body của mình đúng.

Upload kiểm tra thêm mảng `permissions` phải chứa **đúng chuỗi**
`CHATBOT_UPLOAD_DOCUMENT`. Thiếu → `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED`.
Gate này cũng chạy trước khi đọc multipart body, nhưng **không thay thế** OpenFGA:
caller có permission mà chỉ là workspace `member` vẫn nhận
`403 WORKSPACE_ADMIN_REQUIRED`. Gate không áp dụng cho `DELETE`.

### 2.5 Caller KHÔNG gửi tenant / workspace

**HRM không được truyền `tenant_id` hay `workspace_id` như dữ liệu do client
chọn.** Server tự chốt phạm vi: nó so `{workspace_id}` trong URL với giá trị
`HRM_WORKSPACE_ID` cấu hình sẵn, khác một ký tự là `400 HRM_SCOPE_MISMATCH`.

Path vẫn còn `{workspace_id}`, nhưng HRM **không cần biết UUID**: viết đúng chuỗi
`hrm` vào vị trí đó, server tự resolve về `HRM_WORKSPACE_ID`.

```
POST   /workspaces/hrm/documents/upload
GET    /workspaces/hrm/documents/{document_id}
DELETE /workspaces/hrm/documents/{document_id}
POST   /workspaces/hrm/chat
GET    /workspaces/hrm/documents
```

```java
// application.yml
// rag.base-url: http://<RAG_HOST>:<RAG_PORT>
// Không cần rag.workspace-id nữa.
private static final String WORKSPACE_PATH = "/workspaces/hrm/documents/upload";
```

Alias hoạt động trên **mọi** route có `{workspace_id}` trong tài liệu này — upload,
trạng thái, xóa, danh sách, chat và mọi route chat con. Không có route nào là ngoại lệ.

Ba điều cần nhớ:

| Điều | Chi tiết |
|---|---|
| **Phân biệt hoa thường** | Chỉ đúng chuỗi thường `hrm`. `HRM`, `Hrm` **không** phải alias → `400 INVALID_REQUEST` |
| **UUID vẫn dùng được** | Alias là bổ sung, không thay thế. Điền UUID đầy đủ vẫn chạy y hệt |
| **Chỉ khi HRM mode bật** | `HRM_MODE=false` thì `hrm` lại chỉ là chuỗi thường và không parse được thành UUID → `400 INVALID_REQUEST` |

Alias resolve **trước khi routing**, nên phần còn lại của hệ thống chỉ nhìn thấy
UUID: kiểm tra scope, phân quyền, log và metrics đều ghi UUID thật, không ghi `hrm`.

> Vẫn giữ nguyên nguyên tắc: **không** lấy workspace từ input người dùng. Alias
> `hrm` là hằng số trong code HRM, không phải tham số cấu hình cho user chọn.

Nếu vẫn muốn dùng UUID: workspace của HRM là
`fa76881f-6367-4b80-a89e-a3e01206a806`, tenant `a47ab6d6-bf77-4c8c-a22d-a4f1997eb18d`.
Gửi UUID **khác** hai giá trị này → `400 HRM_SCOPE_MISMATCH`.

> **TODO — đề xuất cho phase sau:** nên bỏ hẳn `{workspace_id}` khỏi path. Alias
> đã giấu được UUID, nhưng path gọn hơn vẫn nên là `POST /documents/upload`,
> `POST /chat`. Việc này đổi contract nên để phase sau.

### 2.6 Lỗi xác thực

| HTTP | `code` | Khi nào | HRM nên làm gì |
|---|---|---|---|
| 401 | `UNAUTHORIZED` | Thiếu header, sai tiền tố `Bearer `, token rỗng | Sửa code gửi header |
| 401 | `INVALID_TOKEN` | Token hết hạn, sai chữ ký, sai issuer, sai audience, sai thuật toán, thiếu claim `userid` | Lấy token mới rồi thử lại **một** lần. Vẫn lỗi → sai cấu hình, phải báo người |
| 400 | `HRM_SCOPE_MISMATCH` | `{workspace_id}` trong URL không khớp cấu hình | Bug phía HRM: sai config workspace |
| 403 | `HRM_ROLE_REQUIRED` | `role` không thuộc 4 giá trị hợp lệ | Kiểm tra HRM có nhét `role` vào token chưa |
| 403 | `CHATBOT_PERMISSION_REQUIRED` | Thiếu `CHATBOT_USE` (chỉ endpoint chat) | Cấp quyền cho user, hoặc ẩn tính năng chat |
| 403 | `CHATBOT_UPLOAD_PERMISSION_REQUIRED` | Thiếu exact permission `CHATBOT_UPLOAD_DOCUMENT` (chỉ endpoint upload) | Cấp quyền upload, hoặc ẩn chức năng upload |
| 500 | `INTERNAL_ERROR` | Không tạo được user row | Lỗi server, retry với backoff |
| 500 | `AUTHZ_ERROR` | Dịch vụ phân quyền chết/timeout | Lỗi server, retry với backoff |

> **Quan trọng:** mọi lý do token bị từ chối đều trả về cùng một
> `401 INVALID_TOKEN`. Server **cố ý** không nói cụ thể sai ở đâu. Lý do thật
> (hết hạn / sai thuật toán / sai chữ ký...) chỉ ghi trong log của server RAG.
> Khi debug phải xin log phía RAG, đừng đoán từ response.

---

## 3. Upload tài liệu

```
POST /workspaces/hrm/documents/upload
```

Yêu cầu đồng thời:

1. Token có exact permission `CHATBOT_UPLOAD_DOCUMENT`. Thiếu permission →
   `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED` trước khi server đọc multipart body.
2. Caller có relation OpenFGA `admin` trên workspace. Có permission nhưng chỉ là
   `member` → `403 WORKSPACE_ADMIN_REQUIRED`.

Seed HRM mặc định chỉ cấp permission upload cho `ADMIN` và `HR`; `MANAGER` và
`EMPLOYEE` nhận `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED`. Kể cả khi được cấp riêng
permission này, hai role vẫn là workspace `member` và bị ACL từ chối bằng
`403 WORKSPACE_ADMIN_REQUIRED`. Gate không áp dụng cho DELETE.

### 3.1 Định dạng request

`Content-Type: multipart/form-data`.

| Field | Bắt buộc | Ghi chú |
|---|---|---|
| `file` | Có | Nội dung file. **Lặp lại được** để upload nhiều file trong một request |
| `access_mode` | Không | `workspace_default` (mặc định) hoặc `restricted`. **HRM nên bỏ trống** |

Field tên khác `file` / `access_mode` bị bỏ qua im lặng.

`access_mode=restricted` sẽ ẩn tài liệu khỏi *tất cả* mọi người cho đến khi được
cấp quyền xem riêng — mà endpoint cấp quyền đó nằm ngoài phạm vi tích hợp này.
Đặt `restricted` = tài liệu vô hình với mọi user. **Đừng dùng.**

### 3.2 Giới hạn kích thước

| Giới hạn | Giá trị | Nguồn |
|---|---|---|
| Toàn bộ request body | 50 MiB (`52428800` bytes) | `DOCUMENT_MAX_UPLOAD_BYTES`, mặc định trong code |

Đây là giới hạn cho **cả request**, không phải từng file. Upload 3 file mỗi file
20 MB trong một request sẽ vượt ngưỡng. Vượt → `413 PAYLOAD_TOO_LARGE`.

Khuyến nghị: **mỗi request một file** — vì giới hạn body tính cho cả request, một
file hỏng không làm hỏng lô, và tiến độ upload dễ hiển thị hơn. Lỗi từng file thì
gửi lô cũng biết được: xem `rejected` ở mục 3.4.

### 3.3 Định dạng file được hỗ trợ

Server **không tin `Content-Type` bạn khai báo**, cũng không tin đuôi file (trừ
txt/md). Nó đọc bytes để tự nhận dạng:

| Loại | Cách nhận dạng | Ghi chú |
|---|---|---|
| PDF | Bắt đầu bằng `%PDF-` **và** parse được cấu trúc | PDF hỏng → bị loại ngay lúc upload |
| DOCX | Là file ZIP **và** chứa cả `[Content_Types].xml` lẫn `word/document.xml` | |
| TXT | UTF-8 hợp lệ, không rỗng, không chứa byte NUL, **và** đuôi `.txt` | Không phân biệt hoa thường |
| Markdown | Như TXT nhưng đuôi `.md` | |

**Không** hỗ trợ: `.doc` (Word cũ), `.xlsx`, `.pptx`, `.csv`, `.html`, `.rtf`,
`.odt`, ảnh, ZIP thường. Đổi tên file thành `.txt` không cứu được — file phải
thực sự là UTF-8.

> **PDF scan (ảnh chụp / bản scan) sẽ được nhận nhưng ingest THẤT BẠI.**
> Nó qua được bước nhận dạng (vẫn là PDF hợp lệ), trả `202` bình thường, rồi
> vài giây sau chuyển sang `status=FAILED`, `failure_code=NEEDS_OCR`. Hiện chưa
> có OCR provider nào được đấu nối, nên đây là lỗi **vĩnh viễn** — upload lại
> đúng file đó sẽ fail y hệt. HRM cần nói rõ với người dùng: PDF phải có text
> thật, không phải ảnh.

### 3.4 Xử lý bất đồng bộ và lỗi cục bộ

**`202 Accepted` KHÔNG có nghĩa là tài liệu đã sẵn sàng.** Nó chỉ có nghĩa: row
đã tạo, job đã vào hàng đợi. Lúc này `status=PROCESSING`,
`processing_stage=QUEUED`, `chunk_count=0`. Chat **chưa** tìm thấy tài liệu này.

HRM bắt buộc phải poll trạng thái (mục 4).

**Thành công một phần được báo cáo tường minh.** Response `202` có hai mảng:

| Field | Nội dung |
|---|---|
| `documents` | Các file đã nhận, mỗi phần tử có `document_id` + `filename` |
| `rejected` | Các file **bị loại**, mỗi phần tử có `filename`, `reason_code`, `message` |

`rejected` **luôn có mặt** — mảng rỗng khi không file nào bị loại. Đừng suy ra lỗi
bằng cách so `documents.size()` với số file đã gửi; hãy đọc thẳng `rejected`.

Bảng `reason_code`:

| `reason_code` | Nghĩa | Lỗi của ai | Retry cùng bytes có ích? |
|---|---|---|---|
| `UNSUPPORTED_MEDIA_TYPE` | Nội dung không phải PDF/DOCX/TXT/MD hợp lệ (xem 3.3) | Caller | Không |
| `PAYLOAD_TOO_LARGE` | File vượt `DOCUMENT_MAX_UPLOAD_BYTES` | Caller | Không |
| `FILE_READ_FAILED` | Không đọc hết được phần multipart (client ngắt, body hỏng) | Caller | Có |
| `STORAGE_WRITE_FAILED` | Ghi object storage thất bại | Server | Có |
| `PERSIST_FAILED` | Ghi bản ghi tài liệu / job ingestion vào DB thất bại | Server | Có |
| `AUTHZ_SYNC_FAILED` | Đồng bộ quyền tài liệu sang OpenFGA thất bại | Server | Có |

Bốn mã cuối là sự cố phía server: bản thân file có thể vẫn tốt, upload lại là hợp lý.
Với hai mã đầu, gửi lại đúng bytes đó sẽ ra đúng kết quả đó.

> Trong thực tế `PAYLOAD_TOO_LARGE` ở mức từng file hiếm khi xuất hiện: giới hạn
> body áp cho **cả request** (mục 3.2) nên request thường bị chặn bằng `413`
> trước khi server kịp soi từng file.

`message` là chuỗi tiếng Anh cố định, dùng để ghi log. **Phân nhánh theo
`reason_code`, đừng parse `message`.**

Nếu **mọi** file đều bị loại → `400 INVALID_REQUEST` với message
`Request did not contain an acceptable file`. Danh sách lý do vẫn còn, nằm ở
`error.details.rejected` với đúng schema như trên — `202` chỉ dùng cho trường hợp
có ít nhất một file thực sự được nhận để xử lý.

### 3.5 Ví dụ curl

```bash
curl -i -X POST \
  "http://<RAG_HOST>:<RAG_PORT>/workspaces/hrm/documents/upload" \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -F "file=@noi-quy-cong-ty.pdf;type=application/pdf"
```

Response `202`:

```json
{
  "documents": [
    {
      "document_id": "69f56ad1-f379-4705-9eb4-f58cbd269420",
      "filename": "noi-quy-cong-ty.pdf"
    }
  ],
  "rejected": []
}
```

Gửi 3 file, 1 nhận được và 2 bị loại — vẫn là `202`:

```json
{
  "documents": [
    {
      "document_id": "69f56ad1-f379-4705-9eb4-f58cbd269420",
      "filename": "quy-dinh.md"
    }
  ],
  "rejected": [
    {
      "filename": "bang-luong.xlsx",
      "reason_code": "UNSUPPORTED_MEDIA_TYPE",
      "message": "File content is not a supported document format (PDF, DOCX, TXT, MD)"
    },
    {
      "filename": "anh.bin",
      "reason_code": "UNSUPPORTED_MEDIA_TYPE",
      "message": "File content is not a supported document format (PDF, DOCX, TXT, MD)"
    }
  ]
}
```

Gửi 2 file và **cả hai** đều bị loại — `400`, lý do nằm trong `error.details`:

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "Request did not contain an acceptable file",
    "details": {
      "rejected": [
        {
          "filename": "bang-luong.xlsx",
          "reason_code": "UNSUPPORTED_MEDIA_TYPE",
          "message": "File content is not a supported document format (PDF, DOCX, TXT, MD)"
        }
      ]
    }
  }
}
```

> **`filename` trả về có thể khác cái bạn gửi.** Server làm sạch tên: bỏ đường
> dẫn, bỏ ký tự `/`, `\`, NUL, cắt còn tối đa 255 ký tự. Nếu không lấy được tên
> hợp lệ, nó dùng `upload.pdf` — **kể cả khi file không phải PDF**. Muốn hiển
> thị đúng tên gốc thì HRM tự lưu tên của mình, đừng phụ thuộc field này.

> **PHẢI LƯU `document_id`.** Xem mục 5.

---

## 4. Trạng thái tài liệu

```
GET /workspaces/hrm/documents/{document_id}
```

Yêu cầu role member trở lên — `MANAGER` và `EMPLOYEE` đều gọi được.

### 4.1 Response schema

```json
{
  "document_id": "69f56ad1-f379-4705-9eb4-f58cbd269420",
  "filename": "noi-quy-cong-ty-smoke.md",
  "status": "COMPLETED",
  "processing_stage": "DONE",
  "failure_code": null,
  "failure_message": null,
  "access_mode": "workspace_default",
  "created_at": "2026-08-06T03:22:57.106555",
  "updated_at": "2026-08-06T03:23:18.856347",
  "chunk_count": 1
}
```

*(nguyên văn từ `docs/PHASE3_RESULT.md`)*

| Field | Kiểu | Ghi chú |
|---|---|---|
| `document_id` | UUID | |
| `filename` | string | Tên đã được làm sạch |
| `status` | enum | Xem 4.2 |
| `processing_stage` | enum | Xem 4.3 |
| `failure_code` | string hoặc `null` | Luôn có mặt trong JSON. Xem 4.4 |
| `failure_message` | string hoặc `null` | Tiếng Anh, dành cho log. **Đừng hiện cho người dùng cuối, đừng branch theo nó** |
| `access_mode` | enum | `workspace_default` \| `restricted` |
| `created_at` | timestamp | Lúc upload |
| `updated_at` | timestamp | Xem cảnh báo dưới |
| `chunk_count` | integer | Số đoạn đã tạo. `0` khi đang xử lý và khi thất bại |

> **Cảnh báo định dạng thời gian.** `created_at` / `updated_at` **không có `Z`,
> không có offset múi giờ**: `2026-08-06T03:22:57.106555`. Chúng là giờ UTC theo
> quy ước, nhưng parser RFC 3339 nghiêm ngặt sẽ **ném exception**.
>
> ```java
> // ĐÚNG
> LocalDateTime.parse(value).atOffset(ZoneOffset.UTC);
> // SAI — DateTimeParseException
> OffsetDateTime.parse(value);
> ```
>
> Với Jackson, đừng map thẳng vào `Instant` hay `OffsetDateTime`; dùng
> `LocalDateTime` rồi tự gắn UTC.

> `updated_at` **không phải** thời điểm sửa tài liệu. Nó là thời điểm cập nhật
> gần nhất của *job ingest*, và fallback về `created_at` nếu chưa có job nào.
> Dùng nó để đo tiến độ xử lý, đừng dùng để phát hiện nội dung thay đổi.

### 4.2 Bảng giá trị `status`

| `status` | Nghĩa | HRM nên làm gì |
|---|---|---|
| `PROCESSING` | Đang chờ hoặc đang xử lý. Chưa tìm kiếm được | Tiếp tục poll |
| `COMPLETED` | Đã index xong, chat tìm thấy được | Dừng poll. Đánh dấu sẵn sàng |
| `FAILED` | Thất bại, đã hết số lần retry tự động | Dừng poll. Đọc `failure_code`, xem 4.4 |

Chỉ có đúng 3 giá trị. Không có `PENDING`, `CANCELLED`, `DELETED`.

> `COMPLETED` với `chunk_count = 0` là trường hợp đặc biệt: xử lý xong nhưng
> không rút được nội dung nào (ví dụ file rỗng về mặt nội dung). Tài liệu này
> **sẽ không bao giờ được trích dẫn**. HRM nên coi đây là bất thường và cảnh báo
> cho người upload.

### 4.3 Bảng giá trị `processing_stage`

Đây là thông tin **hiển thị tiến độ**. Logic nghiệp vụ phải branch theo `status`,
không phải theo stage.

| `processing_stage` | Nghĩa | Đi kèm `status` |
|---|---|---|
| `QUEUED` | Đang chờ worker rảnh | `PROCESSING` |
| `PARSING` | Đang rút text từ PDF/DOCX/TXT/MD | `PROCESSING` |
| `EMBEDDING` | Đang tạo vector cho từng đoạn | `PROCESSING` |
| `GRAPH_EXTRACTION` | Đang dựng knowledge graph | `PROCESSING` |
| `SAVING` | Đang ghi đoạn + vector vào database | `PROCESSING` |
| `INDEXING` | Đang đẩy vector lên Qdrant | `PROCESSING` |
| `DONE` | Xong toàn bộ | `COMPLETED` |
| `FAILED` | Dừng giữa chừng | `FAILED` |

> `GRAPH_EXTRACTION` bị **tắt mặc định** (`GMRAG_GRAPH_EXTRACTION_ENABLED=false`).
> Ở cấu hình hiện tại bạn sẽ không thấy stage này. Cứ xử lý enum đầy đủ để phòng
> khi nó được bật.

Thứ tự stage **không đảm bảo tăng dần đơn điệu**: khi một job retry, tài liệu
quay lại `QUEUED`. Đừng viết code kiểu "stage chỉ có thể tiến, không thể lùi".

Cũng đừng dùng `switch` không có nhánh `default` trên enum này — nếu phase sau
thêm stage mới, code HRM sẽ ném exception. Luôn có nhánh fallback.

### 4.4 Bảng giá trị `failure_code`

Chỉ có giá trị khi `status = FAILED`. Cột "Tự retry" cho biết hệ thống RAG **đã**
tự thử lại (tối đa 5 lần, backoff tăng dần) trước khi báo `FAILED`.

| `failure_code` | Nghĩa | Tự retry rồi? | HRM nên làm gì |
|---|---|---|---|
| `NEEDS_OCR` | PDF scan/ảnh, không có text | Không | **Lỗi vĩnh viễn.** Báo người dùng: cần file có text thật. Upload lại vô ích |
| `PDF_PARSE_FAILED` | PDF hỏng hoặc parse quá lâu | Không | Báo người dùng file hỏng. Đề nghị xuất lại PDF |
| `DOCX_PARSE_FAILED` | DOCX hỏng | Không | Như trên |
| `TEXT_DECODE_FAILED` | File text không phải UTF-8 hợp lệ | Không | Yêu cầu lưu lại dưới dạng UTF-8 |
| `DOCUMENT_OBJECT_MISSING` | File đã lưu bị mất khỏi object storage | Có | Lỗi hệ thống. Báo team RAG. Upload lại |
| `EMBEDDING_PROVIDER_UNAVAILABLE` | Dịch vụ embedding chết | Có | Sự cố hạ tầng. Upload lại sau, hoặc báo team RAG |
| `GRAPH_EXTRACTION_FAILED` | Dựng graph thất bại | Có | Như trên |
| `QDRANT_INDEX_FAILED` | Đẩy vector thất bại | Có | Như trên |
| `DATABASE_SAVE_FAILED` | Ghi database thất bại | Có | Như trên |
| `INTERNAL_INGESTION_ERROR` | Lỗi không phân loại được | Có | Báo team RAG kèm `document_id` |
| `INGESTION_JOB_MISSING` | Job biến mất khi tài liệu đang `PROCESSING` | — | Lỗi hệ thống. Xóa rồi upload lại |

Cách chia đơn giản cho HRM:

- **4 code đầu** = lỗi của file. Người dùng phải sửa file. Upload lại y hệt sẽ
  fail lại.
- **7 code còn lại** = lỗi hệ thống. Upload lại có thể thành công.

> **Không có endpoint retry trong phạm vi tích hợp này.** Service *có* một route
> retry cho admin nhưng nó nằm ngoài integration surface bàn giao nên không đưa vào spec. Với
> HRM, cách khôi phục một tài liệu `FAILED` là: xóa (mục 5) rồi upload lại.

### 4.5 Chu kỳ poll khuyến nghị

Không có webhook, không có callback. HRM phải poll.

Không có rate limit trên endpoint này (rate limit chỉ áp cho chat và upload),
nhưng vẫn nên poll có chừng mực:

| Thời điểm sau upload | Chu kỳ |
|---|---|
| 0–30 giây | mỗi 2 giây |
| 30 giây – 5 phút | mỗi 5 giây |
| 5–15 phút | mỗi 30 giây |
| Sau 15 phút | Dừng poll, coi như treo, báo người |

Cơ sở của mốc 15 phút: một job có tối đa 5 lần thử, lease mỗi lần 300 giây, backoff
tối đa 300 giây. Trường hợp xấu nhất trên lý thuyết vượt 15 phút, nhưng một tài
liệu bình thường vẫn `PROCESSING` sau 15 phút gần như chắc chắn là có sự cố.

> **TODO — cần xác nhận:** chưa có số đo thời gian ingest thực tế theo kích
> thước file. Bảng trên là suy ra từ tham số cấu hình, không phải đo đạc. Nên
> chỉnh lại sau khi chạy thử với corpus thật của HRM.

### 4.6 Ví dụ curl

```bash
curl -s \
  "http://<RAG_HOST>:<RAG_PORT>/workspaces/hrm/documents/<DOCUMENT_ID>" \
  -H "Authorization: Bearer <ACCESS_TOKEN>"
```

Đang xử lý:

```json
{"document_id":"69f56ad1-f379-4705-9eb4-f58cbd269420","filename":"noi-quy-cong-ty.pdf","status":"PROCESSING","processing_stage":"PARSING","failure_code":null,"failure_message":null,"access_mode":"workspace_default","created_at":"2026-08-06T03:22:57.106555","updated_at":"2026-08-06T03:23:01.004112","chunk_count":0}
```

Thất bại vì PDF scan:

```json
{"document_id":"69f56ad1-f379-4705-9eb4-f58cbd269420","filename":"hop-dong-scan.pdf","status":"FAILED","processing_stage":"FAILED","failure_code":"NEEDS_OCR","failure_message":"Document requires OCR and no OCR provider is available","access_mode":"workspace_default","created_at":"2026-08-06T03:22:57.106555","updated_at":"2026-08-06T03:23:05.221904","chunk_count":0}
```

Không tìm thấy (nguyên văn từ `PHASE3_RESULT.md`):

```json
{"error":{"code":"RESOURCE_NOT_FOUND","message":"Resource not found"}}
```

### 4.7 Vì sao mọi thứ đều là 404

Endpoint này **cố ý** trả `404` cho cả ba trường hợp:

- `document_id` không tồn tại
- Tài liệu thuộc workspace khác
- Tài liệu tồn tại nhưng caller không có quyền xem

Đây là chống dò tài liệu: không cho phép phân biệt "không có" với "có nhưng cấm".
HRM không thể và không nên cố tách ba trường hợp này.

Chỉ khi **hạ tầng phân quyền** hỏng thì mới ra `500 AUTHZ_ERROR` — đó mới là lỗi
đáng retry.

---

## 5. Xóa tài liệu

```
DELETE /workspaces/hrm/documents/{document_id}
```

Yêu cầu role admin. Thành công → **`204 No Content`, body rỗng**.

### 5.1 Xóa thật hay xóa mềm?

**Xóa thật.** Không phải soft delete, không có thùng rác, không khôi phục được.

Xóa đồng bộ, trong một transaction:

| Xóa khỏi | Thời điểm |
|---|---|
| Quan hệ phân quyền (OpenFGA) | Trước khi commit SQL |
| Row `documents` trong PostgreSQL | Trong transaction |
| Các đoạn (chunks) | Cascade theo document |
| Dữ liệu graph provenance | Trong transaction |

Xóa **sau khi** commit, theo cơ chế best-effort + outbox bền vững:

| Xóa khỏi | Cơ chế |
|---|---|
| Vector trong Qdrant | Thử ngay; thất bại thì worker nền dọn sau |
| File gốc trong MinIO/S3 | Thử ngay; thất bại thì worker nền dọn sau |

Nghĩa là `204` đảm bảo: tài liệu **đã** biến mất khỏi database và khỏi phân
quyền, và việc dọn vector/file **đã được lên lịch chắc chắn**. Nó không đảm bảo
vector đã bị xóa xong ngay tại thời điểm response.

Trên thực tế điều này an toàn: chat lọc kết quả theo tài liệu còn tồn tại trong
database, nên tài liệu vừa xóa không thể bị trích dẫn nữa, kể cả khi vector còn
sót lại vài giây.

### 5.2 HRM PHẢI GIỮ `document_id`

> **Đây là điều dễ bị bỏ sót nhất trong toàn bộ tích hợp.**
>
> `document_id` do RAG service sinh ra lúc upload là **cách duy nhất** để xóa
> hoặc tra trạng thái một tài liệu.
>
> - Không có xóa theo tên file.
> - Không có xóa theo ID của HRM (không có khái niệm external ID).
> - Không có xóa theo checksum.
>
> HRM **phải lưu `document_id` vào database của mình** ngay khi nhận response
> `202`, gắn với record tài liệu tương ứng bên HRM. Mất `document_id` là mất
> khả năng quản lý tài liệu đó qua API.

Schema gợi ý phía HRM:

```sql
ALTER TABLE hrm_documents
  ADD COLUMN rag_document_id UUID NULL,
  ADD COLUMN rag_status      VARCHAR(16) NULL,   -- PROCESSING | COMPLETED | FAILED
  ADD COLUMN rag_synced_at   TIMESTAMP NULL;
```

Nếu lỡ mất: dùng endpoint list (mục 1.4, optional) để đối chiếu theo `filename`
và `created_at`. Đây là cách chữa cháy, không phải giải pháp — `filename` không
duy nhất.

### 5.3 Ví dụ curl

```bash
curl -i -X DELETE \
  "http://<RAG_HOST>:<RAG_PORT>/workspaces/hrm/documents/<DOCUMENT_ID>" \
  -H "Authorization: Bearer <ACCESS_TOKEN>"
```

```
HTTP/1.1 204 No Content
```

### 5.4 Lỗi

| HTTP | `code` | Nghĩa | HRM nên làm gì |
|---|---|---|---|
| 404 | `RESOURCE_NOT_FOUND` | Không tồn tại, thuộc workspace khác, **hoặc** caller không phải admin | **Coi như đã xóa xong.** Xóa hai lần là idempotent |
| 500 | `AUTHZ_REVOKE_FAILED` | Không thu hồi được phân quyền; **chưa xóa gì cả** | An toàn để retry |
| 500 | `INTERNAL_ERROR` | Transaction lỗi, đã rollback | An toàn để retry |
| 500 | `AUTHZ_ERROR` | Hạ tầng phân quyền hỏng | An toàn để retry |

`DELETE` áp dụng **đúng quy tắc che giấu như `GET`** (mục 4.7): không tồn tại,
thuộc workspace khác, và không đủ quyền đều ra `404 RESOURCE_NOT_FOUND` giống hệt
nhau. Trước Phase 5 endpoint này trả `403 WORKSPACE_ADMIN_REQUIRED` khi thiếu
quyền — nghĩa là chỉ cần đổi `GET` thành `DELETE` trên cùng một URL là dò ra được
tài liệu có tồn tại hay không, đúng thứ `GET` cố tình giấu. Nay không còn.

> **Hệ quả cho HRM:** token `MANAGER` hoặc `EMPLOYEE` bấm xóa sẽ nhận
> `404 RESOURCE_NOT_FOUND`, không phải `403`.
> Không suy ra được "file không tồn tại" từ đó. Hãy chặn nút xóa ở phía HRM theo
> role trong token thay vì dựa vào mã lỗi của RAG.

Chỉ `5xx` mới là lỗi đáng retry.

---

## 6. Chat — phần quan trọng nhất

```
POST /workspaces/hrm/chat
```

Yêu cầu role member trở lên **và** `permissions` chứa `CHATBOT_USE`.

Đây là endpoint dễ code sai nhất trong toàn bộ API. Đọc hết mục này trước khi
viết dòng code đầu tiên.

### 6.1 Request

`Content-Type: application/json`

```json
{
  "session_id": "df093a48-ed78-4109-ac96-a75be34ab35c",
  "message": "Giờ làm việc của công ty là mấy giờ?"
}
```

| Field | Kiểu | Bắt buộc |
|---|---|---|
| `session_id` | UUID | Có |
| `message` | string | Có, không được rỗng sau khi trim |

### 6.2 Session — HRM tự sinh ID

> **Không có endpoint tạo session.** `session_id` do **client tự sinh**.

Cách hoạt động:

- Bắt đầu hội thoại mới → HRM sinh một UUID v4 mới. Server thấy chưa tồn tại thì
  tự tạo session, lấy đoạn đầu câu hỏi làm tiêu đề.
- Hỏi tiếp trong cùng hội thoại → gửi lại **đúng** `session_id` đó. Server nạp
  tối đa **5 message gần nhất** của session vào prompt. Bốn route ở mục 6.13 vẫn
  trả toàn bộ session/messages vì hiện chưa có phân trang.

**Đó là toàn bộ cơ chế multi-turn.** HRM không cần và không nên tự gửi lại lịch
sử hội thoại — server đã lưu và tự nạp. Chỉ cần gửi câu hỏi mới nhất.

Session gắn với người dùng: nếu gửi một `session_id` đang thuộc về **user khác**,
kết quả là `403`. Vậy nên UUID v4 ngẫu nhiên là bắt buộc — đừng sinh session id
theo kiểu đoán được.

`event: done` trả về chính `session_id` đó. Xem 6.6.

### 6.3 Response là SSE, không phải JSON

```
Content-Type: text/event-stream
```

Không được dùng `RestTemplate` / `ObjectMapper.readValue()` cho endpoint này.
Với Spring, dùng `WebClient` và đọc từng dòng, hoặc dùng thư viện SSE.

Các loại event:

| Event | Có tên? | Data |
|---|---|---|
| Mảnh text câu trả lời | **Không** (mặc định là `message`) | Text thô, **không phải JSON** |
| `citations` | Có | JSON object |
| `done` | Có | UUID dạng text thuần, **không phải JSON**, không có dấu nháy |
| `error` | Có | Text thuần (chỉ khi lỗi giữa chừng) |

> Event text **không có dòng `event:`**. Theo chuẩn SSE, event không tên có type
> mặc định là `message`. Consumer phải phân biệt bằng field `event` — event nào
> không có tên thì là text câu trả lời.

Ngoài ra, cứ 15 giây im lặng server gửi một dòng comment `:keep-alive`. Thư viện
SSE chuẩn tự bỏ qua dòng comment. Nếu bạn tự parse thì phải bỏ qua mọi dòng bắt
đầu bằng `:`.

### 6.4 ⚠️ CẢNH BÁO QUAN TRỌNG NHẤT: marker `[chunk:N]` bị cắt vụn

# 🚨 **MARKER `[chunk:N]` BỊ CẮT RỜI QUA NHIỀU EVENT.**

# **HRM PHẢI GOM TOÀN BỘ TEXT LẠI RỒI MỚI PARSE MARKER.**

# **TUYỆT ĐỐI KHÔNG PARSE THEO TỪNG EVENT.**

Đây là **lỗi dễ mắc nhất** khi tích hợp endpoint này.

Text đến từ LLM theo từng token. Một marker `[chunk:1]` **không** nằm gọn trong
một event. Trong stream thật đã bắt được, `[chunk:1]` đến dưới dạng **6 event
riêng biệt**:

```
data: [          ← event 1
data: ch         ← event 2
data: unk        ← event 3
data: :          ← event 4
data: 1          ← event 5
data: ]          ← event 6
```

Nếu bạn chạy regex `\[chunk:(\d+)\]` trên từng event, bạn sẽ **không bao giờ
match được gì**, và người dùng sẽ nhìn thấy chuỗi `[chunk:1]` thô nằm giữa câu
trả lời.

Cách chia còn tùy tokenizer của model và **có thể khác ở mỗi lần chạy**. Đừng
bao giờ giả định độ dài event, đừng buffer "vài event" rồi parse. Chỉ có một
cách đúng:

```
✅ ĐÚNG:  gom tất cả data của event không tên  →  đợi event `done`  →  parse marker
❌ SAI:   parse marker trên từng event khi nó đến
❌ SAI:   parse marker trên buffer từng phần khi đang stream
```

Nếu HRM muốn hiển thị text dần dần (typing effect) thì vẫn được — cứ hiển thị
text thô ngay, nhưng **chỉ thay marker thành link sau khi stream kết thúc**.
Hoặc đơn giản hơn: che marker khi đang stream bằng cách chỉ render tới ký tự `[`
cuối cùng chưa đóng.

### 6.5 Thứ tự event

Thứ tự này được đảm bảo:

```
(0..n event text, không tên)
   ↓
[tùy chọn: event: error  — chỉ khi luồng LLM đứt giữa chừng]
   ↓
event: citations       ← LUÔN CÓ, đúng một lần
   ↓
event: done            ← LUÔN CÓ, đúng một lần, cuối cùng
```

- `citations` **luôn được gửi**, kể cả khi không tìm thấy đoạn nào — khi đó là
  mảng rỗng `{"citations":[]}`. Đừng viết code kiểu "không có citations thì
  không có event citations".
- `done` luôn là event cuối. Nhận được `done` là biết stream đã xong bình thường.
- Nếu kết nối đứt mà chưa thấy `done` → coi là lỗi, không phải kết thúc.

### 6.6 `event: done` trả về gì

```
event: done
data: df093a48-ed78-4109-ac96-a75be34ab35c
```

Data là **`session_id`** dạng text thuần — không phải JSON, không có dấu nháy.
Nó chính là giá trị HRM đã gửi trong request.

HRM dùng nó để:

1. **Biết stream đã kết thúc bình thường** (đây là công dụng chính).
2. Xác nhận session server đang dùng đúng session mình gửi.
3. Lưu lại để gửi cho lượt hỏi tiếp theo trong cùng hội thoại.

Nó **không** chứa thống kê token, thời gian xử lý hay thông tin gì khác.

### 6.7 `event: citations`

```json
{
  "citations": [
    {
      "index": 1,
      "chunk_id": "3f601309-1f9e-4f88-9f16-1077bb849460",
      "document_id": "69f56ad1-f379-4705-9eb4-f58cbd269420",
      "document_name": "noi-quy-cong-ty-smoke.md",
      "snippet": "# NỘI QUY CÔNG TY HRM — ..."
    }
  ]
}
```

| Field | Ghi chú |
|---|---|
| `index` | Số `N` trong `[chunk:N]`. **Đếm từ 1.** Đây là khóa để map marker sang tài liệu |
| `chunk_id` | ID đoạn văn bản |
| `document_id` | ID tài liệu — **khớp với `document_id` HRM đã lưu lúc upload** |
| `document_name` | Tên file nguồn |
| `snippet` | Nội dung đoạn, cắt còn tối đa 280 ký tự tại ranh giới từ, thêm `…` nếu bị cắt |

> **`index` đếm từ 1, và KHÔNG phải vị trí trong mảng `citations`.**
> Nó là vị trí của đoạn trong kết quả tìm kiếm. Mảng `citations` chỉ chứa những
> đoạn caller **được phép xem** — đoạn bị lọc vì phân quyền sẽ bị bỏ khỏi mảng
> nhưng `index` của các đoạn còn lại **không** được đánh số lại.
>
> Ví dụ: mảng có thể là `[{index: 2}, {index: 4}]`. Phải tra cứu theo giá trị
> `index`, tuyệt đối không dùng `citations.get(i)`.

Hệ quả: câu trả lời có thể chứa `[chunk:3]` mà trong `citations` **không có**
`index = 3`. Xảy ra khi đoạn đó bị lọc vì phân quyền, hoặc khi LLM bịa ra số
không tồn tại. **HRM phải xử lý được marker không map được** — cách an toàn là
xóa marker đó khỏi text hiển thị.

> `snippet` chứa xuống dòng và ký tự markdown **nguyên bản từ tài liệu nguồn**,
> không được escape HTML. Nếu HRM render ra web thì **phải tự escape**, nếu
> không sẽ dính XSS từ nội dung tài liệu.

### 6.8 Stream mẫu nguyên văn

Dưới đây là **nguyên văn** một stream thật, chép từ `docs/PHASE3_RESULT.md`
(câu hỏi về giờ làm việc, câu trả lời `08:00-17:00[chunk:1]`):

```text
data: 08

data: :

data: 00

data: -

data: 17

data: :

data: 00

data: [

data: ch

data: unk

data: :

data: 1

data: ]

event: citations
data: {"citations":[{"index":1,"chunk_id":"3f601309-1f9e-4f88-9f16-1077bb849460","document_id":"69f56ad1-f379-4705-9eb4-f58cbd269420","document_name":"noi-quy-cong-ty-smoke.md","snippet":"# NỘI QUY CÔNG TY HRM — TÀI LIỆU KIỂM THỬ\n\n## Chương I. Quy định chung\n\n### Điều 1. Phạm vi áp dụng\n1. Nội quy này áp dụng cho toàn bộ nhân viên đang làm việc tại công ty HRM.\n2. Nhân viên có trách nhiệm đọc, hiểu và tuân thủ các quy định trong tài liệu.\n\n### Điều 2. Nguyên tắc…"}]}

event: done
data: df093a48-ed78-4109-ac96-a75be34ab35c
```

Nhìn kỹ: câu trả lời cuối cùng chỉ là `08:00-17:00[chunk:1]` — vậy mà mất **13
event** để gửi, trong đó riêng marker chiếm 6 event. Ngay cả `08:00` cũng bị cắt
thành `08` / `:` / `00`.

Stream khi không tìm thấy đoạn nào:

```text
data: Tôi không tìm thấy thông tin này trong tài liệu.

event: citations
data: {"citations":[]}

event: done
data: df093a48-ed78-4109-ac96-a75be34ab35c
```

### 6.9 Pseudocode xử lý phía Java

```java
// ---- Trạng thái tích luỹ trong suốt stream ----
StringBuilder answerBuffer = new StringBuilder();   // gom TẤT CẢ text
List<Citation> citations   = new ArrayList<>();
String         sessionId   = null;
boolean        completed   = false;
String         streamError = null;

// ---- Đọc stream ----
for (ServerSentEvent event : stream) {

    String name = event.event();   // null hoặc "message" = event text không tên

    if (name == null || name.equals("message")) {
        // ⚠️ CHỈ GOM. TUYỆT ĐỐI KHÔNG PARSE MARKER Ở ĐÂY.
        answerBuffer.append(event.data());

        // Muốn hiệu ứng gõ chữ thì render text thô ở đây cũng được,
        // nhưng KHÔNG được thay marker ở bước này.

    } else if (name.equals("citations")) {
        citations = objectMapper
            .readValue(event.data(), CitationsEnvelope.class)
            .citations();

    } else if (name.equals("done")) {
        sessionId = event.data().trim();   // UUID thuần, KHÔNG phải JSON
        completed = true;

    } else if (name.equals("error")) {
        streamError = event.data();        // câu trả lời sẽ không đầy đủ
    }
}

if (!completed) {
    throw new RagStreamException("Stream ended without a done event");
}

// ---- Chỉ tới ĐÂY mới được parse marker ----
String finalAnswer = renderCitations(answerBuffer.toString(), citations);


// ---- Thay marker bằng link ----
private static final Pattern CHUNK_MARKER = Pattern.compile("\\[chunk:(\\d+)\\]");

String renderCitations(String text, List<Citation> citations) {
    // Tra cứu theo GIÁ TRỊ index, không phải vị trí trong mảng
    Map<Integer, Citation> byIndex = citations.stream()
        .collect(Collectors.toMap(Citation::index, c -> c, (a, b) -> a));

    Matcher  m   = CHUNK_MARKER.matcher(text);
    StringBuilder out = new StringBuilder();

    while (m.find()) {
        int       n = Integer.parseInt(m.group(1));
        Citation  c = byIndex.get(n);

        String replacement = (c == null)
            // Marker không map được (bị lọc quyền, hoặc LLM bịa số).
            // Xóa hẳn — đừng để lộ "[chunk:7]" cho người dùng.
            ? ""
            : renderLink(c);   // NHỚ escape HTML cho document_name và snippet

        m.appendReplacement(out, Matcher.quoteReplacement(replacement));
    }
    m.appendTail(out);
    return out.toString();
}
```

Bốn điểm phải làm đúng, tóm tắt lại:

1. Gom hết text rồi mới regex — không parse từng event.
2. Map theo **giá trị** `index`, không theo vị trí trong mảng.
3. Xử lý được marker không map được (xóa đi).
4. Escape HTML cho `document_name` và `snippet` trước khi render.

### 6.10 Lỗi giữa chừng

Khi luồng LLM đứt sau khi stream đã bắt đầu:

```text
data: Theo nội quy công ty,

event: error
data: Generation service unavailable

event: citations
data: {"citations":[]}

event: done
data: df093a48-ed78-4109-ac96-a75be34ab35c
```

> **HTTP status vẫn là `200`.** Header đã gửi đi rồi nên không thể đổi status
> nữa. Đây là lý do **không được** chỉ dựa vào HTTP status để biết chat thành
> công. Phải kiểm tra: có nhận được `event: done` không, và có gặp
> `event: error` không.

Sau `error`, stream vẫn kết thúc đúng thủ tục với `citations` và `done`. Câu trả
lời tích luỹ được là một phần dở dang — HRM nên hiển thị nó kèm cảnh báo, hoặc
bỏ đi và mời người dùng hỏi lại.

Server vẫn lưu phần trả lời dở dang vào lịch sử session. Điều này cũng đúng khi
**client tự ngắt kết nối** giữa chừng.

### 6.11 Ví dụ curl

```bash
curl -N -X POST \
  "http://<RAG_HOST>:<RAG_PORT>/workspaces/hrm/chat" \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
        "session_id": "df093a48-ed78-4109-ac96-a75be34ab35c",
        "message": "Giờ làm việc của công ty là mấy giờ?"
      }'
```

> Bắt buộc có `-N` (`--no-buffer`), nếu không curl sẽ đệm và bạn không thấy
> stream chảy theo thời gian thực.

### 6.12 Lỗi của endpoint chat

| HTTP | `code` | Nghĩa | HRM nên làm gì |
|---|---|---|---|
| 400 | `INVALID_REQUEST` | `message` rỗng, hoặc JSON sai cú pháp | Bug phía HRM. Validate trước khi gửi |
| 403 | `CHATBOT_PERMISSION_REQUIRED` | Thiếu `CHATBOT_USE` | Ẩn tính năng chat với user này |
| 403 | `FORBIDDEN` | Không phải member, **hoặc** `session_id` thuộc user khác | Kiểm tra logic sinh session id |
| 415 | `UNSUPPORTED_MEDIA_TYPE` | Thiếu `Content-Type: application/json` | Bug phía HRM |
| 422 | `UNPROCESSABLE_ENTITY` | JSON đúng cú pháp nhưng sai schema (thiếu field, `session_id` không phải UUID) | Bug phía HRM |
| 429 | `RATE_LIMITED` | Quá 30 request/phút cho một user | Backoff rồi thử lại. Nên chặn sẵn ở UI |
| 500 | `INTERNAL_ERROR` | Lỗi server | Retry với backoff |
| 502 | `GENERATION_SERVICE_UNAVAILABLE` | LLM / embedding / vector store chết **trước khi** stream mở | Retry với backoff. Báo người dùng "hệ thống bận" |

> Phân biệt hai kiểu lỗi generation:
> - Chết **trước** khi stream mở → `502` JSON bình thường.
> - Chết **sau** khi stream mở → HTTP `200` + `event: error`.
>
> Code HRM phải xử lý cả hai đường.

### 6.13 Liệt kê, đọc và xóa lịch sử chat

Server lưu session và từng message trong PostgreSQL, gắn với canonical user ID lấy
từ claim `userid`. Không có TTL hay job xóa tự động theo tuổi; dữ liệu tồn tại cho
tới khi user xóa session, user/workspace/tenant bị xóa, hoặc vận hành dọn dữ liệu.

#### Phân quyền — điểm phải hiểu đúng

Cả bốn route dưới đây yêu cầu bearer token hợp lệ và relation `member` trên
workspace, rồi khóa dữ liệu bằng **đúng `userid` của token**:

- User chỉ liệt kê, đọc và xóa session của chính mình.
- `ADMIN`/`HR` **không có quyền vượt cấp** để đọc hoặc xóa session của user khác;
  role `admin` vẫn bị kiểm tra owner như mọi role khác.
- Bốn handler này không kiểm tra permission `CHATBOT_USE`. Một workspace member đã
  mất permission chat vẫn có thể đọc hoặc xóa dữ liệu cũ của chính mình.
- Không có query param `user_id`. Muốn hiển thị lịch sử cho nhân viên nào, HRM gọi
  bằng access token của chính nhân viên đó; không dùng một token service/admin để
  đọc thay user khác.

Không phát hiện lỗ hổng đọc/xóa chéo user trong code hiện tại: owner được kiểm tra
trước và câu SQL đọc/xóa cũng ràng buộc đồng thời `workspace_id` + `user_id`.

#### 6.13.1 Liệt kê session của user hiện tại

```http
GET /workspaces/hrm/chat/sessions
Authorization: Bearer <ACCESS_TOKEN_CỦA_USER>
```

Không có phân trang. Response là mảng sắp xếp `created_at` mới nhất trước:

```json
[
  {
    "id": "df093a48-ed78-4109-ac96-a75be34ab35c",
    "title": "Giờ làm việc của công ty là mấy giờ?",
    "created_at": "2026-08-10T07:15:42.120314"
  }
]
```

`title` lấy từ câu hỏi đầu tiên, tối đa 40 ký tự Unicode; dài hơn thì server thêm
`...`. Mảng rỗng nghĩa là user chưa có session trong workspace này. HRM không cần
tự giữ bảng `user_id → session_ids`; lấy danh sách này khi mở màn hình lịch sử.

#### 6.13.2 Đọc messages trong session

Route nên dùng cho code mới:

```http
GET /workspaces/hrm/chat/sessions/<SESSION_ID>/messages
Authorization: Bearer <ACCESS_TOKEN_CỦA_USER>
```

Route tương thích trả cùng schema và cùng dữ liệu:

```http
GET /workspaces/hrm/chat/history?session_id=<SESSION_ID>
Authorization: Bearer <ACCESS_TOKEN_CỦA_USER>
```

Không có phân trang. Messages sắp xếp cũ nhất trước:

```json
[
  {
    "id": "f3f523dc-a9ea-4e35-8fbf-069127dd76f0",
    "role": "user",
    "content": "Giờ làm việc của công ty là mấy giờ?",
    "citations": [],
    "created_at": "2026-08-10T07:15:42.125901"
  },
  {
    "id": "57b3e448-11c7-48ab-8e36-0f6957235522",
    "role": "assistant",
    "content": "Giờ làm việc là 08:00–17:00.[chunk:3f601309-1f9e-4f88-9f16-1077bb849460]",
    "citations": ["3f601309-1f9e-4f88-9f16-1077bb849460"],
    "created_at": "2026-08-10T07:15:43.601842"
  }
]
```

`citations` ở history là mảng **chunk UUID**, khác với object giàu thông tin của
SSE `event: citations`. Khi đọc lại, server re-check ACL tài liệu: citation user
không còn được xem sẽ bị loại và marker UUID tương ứng bị xóa khỏi `content`.

Session không tồn tại trả `200 []` để hai route đọc có cùng hành vi. Session tồn
tại nhưng thuộc user khác trả `403 FORBIDDEN`. `session_id` thiếu hoặc không phải
UUID trả `400 INVALID_REQUEST`.

#### 6.13.3 Xóa session

```http
DELETE /workspaces/hrm/chat/sessions/<SESSION_ID>
Authorization: Bearer <ACCESS_TOKEN_CỦA_USER>
```

- Owner xóa thành công → `204 No Content`, body rỗng.
- Không tồn tại → `404 RESOURCE_NOT_FOUND`.
- Thuộc user khác → `403 FORBIDDEN`.
- Không có quyền admin-delete: mỗi user chỉ xóa được session của mình.

FK `chat_messages.session_id` khai báo `ON DELETE CASCADE`, nên xóa session sẽ xóa
toàn bộ messages trong session đó trong cùng thao tác database. Route không xóa
document, Qdrant point hoặc object MinIO.

---

## 7. Lỗi

### 7.1 Format chung

Mọi lỗi HTTP đều trả về đúng một cấu trúc — kể cả lỗi framework, route không tồn
tại, sai method:

```json
{
  "error": {
    "code": "RESOURCE_NOT_FOUND",
    "message": "Resource not found"
  }
}
```

| Field | Ghi chú |
|---|---|
| `error.code` | **Chuỗi ổn định, máy đọc. HRM phải branch theo field này** |
| `error.message` | Tiếng Anh, cho người đọc. **Câu chữ không thuộc contract, có thể đổi** |
| `error.details` | Hiện xuất hiện khi toàn bộ file upload bị loại: đọc `error.details.rejected` theo mục 3.4. Các lỗi khác có thể bỏ qua field không biết |

Ngoại lệ duy nhất: `204 No Content` (xóa thành công) có body rỗng.

> Đừng bao giờ so sánh chuỗi `message` trong code. Chỉ dùng `code`.

### 7.2 Bảng đầy đủ

Toàn bộ mã lỗi có thể gặp trên 10 endpoint trong tài liệu này:

| HTTP | `code` | Nghĩa | HRM nên làm gì |
|---|---|---|---|
| 400 | `INVALID_REQUEST` | Body/tham số sai, hoặc không có file nào hợp lệ | Sửa request. **Không retry** |
| 400 | `INVALID_ACCESS_MODE` | `access_mode` không phải `workspace_default`/`restricted` | Sửa request. **Không retry** |
| 400 | `HRM_SCOPE_MISMATCH` | `{workspace_id}` sai so với cấu hình server | Sai config phía HRM. **Không retry** |
| 401 | `UNAUTHORIZED` | Thiếu/sai định dạng header `Authorization` | Sửa code. **Không retry** |
| 401 | `INVALID_TOKEN` | Token không hợp lệ vì bất kỳ lý do gì | Lấy token mới, thử lại **1 lần** |
| 403 | `FORBIDDEN` | Không phải member, hoặc session của user khác | **Không retry** |
| 403 | `WORKSPACE_ADMIN_REQUIRED` | Cần role admin (upload). **Xóa** thiếu quyền ra `404`, không phải mã này | **Không retry**. Chặn ở UI |
| 403 | `HRM_ROLE_REQUIRED` | `role` không hợp lệ | **Không retry**. Sửa cách phát hành token |
| 403 | `CHATBOT_PERMISSION_REQUIRED` | Thiếu `CHATBOT_USE` | **Không retry**. Cấp quyền hoặc ẩn chat |
| 403 | `CHATBOT_UPLOAD_PERMISSION_REQUIRED` | Upload thiếu exact permission `CHATBOT_UPLOAD_DOCUMENT` | **Không retry**. Cấp quyền hoặc ẩn upload |
| 404 | `RESOURCE_NOT_FOUND` | Không tồn tại / workspace khác / không có quyền (xem *hoặc* xóa) / URL sai | **Không retry**. Với `DELETE`: coi như đã xong |
| 405 | `METHOD_NOT_ALLOWED` | Sai HTTP method | Bug phía HRM. **Không retry** |
| 413 | `PAYLOAD_TOO_LARGE` | Body vượt 50 MiB | Chia nhỏ. **Không retry** |
| 415 | `UNSUPPORTED_MEDIA_TYPE` | Sai `Content-Type` | Bug phía HRM. **Không retry** |
| 422 | `UNPROCESSABLE_ENTITY` | JSON hợp lệ nhưng sai schema | Bug phía HRM. **Không retry** |
| 429 | `RATE_LIMITED` | Vượt rate limit | **Retry** với exponential backoff |
| 500 | `INTERNAL_ERROR` | Lỗi server không xác định | **Retry** với backoff |
| 500 | `AUTHZ_ERROR` | Dịch vụ phân quyền chết/timeout | **Retry** với backoff |
| 500 | `AUTHZ_REVOKE_FAILED` | Không thu hồi được quyền khi xóa; chưa xóa gì | **Retry** — an toàn |
| 502 | `GENERATION_SERVICE_UNAVAILABLE` | LLM/embedding/vector store chết trước khi stream mở | **Retry** với backoff |

### 7.3 Quy tắc retry gọn

```
4xx  →  KHÔNG retry (trừ 429). Đây là bug hoặc thiếu quyền phía client.
429  →  Retry, exponential backoff.
5xx  →  Retry, exponential backoff, tối đa 3 lần.
```

Không có header `Retry-After` cho `429`. Tự chọn backoff — cửa sổ rate limit mặc
định là 60 giây, nên khởi điểm 5 giây rồi nhân đôi là hợp lý.

---

## 8. Giới hạn hiện tại

Ghi trung thực để HRM biết trước, không phải để bào chữa.

Đã sửa ở Phase 5, **không còn là giới hạn**:

| Từng là giới hạn | Nay |
|---|---|
| Upload lỗi một phần diễn ra âm thầm | `202` trả kèm mảng `rejected` với `reason_code` từng file (mục 3.4) |
| `DELETE` lộ sự tồn tại của tài liệu mà `GET` cố giấu | Cả hai cùng trả `404 RESOURCE_NOT_FOUND` (mục 5.4) |
| HRM phải nhét UUID workspace vào mọi URL | Dùng alias `hrm` (mục 2.5). UUID vẫn dùng được |
| Chưa chứng minh service đọc được cấu hình HRM từ `.env` | Đã verify khởi động thật, có dòng log INFO xác nhận mode/alg/claim/workspace |

### 8.1 Chưa có idempotency

Upload **cùng một file hai lần sẽ tạo ra hai document riêng biệt**, hai
`document_id` khác nhau, cả hai đều được index, và chat có thể trích dẫn cả hai
cho cùng một câu hỏi.

Không có idempotency key, không có dedup theo checksum (server *có* tính SHA-256
và lưu, nhưng **không** dùng nó để chặn trùng).

**HRM phải tự chống trùng.** Ví dụ: giữ cột `rag_document_id` và chỉ upload khi
cột đó còn `NULL`; upload lại thì xóa cái cũ trước.

Rủi ro thực tế cần đề phòng: request upload timeout ở phía HRM nhưng server vẫn
xử lý xong. Retry mù sẽ tạo bản trùng. Khi timeout, nên dùng endpoint list
(optional) để kiểm tra trước khi retry.

### 8.2 Chưa hỗ trợ xóa theo ID bên ngoài

Chỉ xóa được theo `document_id` do RAG service cấp. Không có external ID, không
có cột metadata tuỳ ý cho HRM gắn ID của mình. Xem lại mục 5.2.

### 8.3 PDF scan chưa OCR được

Chưa đấu nối OCR provider nào. PDF ảnh → `FAILED` / `NEEDS_OCR` vĩnh viễn.

Ảnh hưởng thực tế: hợp đồng scan, quyết định có dấu đỏ, form viết tay — nhóm tài
liệu rất phổ biến trong HR — đều **không dùng được**. Cần thống nhất với người
dùng cuối trước khi triển khai.

### 8.4 Chưa có versioning

Không có `/v1`. Đổi contract là đổi trực tiếp trên path hiện tại.

**Đề xuất:** trước khi lên production, hai bên chốt cơ chế version (thêm `/v1`
hoặc dùng header). Không thì bản nâng cấp sau sẽ làm hỏng HRM mà không báo trước.

### 8.5 Rate limit hiện tại

Sliding window trong bộ nhớ, khóa theo user (subject của token):

| Nhóm | Giới hạn mặc định | Biến môi trường |
|---|---|---|
| Chat | **30 request / 60 giây / user** | `RATE_LIMIT_CHAT_PER_WINDOW` |
| Upload | **10 request / 60 giây / user** | `RATE_LIMIT_UPLOAD_PER_WINDOW` |
| Cửa sổ | **60 giây** | `RATE_LIMIT_WINDOW_SECS` |

**Không** có rate limit trên GET status, DELETE và `/health`.

Hai điều cần biết:

- Giới hạn tính **theo user**, không theo HRM backend. Nếu HRM gọi bằng token
  riêng của từng nhân viên thì mỗi người có hạn mức riêng.
- Bộ đếm nằm **trong bộ nhớ của một process**. Restart API là reset sạch. Chạy
  nhiều instance thì mỗi instance đếm riêng — tức là giới hạn thực tế bị nhân
  lên theo số instance. Không nên coi đây là cơ chế bảo vệ nghiêm túc.

### 8.6 Những điều khác nên biết trước

Đọc được trong code, HRM nên nắm:

1. **Không có webhook/callback.** Muốn biết ingest xong phải poll. Nếu HRM cần
   push, phải đề xuất cho phase sau.

2. **Không có endpoint tải lại file gốc trong phạm vi này.** HRM phải tự giữ bản
   gốc.

3. **Timestamp không có timezone.** Xem cảnh báo ở mục 4.1. Đây là lỗi runtime
   rất dễ dính.

4. **API đã mở cho LAN.** Deployment bàn giao bind `0.0.0.0:18083` và firewall
   đã mở inbound TCP port `18083`; máy khác trong LAN gọi qua
   `http://<RAG_HOST>:18083`. Endpoint hiện vẫn là HTTP, chưa phải URL production.
   Xem 1.4.

5. **Đã verify end-to-end bằng token production thật của HRM và shared secret thật.**
   Phase 6 xác nhận issuer `hrm-gm-group-access`, canonical user ID lấy từ `userid`,
   role `HR` đồng bộ thành đúng một tuple `admin`; upload, poll, chat SSE/citation và
   delete đều PASS. Không ghi token hoặc secret vào tài liệu. Xem
   `docs/PHASE6_RESULT.md` để đọc bằng chứng đã khử dữ liệu bí mật.

6. **Chat lọc theo tài liệu `COMPLETED` + `DONE`.** Tài liệu đang `PROCESSING`
   hoặc `FAILED` hoàn toàn vô hình với chat. Không có kết quả một phần.

7. **Retrieval lấy tối đa 5 đoạn mỗi câu hỏi** (`QDRANT_TOP_K=5`). Câu hỏi cần
   tổng hợp từ nhiều tài liệu có thể trả lời thiếu.

8. **Chưa có kiểm soát độ dài `message`.** API không giới hạn, nhưng câu quá dài
   sẽ bị LLM từ chối hoặc cắt. HRM nên tự giới hạn ở UI.

9. **Model trả lời và model embedding cấu hình phía server.** HRM không chọn được
   model, không chỉnh được temperature, không sửa được system prompt.

10. **Câu trả lời được sinh bằng tiếng Việt hay tiếng Anh phụ thuộc LLM**, không
    có tham số ép ngôn ngữ.

---

## 9. Checklist tích hợp

Tick dần khi làm.

### Chuẩn bị (làm cùng team RAG)

- [ ] Nhận **base URL** thật (host + port + có HTTPS hay không)
- [x] ~~Xác nhận API đã bind ra ngoài `127.0.0.1`~~ — deployment bàn giao bind
      `0.0.0.0:18083`, firewall đã mở inbound TCP `18083`; gọi được qua LAN (mục 1.4)
- [x] ~~Nhận **UUID workspace cố định**~~ — không còn cần: dùng alias `hrm` trong path (mục 2.5).
      Muốn gọi bằng UUID thì workspace là `fa76881f-6367-4b80-a89e-a3e01206a806`
- [x] ~~Xác nhận team RAG đã bật `HRM_MODE=true`~~ — đã bật và verify từ `.env` (Phase 5.1)
- [x] ~~Bàn giao shared secret để RAG verify token HRM~~ — đã cấu hình và verify
      end-to-end bằng token production thật ở Phase 6; secret không nằm trong repo/tài liệu
- [x] ~~Xác nhận `JWT_ISSUER` khớp issuer của HRM~~ — `hrm-gm-group-access`
- [x] ~~Xác nhận `JWT_SUBJECT_CLAIM=userid`~~ — canonical user ID đã được verify ở Phase 6

### Token

- [ ] Token HRM có claim `userid`, giá trị ổn định vĩnh viễn theo nhân viên
- [ ] Token có claim `role` ∈ {`ADMIN`, `HR`, `MANAGER`, `EMPLOYEE`} — **đúng hoa thường**
- [ ] Token có `permissions` chứa `CHATBOT_USE` cho user được phép chat
- [ ] Token có `permissions` chứa `CHATBOT_UPLOAD_DOCUMENT` cho user được phép upload
- [ ] Đã thử token **hết hạn** → nhận `401 INVALID_TOKEN`
- [ ] Đã thử role sai (ví dụ `SUPERVISOR`) → nhận `403 HRM_ROLE_REQUIRED`

### Kết nối cơ bản

- [ ] `GET /health` trả `200 {"status":"ok","db":"connected"}`
- [ ] Gọi một endpoint có auth bằng token thật → không phải `401`

### Upload

- [ ] Upload một PDF có text → nhận `202` kèm `document_id`
- [ ] **Đã lưu `document_id` vào database của HRM**
- [ ] Upload bằng token thiếu `CHATBOT_UPLOAD_DOCUMENT` → nhận `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED`
- [ ] Upload bằng token `MANAGER` hoặc `EMPLOYEE` thiếu `CHATBOT_UPLOAD_DOCUMENT` → nhận `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED`
- [ ] Upload bằng token `MANAGER` hoặc `EMPLOYEE` có `CHATBOT_UPLOAD_DOCUMENT` → vẫn nhận `403 WORKSPACE_ADMIN_REQUIRED`
- [ ] Upload file không hỗ trợ (ví dụ `.xlsx`) → nhận `400 INVALID_REQUEST`,
      `error.details.rejected[0].reason_code = UNSUPPORTED_MEDIA_TYPE`
- [ ] Upload lô hỗn hợp (1 file tốt + 1 file `.xlsx`) → `202`, `documents` có 1 phần tử
      và `rejected` có 1 phần tử. **Đọc `rejected`, đừng đếm `documents`**
- [ ] Upload PDF scan → theo dõi tới `FAILED` / `NEEDS_OCR`, và HRM hiển thị được thông báo phù hợp

### Poll trạng thái

- [ ] Poll ngay sau upload → thấy `PROCESSING`
- [ ] Poll tới khi thấy `COMPLETED` / `DONE`
- [ ] Kiểm tra `chunk_count > 0`
- [ ] Parse `created_at` / `updated_at` **không ném exception** (dùng `LocalDateTime`)
- [ ] Có timeout dừng poll (khuyến nghị 15 phút) và cảnh báo
- [ ] Xử lý được `404` khi poll một `document_id` không tồn tại

### Chat

- [ ] Sinh `session_id` bằng UUID v4 ngẫu nhiên
- [ ] Nhận được stream, đọc được các event
- [ ] **Gom toàn bộ text rồi mới parse marker** — *xem 6.4*
- [ ] Thay `[chunk:N]` bằng link, tra cứu theo **giá trị** `index`
- [ ] Xử lý được marker không map được (xóa khỏi text hiển thị)
- [ ] Escape HTML cho `document_name` và `snippet`
- [ ] Xử lý được `citations` rỗng (hỏi câu không có trong tài liệu)
- [ ] Coi thiếu `event: done` là lỗi
- [ ] Xử lý được `event: error` giữa chừng (HTTP vẫn `200`)
- [ ] Bỏ qua dòng comment `:keep-alive`
- [ ] Multi-turn: gửi lại cùng `session_id`, xác nhận LLM nhớ ngữ cảnh
- [ ] Chat bằng token thiếu `CHATBOT_USE` → nhận `403 CHATBOT_PERMISSION_REQUIRED`

### Xóa

- [ ] Xóa bằng `document_id` đã lưu → nhận `204`
- [ ] Poll lại sau khi xóa → nhận `404`
- [ ] Xóa hai lần → lần hai `404`, HRM coi là thành công
- [ ] Xóa bằng token `MANAGER` hoặc `EMPLOYEE` → nhận **`404 RESOURCE_NOT_FOUND`** (không phải `403` — xem 5.4).
      Chặn nút xóa theo `role` trong token, đừng dựa vào mã lỗi của RAG

### Xử lý lỗi

- [ ] Branch theo `error.code`, **không** theo `error.message`
- [ ] Không retry 4xx (trừ 429)
- [ ] Retry 429 và 5xx với exponential backoff, có giới hạn số lần
- [ ] Log kèm `document_id` / `session_id` để còn đối chiếu với log phía RAG
- [ ] Xử lý được `502` (trước stream) và `event: error` (trong stream) như hai đường khác nhau

### Trước khi lên production

- [ ] Chốt cơ chế versioning API với team RAG (xem 8.4)
- [ ] Chốt cách chống upload trùng phía HRM (xem 8.1)
- [ ] Thống nhất với người dùng cuối rằng PDF scan chưa dùng được (xem 8.3)
- [x] ~~Chạy smoke test chung end-to-end bằng token production thật~~ — upload,
      chat/citation và delete đều PASS ở Phase 6 (xem 8.6 mục 5)

---

## Phụ lục — tóm tắt biến môi trường phía RAG

HRM **không** đặt các biến này, nhưng cần biết để trao đổi khi debug.

| Biến | Ảnh hưởng tới HRM |
|---|---|
| `API_BIND_ADDR` | Base URL |
| `HRM_MODE` | Phải `true`, nếu không toàn bộ logic role/permission của HRM không chạy |
| `HRM_TENANT_ID`, `HRM_WORKSPACE_ID` | Phạm vi server tự chốt. `HRM_WORKSPACE_ID` cũng là giá trị alias `hrm` resolve về |
| `JWT_ISSUER` | Phải khớp `iss` trong token HRM |
| `JWT_SUBJECT_CLAIM` | Phải là `userid` |
| `JWT_ALG`, `JWT_HMAC_SECRET`, `JWT_JWKS_URL` | Cách verify chữ ký |
| `JWT_VERIFY_AUDIENCE` | `false` cho HRM → token không cần `aud` |
| `DOCUMENT_MAX_UPLOAD_BYTES` | Giới hạn upload (mặc định 50 MiB) |
| `RATE_LIMIT_CHAT_PER_WINDOW`, `RATE_LIMIT_UPLOAD_PER_WINDOW`, `RATE_LIMIT_WINDOW_SECS` | Rate limit |
| `QDRANT_TOP_K` | Số đoạn tối đa mỗi câu hỏi (mặc định 5) |
| `INGESTION_JOB_MAX_ATTEMPTS` | Số lần retry ingest trước khi `FAILED` (mặc định 5) |
| `GMRAG_GRAPH_EXTRACTION_ENABLED` | Có thấy stage `GRAPH_EXTRACTION` hay không |

---

*Tài liệu này được sinh ở Phase 4, đọc trực tiếp từ source `gmrag_api/src`.
Các mục `TODO` được tổng hợp lại trong `docs/PHASE4_RESULT.md`.*
