# Bàn giao tích hợp HRM RAG API

## Địa chỉ API local

`http://127.0.0.1:18083`

API hiện chỉ bind loopback trên máy dev, chưa mở ra LAN và chưa phải endpoint production.
Khi có server chính thức, team RAG sẽ cung cấp base URL cố định.

## Đọc theo thứ tự này

1. [`INTEGRATION_GUIDE.md`](./INTEGRATION_GUIDE.md) — đọc trước, đặc biệt phần
   xử lý SSE và citation.
2. [`openapi.yaml`](./openapi.yaml) — import vào Postman hoặc Swagger.
3. [`examples/`](./examples/) — có file `.http` và `smoke.sh` để chạy thử ngay.

## Ba điều cần biết trước khi bắt đầu

1. Corpus đang **rỗng có chủ đích**. Phải upload tài liệu công ty đã được phê
   duyệt trước khi cho nhân viên sử dụng. Khi chưa có tài liệu, bot trả lời rằng
   không tìm thấy thông tin và không trả citation.
2. Upload yêu cầu exact permission `CHATBOT_UPLOAD_DOCUMENT`. Theo seed hiện tại,
   chỉ `HR` và `ADMIN` có permission này. `MANAGER` và `EMPLOYEE` mặc định nhận
   `403 CHATBOT_UPLOAD_PERMISSION_REQUIRED` khi upload.
3. `MANAGER` và `EMPLOYEE` không được xóa. DELETE trả `404 RESOURCE_NOT_FOUND`
   thay vì `403` là có chủ đích để không tiết lộ tài liệu có tồn tại hay không.

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
curl http://127.0.0.1:18083/health
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
- Endpoint tạm dùng HTTP, chưa có TLS.

Chi tiết xem [mục 8 trong `INTEGRATION_GUIDE.md`](./INTEGRATION_GUIDE.md#8-giới-hạn-hiện-tại).
