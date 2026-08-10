# Bàn giao tích hợp HRM RAG API

## Địa chỉ API tạm thời

`http://192.168.169.150:18083`

Đây là môi trường tạm trên máy dev trong mạng LAN nội bộ. Địa chỉ chỉ hoạt động
trong giờ máy được bật và có thể thay đổi khi mạng cấp lại IP. Khi có server chính
thức, team RAG sẽ cung cấp base URL cố định.

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
3. IP trên là môi trường tạm trên máy dev, không phải endpoint production. Dịch
   vụ chỉ chạy khi máy dev và API đang bật.

## Test nhanh nhất

```bash
curl http://192.168.169.150:18083/health
```

Kết quả mong đợi:

```json
{"status":"ok","db":"connected"}
```

## Hai câu cần team HRM trả lời

1. `CHATBOT_UPLOAD_DOCUMENT` có bao gồm quyền **xóa** tài liệu, hay chỉ upload?
   Câu trả lời quyết định có áp dụng permission gate cho DELETE hay không.
2. Branch HRM nào đang deploy? Snapshot source đã đọc còn hardcode issuer
   `restaurant-access`, trong khi token production đã kiểm chứng dùng
   `hrm-gm-group-access`.

## Giới hạn hiện tại

- Chưa có idempotency cho upload.
- PDF scan chưa OCR được.
- Chưa có prefix `/v1`.
- Endpoint tạm dùng HTTP, chưa có TLS.

Chi tiết xem [mục 8 trong `INTEGRATION_GUIDE.md`](./INTEGRATION_GUIDE.md#8-giới-hạn-hiện-tại).
