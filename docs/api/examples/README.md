# Ví dụ chạy được

Kèm theo [`../INTEGRATION_GUIDE.md`](../INTEGRATION_GUIDE.md) và
[`../openapi.yaml`](../openapi.yaml).

| File | Dùng khi nào |
|---|---|
| `hrm-rag.http` | Gọi thử từng endpoint trong IDE. Có sẵn cả các case lỗi |
| `http-client.env.json` | Biến môi trường cho `hrm-rag.http` |
| `smoke.sh` | Chạy một mạch end-to-end cả 5 việc, dùng để nghiệm thu |

---

## `hrm-rag.http`

Cho **IntelliJ IDEA** (Tools → HTTP Client) hoặc **VS Code** + extension
*REST Client*. Tiện nhất cho dev Java vì chạy thẳng trong IDE.

1. Mở `http-client.env.json`, điền `baseUrl`, `workspaceId`, `accessToken`.
2. Mở `hrm-rag.http`, chọn environment `local` ở góc trên.
3. Bấm ▶ bên trái từng request.

Request 1–10 là luồng chính (health → upload → poll → chat → xóa). Request
`E1`–`E10` là các case lỗi — chạy để xác nhận client HRM xử lý đúng từng mã lỗi.

Request số 2 upload file `./fixtures/noi-quy-cong-ty.pdf`. Thư mục `fixtures/`
**không** có sẵn trong repo — tự trỏ sang một file thật trên máy bạn, hoặc tạo
thư mục và bỏ file vào.

> IntelliJ tự lưu `document_id` từ response upload sang các request sau. Nếu
> dùng VS Code REST Client thì phải copy tay `document_id` vào `{{documentId}}`.

---

## `smoke.sh`

Chạy tuần tự cả 5 việc và kiểm tra kết quả từng bước. Dùng khi muốn xác nhận
"hệ thống thông không" trước khi bắt tay viết code Java.

```bash
export RAG_BASE_URL="http://127.0.0.1:18083"
export RAG_WORKSPACE_ID="<uuid workspace cố định của HRM>"
export RAG_TOKEN="<access token role ADMIN/HR/MANAGER, có CHATBOT_USE>"

./smoke.sh                          # tự tạo file .md mẫu để upload
./smoke.sh /duong/dan/tai-lieu.pdf  # hoặc upload file của bạn
```

Cần `bash`, `curl`, `python3`. Không cần `jq`, không cài thêm gì.
Chạy được trên Linux, macOS và Git Bash trên Windows.

Script làm gì:

1. `GET /health`
2. Upload, in ra `document_id`
3. Poll trạng thái với chu kỳ tăng dần (2s → 5s → 30s), timeout 15 phút.
   Gặp `FAILED` thì giải thích `failure_code` và nói nên làm gì
4. Chat SSE, in **nguyên văn stream**, rồi ráp lại đúng cách và in kết quả
5. Xóa, và xác nhận đã xóa bằng cách poll lại (mong đợi `404`)

Bước 4 chính là phần đáng xem nhất: nó in ra stream thô để bạn **tận mắt thấy**
marker `[chunk:1]` bị cắt vụn qua nhiều event, rồi in text đã gom lại để thấy
cách xử lý đúng. Đoạn Python trong script là bản tham chiếu tối giản của
pseudocode Java ở mục 6.9 — logic giống hệt.

Kết thúc, script tự xóa tài liệu vừa tạo nên không để lại rác.

---

## Lưu ý bảo mật

- **Không commit token thật.** `http-client.env.json` chỉ chứa placeholder —
  giữ nguyên như vậy. Muốn dùng thật thì tạo `http-client.private.env.json`
  (IntelliJ tự ưu tiên file này và nó nằm trong `.gitignore` mặc định).
- `smoke.sh` đọc token từ biến môi trường, không hard-code. Đừng sửa lại.
- Ví dụ trong `hrm-rag.http` dùng placeholder `REPLACE_WITH_...` — nếu thấy
  giá trị trông giống token thật trong diff thì đó là nhầm lẫn, phải revert.
