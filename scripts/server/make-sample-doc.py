#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sinh một quy chế nhân sự tiếng Việt cỡ ~25 trang A4 để ĐO chi phí ingest.

Mục đích duy nhất: có một tài liệu đại diện đúng loại văn bản HRM sẽ upload
(chương/điều/khoản, tiếng Việt có dấu), để đo số chunk và thời gian xử lý thật
thay vì ước lượng. Nội dung là văn bản mẫu, không phải quy chế có hiệu lực.

Dùng: python3 make-sample-doc.py <so_trang> > quyche.md
"""
import sys

WORDS_PER_PAGE = 450  # trang A4 tiếng Việt, cỡ chữ 12, giãn dòng 1.5

CHUONG = [
    "QUY ĐỊNH CHUNG",
    "TUYỂN DỤNG VÀ THỬ VIỆC",
    "THỜI GIỜ LÀM VIỆC VÀ NGHỈ NGƠI",
    "TIỀN LƯƠNG, PHỤ CẤP VÀ THƯỞNG",
    "BẢO HIỂM VÀ PHÚC LỢI",
    "ĐÀO TẠO VÀ PHÁT TRIỂN",
    "ĐÁNH GIÁ KẾT QUẢ CÔNG VIỆC",
    "KỶ LUẬT LAO ĐỘNG VÀ TRÁCH NHIỆM VẬT CHẤT",
    "AN TOÀN, VỆ SINH LAO ĐỘNG",
    "CHẤM DỨT HỢP ĐỒNG LAO ĐỘNG",
]

DIEU_TIEU_DE = [
    "Phạm vi điều chỉnh", "Đối tượng áp dụng", "Nguyên tắc thực hiện",
    "Trách nhiệm của người lao động", "Trách nhiệm của người sử dụng lao động",
    "Hồ sơ tuyển dụng", "Thời gian thử việc", "Đánh giá kết quả thử việc",
    "Ký kết hợp đồng lao động", "Thời giờ làm việc bình thường",
    "Làm thêm giờ", "Nghỉ hằng tuần", "Nghỉ lễ, tết", "Nghỉ phép năm",
    "Nghỉ việc riêng có hưởng lương", "Nghỉ không hưởng lương",
    "Nguyên tắc trả lương", "Kỳ hạn trả lương", "Phụ cấp trách nhiệm",
    "Phụ cấp đi lại và ăn trưa", "Thưởng hiệu quả công việc",
    "Thưởng cuối năm", "Nâng bậc lương", "Bảo hiểm xã hội bắt buộc",
    "Bảo hiểm y tế", "Khám sức khỏe định kỳ", "Chế độ thai sản",
    "Kế hoạch đào tạo hằng năm", "Cam kết sau đào tạo", "Chu kỳ đánh giá",
    "Tiêu chí đánh giá", "Sử dụng kết quả đánh giá", "Các hành vi vi phạm",
    "Hình thức xử lý kỷ luật", "Trình tự xử lý kỷ luật",
    "Bồi thường thiệt hại", "Trang bị bảo hộ lao động",
    "Phòng chống cháy nổ", "Các trường hợp chấm dứt hợp đồng",
    "Thời hạn báo trước", "Bàn giao công việc", "Trợ cấp thôi việc",
    "Giải quyết tranh chấp", "Điều khoản thi hành",
]

DOAN = [
    "Quy chế này quy định chi tiết về quyền, nghĩa vụ và trách nhiệm của các bên "
    "trong quan hệ lao động tại Công ty, bảo đảm phù hợp với Bộ luật Lao động và "
    "các văn bản hướng dẫn thi hành hiện hành.",

    "Người lao động có trách nhiệm chấp hành nghiêm túc nội quy lao động, hoàn "
    "thành công việc được giao đúng tiến độ và chất lượng, giữ gìn bí mật kinh "
    "doanh và bảo vệ tài sản của Công ty.",

    "Người sử dụng lao động có trách nhiệm bảo đảm việc làm, trả lương đầy đủ và "
    "đúng hạn, thực hiện đầy đủ các chế độ bảo hiểm và phúc lợi theo quy định của "
    "pháp luật và theo thỏa thuận trong hợp đồng lao động.",

    "Thời giờ làm việc bình thường không quá 08 giờ trong một ngày và không quá "
    "48 giờ trong một tuần. Công ty áp dụng chế độ làm việc từ thứ Hai đến thứ "
    "Sáu, buổi sáng từ 08 giờ 00 đến 12 giờ 00, buổi chiều từ 13 giờ 00 đến 17 giờ 00.",

    "Việc làm thêm giờ phải được sự đồng ý của người lao động và phải được người "
    "quản lý trực tiếp phê duyệt bằng văn bản trước khi thực hiện. Tổng số giờ làm "
    "thêm không vượt quá 40 giờ trong một tháng và không quá 200 giờ trong một năm.",

    "Người lao động làm việc đủ 12 tháng cho Công ty được nghỉ hằng năm 12 ngày "
    "làm việc và hưởng nguyên lương. Cứ đủ 05 năm làm việc thì số ngày nghỉ hằng "
    "năm được cộng thêm 01 ngày.",

    "Đơn xin nghỉ phép phải được gửi tới người quản lý trực tiếp trước ít nhất 03 "
    "ngày làm việc. Trường hợp nghỉ đột xuất vì lý do sức khỏe hoặc việc gia đình "
    "cấp bách, người lao động phải thông báo ngay trong ngày và bổ sung đơn sau.",

    "Tiền lương được trả căn cứ vào vị trí công việc, mức độ phức tạp, kết quả "
    "thực hiện công việc và thời gian làm việc thực tế. Công ty bảo đảm mức lương "
    "không thấp hơn mức lương tối thiểu vùng do Chính phủ công bố.",

    "Tiền lương được trả một lần vào ngày 05 của tháng liền kề. Trường hợp ngày "
    "trả lương trùng vào ngày nghỉ lễ hoặc ngày nghỉ hằng tuần thì được trả vào "
    "ngày làm việc liền trước đó.",

    "Công ty tham gia đóng bảo hiểm xã hội, bảo hiểm y tế và bảo hiểm thất nghiệp "
    "cho người lao động theo đúng tỷ lệ và thời hạn do pháp luật quy định, tính "
    "trên tiền lương ghi trong hợp đồng lao động.",

    "Người lao động được khám sức khỏe định kỳ ít nhất một lần trong một năm. Đối "
    "với người làm nghề, công việc nặng nhọc, độc hại, nguy hiểm, việc khám sức "
    "khỏe được thực hiện ít nhất 06 tháng một lần.",

    "Việc đánh giá kết quả công việc được thực hiện định kỳ 06 tháng một lần, căn "
    "cứ trên các chỉ tiêu đã được thống nhất từ đầu kỳ giữa người lao động và "
    "người quản lý trực tiếp.",

    "Kết quả đánh giá là căn cứ để xem xét nâng bậc lương, xét thưởng, quy hoạch "
    "và bổ nhiệm, đồng thời là cơ sở để xây dựng kế hoạch đào tạo phù hợp với nhu "
    "cầu phát triển của từng cá nhân.",

    "Người lao động vi phạm nội quy lao động thì tùy theo tính chất và mức độ vi "
    "phạm sẽ bị xử lý kỷ luật theo một trong các hình thức: khiển trách, kéo dài "
    "thời hạn nâng lương không quá 06 tháng, cách chức hoặc sa thải.",

    "Trình tự xử lý kỷ luật phải bảo đảm nguyên tắc công khai, minh bạch, có sự "
    "tham gia của tổ chức đại diện người lao động và phải lập thành biên bản. "
    "Người lao động có quyền tự bào chữa hoặc nhờ người khác bào chữa.",

    "Người lao động làm hư hỏng dụng cụ, thiết bị hoặc có hành vi gây thiệt hại "
    "tài sản của Công ty thì phải bồi thường theo quy định của pháp luật và theo "
    "mức độ lỗi thực tế.",

    "Công ty trang bị đầy đủ phương tiện bảo vệ cá nhân cho người lao động làm "
    "công việc có yếu tố nguy hiểm, tổ chức huấn luyện an toàn lao động và diễn "
    "tập phòng cháy chữa cháy định kỳ hằng năm.",

    "Khi đơn phương chấm dứt hợp đồng lao động, người lao động phải báo trước cho "
    "Công ty ít nhất 30 ngày đối với hợp đồng xác định thời hạn và ít nhất 45 ngày "
    "đối với hợp đồng không xác định thời hạn.",

    "Trước khi nghỉ việc, người lao động có trách nhiệm bàn giao đầy đủ công việc, "
    "hồ sơ, tài liệu và tài sản được giao cho người kế nhiệm hoặc người quản lý "
    "trực tiếp, có xác nhận bằng biên bản bàn giao.",

    "Mọi tranh chấp phát sinh được giải quyết trước hết thông qua thương lượng, "
    "hòa giải nội bộ. Trường hợp không đạt được thỏa thuận, các bên có quyền yêu "
    "cầu hòa giải viên lao động hoặc khởi kiện tại Tòa án có thẩm quyền.",
]


def main():
    target_pages = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    target_words = target_pages * WORDS_PER_PAGE

    out = ["# QUY CHẾ QUẢN LÝ NHÂN SỰ", "", "(Tài liệu mẫu dùng để đo hiệu năng hệ thống)", ""]
    words = sum(len(line.split()) for line in out)

    dieu_no = 0
    chuong_no = 0
    khoan_cycle = 0

    while words < target_words:
        chuong_no += 1
        out.append("")
        out.append(f"## CHƯƠNG {chuong_no}. {CHUONG[(chuong_no - 1) % len(CHUONG)]}")
        out.append("")
        words += 6

        for _ in range(5):
            if words >= target_words:
                break
            dieu_no += 1
            tieu_de = DIEU_TIEU_DE[(dieu_no - 1) % len(DIEU_TIEU_DE)]
            out.append(f"### Điều {dieu_no}. {tieu_de}")
            out.append("")
            words += 5

            for khoan in range(1, 4):
                if words >= target_words:
                    break
                doan = DOAN[khoan_cycle % len(DOAN)]
                khoan_cycle += 1
                out.append(f"{khoan}. {doan}")
                out.append("")
                words += len(doan.split())

    sys.stdout.write("\n".join(out) + "\n")


if __name__ == "__main__":
    main()
