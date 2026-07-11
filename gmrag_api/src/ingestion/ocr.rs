/// Fallback OCR khi text native của PDF không đủ.
/// Production chưa có provider — trả `None` (không inject chữ giả vào pipeline).
/// Chỉ `cfg(test)` mới compile nhánh mock; integration test (không `cfg(test)` trên lib) xác minh path production.
pub async fn vision_ocr_fallback(image_bytes: &[u8]) -> Option<String> {
    let _ = image_bytes;
    ocr_fallback_text()
}

#[cfg(not(test))]
fn ocr_fallback_text() -> Option<String> {
    None
}

#[cfg(test)]
fn ocr_fallback_text() -> Option<String> {
    Some(MOCK_OCR_TEXT.to_string())
}

#[cfg(test)]
const MOCK_OCR_TEXT: &str = "mock_ocr_text";

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unit_test_cfg_fallback_returns_mock() {
        let result = vision_ocr_fallback(b"").await;
        assert_eq!(result.as_deref(), Some(MOCK_OCR_TEXT));
    }
}
