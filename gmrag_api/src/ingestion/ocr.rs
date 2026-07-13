/// Literal từng inject vào chunk khi mock OCR còn sống runtime (OCR-001 đã chặn ngoài test).
/// Dùng cho audit OCR-004; không phải provider thật.
pub const MOCK_OCR_MARKER: &str = "mock_ocr_text";

/// Trạng thái capability production OCR (gate cho apply reingest OCR-004).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OcrCapability {
    pub available: bool,
    pub provider: Option<&'static str>,
    pub detail: &'static str,
}

/// Production OCR chưa wire (ADR-24 chỉ chọn provider). Luôn unavailable cho đến task tích hợp riêng.
pub fn production_ocr_capability() -> OcrCapability {
    OcrCapability {
        available: false,
        provider: None,
        detail: "Production OCR is not integrated; vision_ocr_fallback returns None (ADR-24 selection only)",
    }
}

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
    Some(MOCK_OCR_MARKER.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unit_test_cfg_fallback_returns_mock() {
        let result = vision_ocr_fallback(b"").await;
        assert_eq!(result.as_deref(), Some(MOCK_OCR_MARKER));
    }

    #[test]
    fn production_capability_gate_is_closed_until_integration() {
        let cap = production_ocr_capability();
        assert!(!cap.available);
        assert!(cap.provider.is_none());
        assert!(!cap.detail.is_empty());
    }
}
