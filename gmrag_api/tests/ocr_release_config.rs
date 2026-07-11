//! Chứng minh hermetic: lib link không qua `cfg(test)` không phát sinh OCR giả.
//! Integration test biên dịch `gmrag_api` như dependency thường (không `cfg(test)`),
//! nên đây là path production-facing, không phải nhánh mock unit-test.

use gmrag_api::ingestion::ocr::vision_ocr_fallback;

#[tokio::test]
async fn production_ocr_fallback_returns_no_synthetic_text() {
    let result = vision_ocr_fallback(b"not-a-real-page-image").await;

    assert!(
        result.is_none(),
        "production OCR fallback must return None (no provider yet); got {result:?}"
    );

    // Phòng khi API đổi sang String rỗng: vẫn cấm literal mock trong non-test build
    if let Some(text) = &result {
        assert!(
            !text.contains("mock_ocr_text"),
            "synthetic mock_ocr_text must not appear outside cfg(test)"
        );
        assert!(
            text.trim().is_empty(),
            "production OCR must not invent page content"
        );
    }
}

#[tokio::test]
async fn production_ocr_fallback_empty_input_is_not_mock() {
    let result = vision_ocr_fallback(&[]).await;
    assert_ne!(
        result.as_deref(),
        Some("mock_ocr_text"),
        "non-test library must not return mock_ocr_text"
    );
    assert!(result.is_none() || result.as_deref() == Some(""));
}
