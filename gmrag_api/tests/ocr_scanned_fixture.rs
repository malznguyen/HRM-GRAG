//! Fixture image-only PDF (hermetic). Lib không `cfg(test)` → OCR production = None.

use gmrag_api::ingestion::pdf_parser::{MIN_TEXT_CHARS, extract_pdf_from_bytes};
use gmrag_api::ingestion::processor::{ProcessError, resolve_pages_for_chunking};
use lopdf::content::{Content, Operation};
use lopdf::{Document, Object, Stream, dictionary};

/// PDF 1 trang: Image XObject + `Do`, không text operator (scan/image-only).
fn build_image_only_page_pdf_bytes() -> Vec<u8> {
    let mut doc = Document::with_version("1.5");
    let pages_id = doc.new_object_id();

    // 2×2 DeviceGray — tối thiểu để có XObject ảnh thật, không cần decode OCR
    let image_id = doc.add_object(Stream::new(
        dictionary! {
            "Type" => "XObject",
            "Subtype" => "Image",
            "Width" => 2,
            "Height" => 2,
            "ColorSpace" => "DeviceGray",
            "BitsPerComponent" => 8,
        },
        vec![0x00, 0x40, 0xC0, 0xFF],
    ));

    let resources_id = doc.add_object(dictionary! {
        "XObject" => dictionary! {
            "Im1" => image_id,
        },
    });

    // q … cm /Im1 Do Q — vẽ ảnh full MediaBox, không BT/Tj
    let content = Content {
        operations: vec![
            Operation::new("q", vec![]),
            Operation::new(
                "cm",
                vec![
                    612.into(),
                    0.into(),
                    0.into(),
                    792.into(),
                    0.into(),
                    0.into(),
                ],
            ),
            Operation::new("Do", vec![Object::Name(b"Im1".to_vec())]),
            Operation::new("Q", vec![]),
        ],
    };
    let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
    let page_id = doc.add_object(dictionary! {
        "Type" => "Page",
        "Parent" => pages_id,
        "Contents" => content_id,
        "Resources" => resources_id,
        "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
    });
    doc.objects.insert(
        pages_id,
        Object::Dictionary(dictionary! {
            "Type" => "Pages",
            "Kids" => vec![Object::Reference(page_id)],
            "Count" => 1,
        }),
    );
    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id,
    });
    doc.trailer.set("Root", catalog_id);

    let mut bytes = Vec::new();
    doc.save_to(&mut bytes)
        .expect("image-only fixture PDF should serialize");
    assert!(
        bytes.starts_with(b"%PDF-"),
        "fixture must be a structural PDF"
    );
    bytes
}

fn build_rich_text_page_pdf_bytes() -> Vec<u8> {
    let mut doc = Document::with_version("1.5");
    let pages_id = doc.new_object_id();
    let font_id = doc.add_object(dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Helvetica",
    });
    let resources_id = doc.add_object(dictionary! {
        "Font" => dictionary! {
            "F1" => font_id,
        },
    });
    let line = "This is a sufficiently long native PDF text line for extraction.";
    let content = Content {
        operations: vec![
            Operation::new("BT", vec![]),
            Operation::new("Tf", vec!["F1".into(), 12.into()]),
            Operation::new(
                "Tm",
                vec![
                    1.into(),
                    0.into(),
                    0.into(),
                    1.into(),
                    72.into(),
                    720.into(),
                ],
            ),
            Operation::new("Tj", vec![Object::string_literal(line)]),
            Operation::new("ET", vec![]),
        ],
    };
    let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
    let page_id = doc.add_object(dictionary! {
        "Type" => "Page",
        "Parent" => pages_id,
        "Contents" => content_id,
        "Resources" => resources_id,
        "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
    });
    doc.objects.insert(
        pages_id,
        Object::Dictionary(dictionary! {
            "Type" => "Pages",
            "Kids" => vec![Object::Reference(page_id)],
            "Count" => 1,
        }),
    );
    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id,
    });
    doc.trailer.set("Root", catalog_id);

    let mut bytes = Vec::new();
    doc.save_to(&mut bytes)
        .expect("rich-text fixture PDF should serialize");
    bytes
}

#[tokio::test]
async fn image_only_pdf_parser_signals_needs_ocr_and_ingestion_is_terminal() {
    let bytes = build_image_only_page_pdf_bytes();
    let extracted =
        extract_pdf_from_bytes(&bytes).expect("structurally valid image-only PDF must parse");

    assert_eq!(
        extracted.pages.len(),
        1,
        "fixture must have at least one page"
    );
    let page = &extracted.pages[0];
    assert!(
        page.text.chars().count() < MIN_TEXT_CHARS,
        "image-only fixture native text must be below threshold; got {:?}",
        page.text
    );
    assert!(
        page.needs_ocr,
        "parser must mark image-only/low-text page as needs_ocr"
    );

    let err = resolve_pages_for_chunking(extracted.pages)
        .await
        .expect_err("image-only PDF without OCR provider must not succeed");
    assert!(
        matches!(err, ProcessError::NeedsOcr),
        "expected NeedsOcr, got {err:?}"
    );

    let (code, message, retryable) = err.failure_kind();
    assert_eq!(code, "NEEDS_OCR");
    assert_eq!(
        message,
        "Document requires OCR and no OCR provider is available"
    );
    assert!(!retryable);
}

#[tokio::test]
async fn rich_native_text_pdf_does_not_need_ocr_decision() {
    let bytes = build_rich_text_page_pdf_bytes();
    let extracted = extract_pdf_from_bytes(&bytes).expect("rich-text PDF must parse");
    assert_eq!(extracted.pages.len(), 1);
    let page = &extracted.pages[0];
    assert!(
        !page.needs_ocr,
        "rich native text must not set needs_ocr; text={:?}",
        page.text
    );

    let page_texts = resolve_pages_for_chunking(extracted.pages)
        .await
        .expect("rich native text must not fail with NEEDS_OCR");
    assert_eq!(page_texts.len(), 1);
    assert!(
        page_texts[0].chars().count() >= MIN_TEXT_CHARS,
        "resolved text should remain usable"
    );
}

#[tokio::test]
async fn mixed_document_with_one_ocr_page_is_not_completed() {
    let mut pages = extract_pdf_from_bytes(&build_rich_text_page_pdf_bytes())
        .expect("rich page")
        .pages;
    let mut image_only = extract_pdf_from_bytes(&build_image_only_page_pdf_bytes())
        .expect("image-only page")
        .pages;
    image_only[0].page_number = 2;
    pages.extend(image_only);

    assert!(pages.iter().any(|p| p.needs_ocr));
    assert!(pages.iter().any(|p| !p.needs_ocr));

    let err = resolve_pages_for_chunking(pages)
        .await
        .expect_err("mixed document with unprocessed OCR page must fail");
    assert!(matches!(err, ProcessError::NeedsOcr));
    let (code, _, retryable) = err.failure_kind();
    assert_eq!(code, "NEEDS_OCR");
    assert!(!retryable);
}
