use lopdf::Document;
use std::path::Path;

pub const MIN_TEXT_CHARS: usize = 50;

#[derive(Debug)]
pub enum PdfParseError {
    Load(String),
    NoPages,
}

impl std::fmt::Display for PdfParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PdfParseError::Load(e) => write!(f, "PDF load error: {e}"),
            PdfParseError::NoPages => write!(f, "PDF contains no pages"),
        }
    }
}

impl std::error::Error for PdfParseError {}

#[derive(Debug, Clone)]
pub struct PageExtract {
    pub page_number: u32,
    pub text: String,
    pub needs_ocr: bool,
    pub image_bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct PdfExtractResult {
    pub pages: Vec<PageExtract>,
}

pub fn extract_pdf_from_path(path: &Path) -> Result<PdfExtractResult, PdfParseError> {
    let doc = Document::load(path).map_err(|e| PdfParseError::Load(e.to_string()))?;

    let mut page_numbers: Vec<u32> = doc.get_pages().into_keys().collect();
    page_numbers.sort_unstable();

    if page_numbers.is_empty() {
        return Err(PdfParseError::NoPages);
    }

    let mut pages = Vec::with_capacity(page_numbers.len());

    for page_number in page_numbers {
        let text = doc
            .extract_text(&[page_number])
            .unwrap_or_default()
            .trim()
            .to_string();

        let char_count = text.chars().count();
        let needs_ocr = char_count < MIN_TEXT_CHARS;

        pages.push(PageExtract {
            page_number,
            text,
            needs_ocr,
            // Page rasterization for OCR will use pdfium-render in a later phase.
            image_bytes: Vec::new(),
        });
    }

    Ok(PdfExtractResult { pages })
}
