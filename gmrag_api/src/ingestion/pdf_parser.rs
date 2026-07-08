use lopdf::Document;
use pdf_extract::{PlainTextOutput, output_doc_page};
use std::path::Path;

pub const MIN_TEXT_CHARS: usize = 50;

#[derive(Debug)]
pub enum PdfParseError {
    Load(String),
    Decrypt(String),
    NoPages,
}

impl std::fmt::Display for PdfParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PdfParseError::Load(e) => write!(f, "PDF load error: {e}"),
            PdfParseError::Decrypt(e) => write!(f, "PDF decrypt error: {e}"),
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
    extract_pdf_from_document(doc)
}

pub fn extract_pdf_from_bytes(bytes: &[u8]) -> Result<PdfExtractResult, PdfParseError> {
    let doc = Document::load_mem(bytes).map_err(|e| PdfParseError::Load(e.to_string()))?;
    extract_pdf_from_document(doc)
}

fn extract_pdf_from_document(mut doc: Document) -> Result<PdfExtractResult, PdfParseError> {
    if doc.is_encrypted() {
        doc.decrypt("")
            .map_err(|e| PdfParseError::Decrypt(e.to_string()))?;
    }

    let mut page_numbers: Vec<u32> = doc.get_pages().into_keys().collect();
    page_numbers.sort_unstable();

    if page_numbers.is_empty() {
        return Err(PdfParseError::NoPages);
    }

    let mut pages = Vec::with_capacity(page_numbers.len());

    for page_number in page_numbers {
        let text = extract_page_text(&doc, page_number);

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

fn extract_page_text(doc: &Document, page_number: u32) -> String {
    let mut text = String::new();
    let result = {
        let mut output = PlainTextOutput::new(&mut text);
        output_doc_page(doc, &mut output, page_number)
    };

    if result.is_err() {
        return String::new();
    }

    clean_extracted_text(&text)
}

fn clean_extracted_text(text: &str) -> String {
    let mut lines = Vec::new();
    let mut last_was_blank = false;

    for line in text.lines() {
        let normalized = line.split_whitespace().collect::<Vec<_>>().join(" ");

        if normalized.is_empty() {
            if !last_was_blank && !lines.is_empty() {
                lines.push(String::new());
                last_was_blank = true;
            }
        } else {
            lines.push(normalized);
            last_was_blank = false;
        }
    }

    while lines.last().is_some_and(|line| line.is_empty()) {
        lines.pop();
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use lopdf::content::{Content, Operation};
    use lopdf::{Object, Stream, dictionary};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn clean_extracted_text_collapses_runs_without_concatenating_words() {
        let text = "  Published   as\t a  \r\n\r\n conference   paper  ";

        assert_eq!(
            clean_extracted_text(text),
            "Published as a\n\nconference paper"
        );
    }

    #[test]
    fn extract_pdf_from_path_preserves_layout_inferred_spaces() {
        let path = write_positioned_words_pdf();
        let extracted = extract_pdf_from_path(&path);
        let _ = fs::remove_file(&path);
        let extracted = extracted.expect("positioned words PDF should extract");

        assert_eq!(extracted.pages.len(), 1);

        let text = &extracted.pages[0].text;
        assert!(
            text.contains("Published as a conference paper"),
            "extracted text was: {text:?}"
        );
        assert!(
            !text.contains("Publishedasaconferencepaper"),
            "extracted text was: {text:?}"
        );
    }

    fn write_positioned_words_pdf() -> PathBuf {
        let mut doc = Document::with_version("1.5");
        let pages_id = doc.new_object_id();
        let font_id = doc.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => "Courier",
        });
        let resources_id = doc.add_object(dictionary! {
            "Font" => dictionary! {
                "F1" => font_id,
            },
        });
        let mut operations = vec![
            Operation::new("BT", vec![]),
            Operation::new("Tf", vec!["F1".into(), 24.into()]),
        ];
        operations.extend(show_word_at("Published", 72));
        operations.extend(show_word_at("as", 220));
        operations.extend(show_word_at("a", 260));
        operations.extend(show_word_at("conference", 300));
        operations.extend(show_word_at("paper", 465));
        operations.push(Operation::new("ET", vec![]));

        let content = Content { operations };
        let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
        let page_id = doc.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "Contents" => content_id,
        });
        doc.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages",
                "Kids" => vec![Object::Reference(page_id)],
                "Count" => 1,
                "Resources" => resources_id,
                "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
            }),
        );
        let catalog_id = doc.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        doc.trailer.set("Root", catalog_id);

        let path = temp_pdf_path();
        doc.save(&path).expect("test PDF should save");
        path
    }

    fn show_word_at(word: &str, x: i64) -> Vec<Operation> {
        vec![
            Operation::new(
                "Tm",
                vec![1.into(), 0.into(), 0.into(), 1.into(), x.into(), 720.into()],
            ),
            Operation::new("Tj", vec![Object::string_literal(word)]),
        ]
    }

    fn temp_pdf_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos();

        std::env::temp_dir().join(format!(
            "gmrag-positioned-words-{}-{nanos}.pdf",
            std::process::id()
        ))
    }
}
