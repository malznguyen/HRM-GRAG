use std::io::{Cursor, Read};

use quick_xml::{Reader, escape::unescape, events::Event};
use zip::ZipArchive;

pub const PDF_CONTENT_TYPE: &str = "application/pdf";
pub const DOCX_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
pub const TEXT_CONTENT_TYPE: &str = "text/plain";
pub const MARKDOWN_CONTENT_TYPE: &str = "text/markdown";

const DEFAULT_DOCUMENT_MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DocumentFormat {
    Pdf,
    Docx,
    Text,
    Markdown,
}

impl DocumentFormat {
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Pdf => PDF_CONTENT_TYPE,
            Self::Docx => DOCX_CONTENT_TYPE,
            Self::Text => TEXT_CONTENT_TYPE,
            Self::Markdown => MARKDOWN_CONTENT_TYPE,
        }
    }

    pub fn from_content_type(content_type: &str) -> Option<Self> {
        match content_type {
            PDF_CONTENT_TYPE => Some(Self::Pdf),
            DOCX_CONTENT_TYPE => Some(Self::Docx),
            TEXT_CONTENT_TYPE => Some(Self::Text),
            MARKDOWN_CONTENT_TYPE => Some(Self::Markdown),
            _ => None,
        }
    }

    pub fn detect_upload(filename: &str, data: &[u8]) -> Option<Self> {
        if is_parseable_pdf(data) {
            return Some(Self::Pdf);
        }
        if is_parseable_docx(data) {
            return Some(Self::Docx);
        }
        if !is_valid_text(data) {
            return None;
        }

        match sanitized_extension(filename).as_deref() {
            Some("txt") => Some(Self::Text),
            Some("md") => Some(Self::Markdown),
            _ => None,
        }
    }
}

pub fn document_max_upload_bytes() -> usize {
    std::env::var("DOCUMENT_MAX_UPLOAD_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DOCUMENT_MAX_UPLOAD_BYTES)
}

pub fn is_parseable_pdf(data: &[u8]) -> bool {
    data.starts_with(b"%PDF-") && lopdf::Document::load_mem(data).is_ok()
}

pub fn is_parseable_docx(data: &[u8]) -> bool {
    if !data.starts_with(b"PK\x03\x04") {
        return false;
    }

    let Ok(mut archive) = ZipArchive::new(Cursor::new(data)) else {
        return false;
    };
    let has_content_types = archive.by_name("[Content_Types].xml").is_ok();
    let has_document = archive.by_name("word/document.xml").is_ok();
    has_content_types && has_document
}

pub fn is_valid_text(data: &[u8]) -> bool {
    !data.is_empty() && !data.contains(&0) && std::str::from_utf8(data).is_ok()
}

pub fn extract_docx_text(data: &[u8]) -> Result<String, DocxParseError> {
    if !data.starts_with(b"PK\x03\x04") {
        return Err(DocxParseError);
    }

    let mut archive = ZipArchive::new(Cursor::new(data)).map_err(|_| DocxParseError)?;
    archive
        .by_name("[Content_Types].xml")
        .map_err(|_| DocxParseError)?;

    let mut document_xml = String::new();
    archive
        .by_name("word/document.xml")
        .map_err(|_| DocxParseError)?
        .read_to_string(&mut document_xml)
        .map_err(|_| DocxParseError)?;

    extract_wordprocessing_text(&document_xml)
}

pub fn decode_text(data: &[u8]) -> Result<String, TextDecodeError> {
    if !is_valid_text(data) {
        return Err(TextDecodeError);
    }
    String::from_utf8(data.to_vec()).map_err(|_| TextDecodeError)
}

fn extract_wordprocessing_text(xml: &str) -> Result<String, DocxParseError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut text = String::new();
    let mut in_text_node = false;

    loop {
        match reader.read_event().map_err(|_| DocxParseError)? {
            Event::Start(element) => match local_name(element.name().as_ref()) {
                b"t" => in_text_node = true,
                b"tab" if in_text_node => text.push('\t'),
                b"br" | b"cr" if in_text_node => text.push('\n'),
                _ => {}
            },
            Event::Empty(element) => match local_name(element.name().as_ref()) {
                b"tab" => text.push('\t'),
                b"br" | b"cr" => text.push('\n'),
                _ => {}
            },
            Event::Text(value) if in_text_node => {
                text.push_str(&value.xml10_content().map_err(|_| DocxParseError)?);
            }
            Event::GeneralRef(value) if in_text_node => {
                let reference = value.decode().map_err(|_| DocxParseError)?;
                let encoded = format!("&{reference};");
                text.push_str(&unescape(&encoded).map_err(|_| DocxParseError)?);
            }
            Event::End(element) => match local_name(element.name().as_ref()) {
                b"t" => in_text_node = false,
                b"p" => push_line_break(&mut text),
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
    }

    while text.ends_with('\n') {
        text.pop();
    }
    if text.trim().is_empty() {
        return Err(DocxParseError);
    }
    Ok(text)
}

fn push_line_break(text: &mut String) {
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
}

fn local_name(name: &[u8]) -> &[u8] {
    name.rsplit(|byte| *byte == b':').next().unwrap_or(name)
}

fn sanitized_extension(filename: &str) -> Option<String> {
    std::path::Path::new(filename)
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
}

#[derive(Debug)]
pub struct DocxParseError;

impl std::fmt::Display for DocxParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DOCX text could not be extracted")
    }
}

impl std::error::Error for DocxParseError {}

#[derive(Debug)]
pub struct TextDecodeError;

impl std::fmt::Display for TextDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Text document is not valid UTF-8 text")
    }
}

impl std::error::Error for TextDecodeError {}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use lopdf::{Document, Object, dictionary};
    use zip::{ZipWriter, write::SimpleFileOptions};

    use super::*;

    #[test]
    fn detects_supported_uploads_from_server_validated_content() {
        assert_eq!(
            DocumentFormat::detect_upload("renamed.bin", &minimal_pdf()),
            Some(DocumentFormat::Pdf)
        );
        assert_eq!(
            DocumentFormat::detect_upload("renamed.bin", &docx_fixture("Xin chào")),
            Some(DocumentFormat::Docx)
        );
        assert_eq!(
            DocumentFormat::detect_upload("notes.TXT", b"plain UTF-8"),
            Some(DocumentFormat::Text)
        );
        assert_eq!(
            DocumentFormat::detect_upload("notes.MD", b"# raw markdown"),
            Some(DocumentFormat::Markdown)
        );
    }

    #[test]
    fn rejects_invalid_or_ambiguous_uploads() {
        assert_eq!(
            DocumentFormat::detect_upload("fake.docx", b"not a zip"),
            None
        );
        assert_eq!(
            DocumentFormat::detect_upload("bad.txt", &[0xff, 0xfe]),
            None
        );
        assert_eq!(DocumentFormat::detect_upload("bad.txt", b"has\0nul"), None);
        assert_eq!(DocumentFormat::detect_upload("empty.md", b""), None);
        assert_eq!(
            DocumentFormat::detect_upload("unknown.bin", b"valid UTF-8"),
            None
        );
        assert_eq!(
            DocumentFormat::detect_upload("broken.pdf", b"%PDF-not-structural"),
            None
        );
    }

    #[test]
    fn docx_requires_ooxml_entries_and_extracts_text_structure() {
        assert!(!is_parseable_docx(&zip_fixture(&[(
            "word/document.xml",
            "<w:document/>"
        )])));

        let bytes = docx_fixture("A &amp; B");
        assert!(is_parseable_docx(&bytes));
        assert_eq!(extract_docx_text(&bytes).unwrap(), "A & B");
    }

    #[test]
    fn text_decode_preserves_markdown_markup() {
        let markdown = b"# Heading\n\n**bold**";
        assert_eq!(decode_text(markdown).unwrap(), "# Heading\n\n**bold**");
    }

    fn minimal_pdf() -> Vec<u8> {
        let mut document = Document::with_version("1.4");
        let pages_id = document.new_object_id();
        let page_id = document.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
        });
        document.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages",
                "Kids" => vec![Object::Reference(page_id)],
                "Count" => 1,
            }),
        );
        let catalog_id = document.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        document.trailer.set("Root", catalog_id);

        let mut bytes = Vec::new();
        document.save_to(&mut bytes).unwrap();
        bytes
    }

    fn docx_fixture(text: &str) -> Vec<u8> {
        let document = format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:body>
</w:document>"#
        );
        zip_fixture(&[
            ("[Content_Types].xml", "<Types/>"),
            ("word/document.xml", &document),
        ])
    }

    fn zip_fixture(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = ZipWriter::new(&mut output);
            let options = SimpleFileOptions::default();
            for (name, contents) in entries {
                writer.start_file(*name, options).unwrap();
                writer.write_all(contents.as_bytes()).unwrap();
            }
            writer.finish().unwrap();
        }
        output.into_inner()
    }
}
