pub mod combine;
pub mod doc_parser;
pub mod dttm;
pub mod model;
pub mod normalize;
pub mod pipeline;
pub mod splitter;
pub mod sprm;

pub use doc_parser::{parse_doc_file, DocParseError};
pub use model::{ParsedDocument, RevisionEntry, RevisionType};
pub use pipeline::extract_revisions;

use rustler::{Atom, NifStruct};

mod atoms {
    rustler::atoms! {
        insertion,
        deletion
    }
}

#[derive(Debug, Default, Clone)]
pub struct Extractor;

impl Extractor {
    pub fn extract(&self, document: &ParsedDocument) -> Vec<RevisionEntry> {
        extract_revisions(document)
    }
}

pub fn extract_revisions_from_doc(
    path: &std::path::Path,
) -> Result<Vec<RevisionEntry>, DocParseError> {
    let parsed = parse_doc_file(path)?;
    Ok(extract_revisions(&parsed))
}

#[derive(NifStruct)]
#[module = "DocRedlines.Redline"]
struct NifRedline {
    #[rustler(rename = "type")]
    r#type: Atom,
    text: String,
    author: Option<String>,
    timestamp: Option<String>,
    start_cp: u32,
    end_cp: u32,
    paragraph_index: Option<u32>,
    char_offset: Option<u32>,
    context: Option<String>,
}

#[derive(NifStruct)]
#[module = "DocRedlines.Result"]
struct NifResult {
    redlines: Vec<NifRedline>,
}

fn to_atom_type(revision_type: RevisionType) -> Atom {
    match revision_type {
        RevisionType::Insertion => atoms::insertion(),
        RevisionType::Deletion => atoms::deletion(),
    }
}

fn to_nif_redline(entry: RevisionEntry) -> NifRedline {
    NifRedline {
        r#type: to_atom_type(entry.revision_type),
        text: entry.text,
        author: entry.author,
        timestamp: entry.timestamp,
        start_cp: entry.start_cp,
        end_cp: entry.end_cp,
        paragraph_index: entry.paragraph_index,
        char_offset: entry.char_offset,
        context: entry.context,
    }
}

#[rustler::nif]
fn extract_redlines_from_path(path: String) -> Result<NifResult, String> {
    let revisions = extract_revisions_from_doc(std::path::Path::new(&path))
        .map_err(|err| err.to_string())?;
    let redlines = revisions.into_iter().map(to_nif_redline).collect();
    Ok(NifResult { redlines })
}

rustler::init!("Elixir.DocRedlines.Native");
