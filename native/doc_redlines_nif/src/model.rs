use serde::{Deserialize, Serialize};

use crate::dttm::Dttm;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RevisionType {
    Insertion,
    Deletion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sprm {
    pub opcode: u16,
    #[serde(default)]
    pub operand: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChpxRun {
    pub start_cp: u32,
    pub end_cp: u32,
    pub text: String,
    #[serde(default)]
    pub sprms: Vec<Sprm>,
    #[serde(default)]
    pub source_chpx_id: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bookmark {
    pub name: String,
    pub start_cp: u32,
    pub end_cp: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedDocument {
    #[serde(default)]
    pub runs: Vec<ChpxRun>,
    #[serde(default)]
    pub authors: Vec<String>,
    #[serde(default)]
    pub bookmarks: Vec<Bookmark>,
    #[serde(default)]
    pub style_defaults: StyleDefaults,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StyleDefaults {
    pub insertion_active: bool,
    pub deletion_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionMetadata {
    pub author_index: Option<u16>,
    pub timestamp: Option<Dttm>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackMetadata {
    pub revision_type: RevisionType,
    pub author_index: Option<u16>,
    pub timestamp: Option<Dttm>,
    pub next: Option<Box<StackMetadata>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedlineSignature {
    pub revision_type: RevisionType,
    pub author_index: Option<u16>,
    pub timestamp: Option<Dttm>,
    pub stack: Option<Box<StackMetadata>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSegment {
    pub start_cp: u32,
    pub end_cp: u32,
    pub text: String,
    pub formatting_fingerprint: u64,
    pub formatting_sequence_fingerprint: u64,
    pub source_chpx_id: Option<u32>,
    pub segment_author_index: Option<u16>,
    pub segment_timestamp: Option<Dttm>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltRedline {
    pub signature: RedlineSignature,
    pub start_cp: u32,
    pub end_cp: u32,
    pub segments: Vec<SourceSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionEntry {
    #[serde(rename = "type")]
    pub revision_type: RevisionType,
    pub text: String,
    pub author: Option<String>,
    pub timestamp: Option<String>,
    pub start_cp: u32,
    pub end_cp: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paragraph_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub char_offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}
