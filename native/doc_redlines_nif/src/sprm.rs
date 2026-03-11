use crate::dttm::Dttm;
use crate::model::ChpxRun;
use std::sync::OnceLock;

pub const SPRM_FRMARK_DEL: u16 = 0x0800;
pub const SPRM_FRMARK: u16 = 0x0801;
pub const SPRM_IBST_RMARK: u16 = 0x4804;
pub const SPRM_DTTM_RMARK: u16 = 0x6805;
pub const SPRM_IBST_RMARK_DEL_WW8: u16 = 0x4863;
pub const SPRM_DTTM_RMARK_DEL_WW8: u16 = 0x6864;
pub const SPRM_IBST_RMARK_DEL_LEGACY: u16 = 0x4806;
pub const SPRM_DTTM_RMARK_DEL_LEGACY: u16 = 0x6807;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToggleOp {
    Set(bool),
    UseStyle,
    InvertStyle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionSprms {
    pub insertion_toggle: Option<ToggleOp>,
    pub deletion_toggle: Option<ToggleOp>,
    pub has_insertion: bool,
    pub has_deletion: bool,
    pub insertion_author: Option<u16>,
    pub insertion_timestamp: Option<Dttm>,
    pub deletion_author: Option<u16>,
    pub deletion_timestamp: Option<Dttm>,
    pub insertion_sprm_index: Option<usize>,
    pub deletion_sprm_index: Option<usize>,
    pub formatting_fingerprint: u64,
    pub formatting_sequence_fingerprint: u64,
}

pub fn collect_revision_sprms(run: &ChpxRun) -> RevisionSprms {
    let mut insertion_toggle: Option<ToggleOp> = None;
    let mut deletion_toggle: Option<ToggleOp> = None;
    let mut insertion_author = None;
    let mut insertion_timestamp = None;
    let mut deletion_author = None;
    let mut deletion_timestamp = None;
    let mut insertion_sprm_index = None;
    let mut deletion_sprm_index = None;

    // Collect formatting SPRMs for both fingerprints:
    // - sequence fingerprint: preserves grpprl order
    // - set fingerprint: order-independent
    let mut fmt_sprms: Vec<(u16, &[u8])> = Vec::new();

    for (idx, sprm) in run.sprms.iter().enumerate() {
        match sprm.opcode {
            SPRM_FRMARK => {
                insertion_toggle = Some(parse_toggle_operand(&sprm.operand));
                if insertion_sprm_index.is_none() {
                    insertion_sprm_index = Some(idx);
                }
            }
            SPRM_FRMARK_DEL => {
                deletion_toggle = Some(parse_toggle_operand(&sprm.operand));
                if deletion_sprm_index.is_none() {
                    deletion_sprm_index = Some(idx);
                }
            }
            SPRM_IBST_RMARK => {
                insertion_author = parse_u16(&sprm.operand);
            }
            SPRM_DTTM_RMARK => {
                insertion_timestamp = parse_u32(&sprm.operand).and_then(Dttm::from_raw);
            }
            SPRM_IBST_RMARK_DEL_WW8 | SPRM_IBST_RMARK_DEL_LEGACY => {
                deletion_author = parse_u16(&sprm.operand);
            }
            SPRM_DTTM_RMARK_DEL_WW8 | SPRM_DTTM_RMARK_DEL_LEGACY => {
                deletion_timestamp = parse_u32(&sprm.operand).and_then(Dttm::from_raw);
            }
            0xCA89 | 0x2A83 | 0x0868 => {
                fmt_sprms.push((sprm.opcode, &sprm.operand));
            }
            0x6815 | 0x6816 | 0x6817 => {
                if fingerprint_include_rsid() {
                    fmt_sprms.push((sprm.opcode, &sprm.operand));
                }
            }
            _ => {
                fmt_sprms.push((sprm.opcode, &sprm.operand));
            }
        }
    }

    let mut seq_hash = 0xcbf29ce484222325_u64;
    for (opcode, operand) in &fmt_sprms {
        fnv_update(&mut seq_hash, &opcode.to_le_bytes());
        fnv_update(&mut seq_hash, &[0xFF]);
        fnv_update(&mut seq_hash, operand);
        fnv_update(&mut seq_hash, &[0x00]);
    }

    // Sort by opcode then operand for deterministic order-insensitive hash.
    fmt_sprms.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));

    let mut set_hash = 0xcbf29ce484222325_u64;
    for (opcode, operand) in &fmt_sprms {
        fnv_update(&mut set_hash, &opcode.to_le_bytes());
        fnv_update(&mut set_hash, &[0xFF]);
        fnv_update(&mut set_hash, operand);
        fnv_update(&mut set_hash, &[0x00]);
    }

    let has_insertion = matches!(
        insertion_toggle,
        Some(ToggleOp::Set(true)) | Some(ToggleOp::InvertStyle)
    );
    let has_deletion = matches!(
        deletion_toggle,
        Some(ToggleOp::Set(true)) | Some(ToggleOp::InvertStyle)
    );

    RevisionSprms {
        insertion_toggle,
        deletion_toggle,
        has_insertion,
        has_deletion,
        insertion_author,
        insertion_timestamp,
        deletion_author,
        deletion_timestamp,
        insertion_sprm_index,
        deletion_sprm_index,
        formatting_fingerprint: set_hash,
        formatting_sequence_fingerprint: seq_hash,
    }
}

fn fingerprint_include_rsid() -> bool {
    static VALUE: OnceLock<bool> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_FINGERPRINT_INCLUDE_RSID")
            .ok()
            .as_deref()
            .is_some_and(|value| value == "1")
    })
}

fn parse_toggle_operand(operand: &[u8]) -> ToggleOp {
    match operand.first().copied().unwrap_or(0) {
        0x00 => ToggleOp::Set(false),
        0x01 => ToggleOp::Set(true),
        // MS-DOC toggle operands: 0x80 = "use style value", 0x81 = "NOT style value".
        0x80 => ToggleOp::UseStyle,
        0x81 => ToggleOp::InvertStyle,
        value => ToggleOp::Set(value != 0),
    }
}

fn parse_u16(operand: &[u8]) -> Option<u16> {
    if operand.len() < 2 {
        None
    } else {
        Some(u16::from_le_bytes([operand[0], operand[1]]))
    }
}

fn parse_u32(operand: &[u8]) -> Option<u32> {
    if operand.len() < 4 {
        None
    } else {
        Some(u32::from_le_bytes([
            operand[0], operand[1], operand[2], operand[3],
        ]))
    }
}

fn fnv_update(state: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *state ^= *byte as u64;
        *state = state.wrapping_mul(0x100000001b3);
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{ChpxRun, Sprm};

    use super::{
        SPRM_DTTM_RMARK, SPRM_FRMARK, SPRM_FRMARK_DEL, SPRM_IBST_RMARK, collect_revision_sprms,
    };

    #[test]
    fn captures_revision_flags_and_metadata() {
        let run = ChpxRun {
            start_cp: 0,
            end_cp: 3,
            text: "abc".to_string(),
            sprms: vec![
                Sprm {
                    opcode: SPRM_FRMARK,
                    operand: vec![1],
                },
                Sprm {
                    opcode: SPRM_FRMARK_DEL,
                    operand: vec![0],
                },
                Sprm {
                    opcode: SPRM_IBST_RMARK,
                    operand: vec![2, 0],
                },
                Sprm {
                    opcode: SPRM_DTTM_RMARK,
                    operand: vec![1, 2, 3, 4],
                },
            ],
            source_chpx_id: None,
        };

        let got = collect_revision_sprms(&run);
        assert!(got.has_insertion);
        assert!(!got.has_deletion);
        assert_eq!(got.insertion_author, Some(2));
        assert!(got.insertion_timestamp.is_some());
    }

    #[test]
    fn includes_cfdata_in_formatting_fingerprint() {
        let baseline = ChpxRun {
            start_cp: 0,
            end_cp: 3,
            text: "abc".to_string(),
            sprms: vec![
                Sprm {
                    opcode: SPRM_FRMARK,
                    operand: vec![1],
                },
                Sprm {
                    opcode: SPRM_IBST_RMARK,
                    operand: vec![2, 0],
                },
            ],
            source_chpx_id: None,
        };

        let mut with_cfdata = baseline.clone();
        with_cfdata.sprms.push(Sprm {
            opcode: 0x0868,
            operand: vec![1],
        });

        let left = collect_revision_sprms(&baseline);
        let right = collect_revision_sprms(&with_cfdata);
        assert_ne!(left.formatting_fingerprint, right.formatting_fingerprint);
    }
}
