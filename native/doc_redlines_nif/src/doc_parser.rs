use std::char;
use std::env;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::path::Path;

use cfb::CompoundFile;
use encoding_rs::WINDOWS_1252;
use thiserror::Error;

use crate::model::{Bookmark, ChpxRun, ParsedDocument, Sprm, StyleDefaults};

const FIB_BASE_LEN: usize = 32;
const DOC_PAGE_SIZE: usize = 512;

const PAIR_PLCF_BTE_CHPX: usize = 12;
const PAIR_STTBF_BKMK: usize = 21;
const PAIR_PLCF_BKF: usize = 22;
const PAIR_PLCF_BKL: usize = 23;
const PAIR_PLCF_ATNBKF: usize = 42;
const PAIR_PLCF_ATNBKL: usize = 43;
const PAIR_CLX: usize = 33;
const PAIR_STSHF: usize = 0;
const PAIR_STTBF_RMARK: usize = 51;

#[derive(Debug, Error)]
pub enum DocParseError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid FIB: {0}")]
    InvalidFib(String),
    #[error("missing required stream: {0}")]
    MissingStream(String),
    #[error("corrupt table range {label}: fc={fc} lcb={lcb}")]
    InvalidTableRange {
        label: &'static str,
        fc: u32,
        lcb: u32,
    },
    #[error("unsupported CLX structure")]
    InvalidClx,
}

#[derive(Debug, Clone)]
struct FibInfo {
    table_stream: &'static str,
    ccp_text: u32,
    ccp_ftn: u32,
    ccp_hdr: u32,
    ccp_mcr: u32,
    ccp_atn: u32,
    ccp_edn: u32,
    ccp_txbx: u32,
    ccp_hdr_txbx: u32,
    fc_lcb_pairs: Vec<(u32, u32)>,
}

impl FibInfo {
    fn pair(&self, index: usize) -> Option<(u32, u32)> {
        self.fc_lcb_pairs.get(index).copied()
    }
}

#[derive(Debug, Clone)]
struct Piece {
    cp_start: u32,
    cp_end: u32,
    byte_start: u32,
    compressed: bool,
    prm_sprms: Vec<Sprm>,
}

#[derive(Debug, Clone)]
pub struct FcPieceProbe {
    pub table_stream: &'static str,
    pub ccp_text: u32,
    pub cp_limit: u32,
    pub fc: u32,
    pub cp: Option<u32>,
    pub piece_count: usize,
    pub hit_piece: Option<FcPieceProbeHit>,
}

#[derive(Debug, Clone)]
pub struct FcPieceProbeHit {
    pub cp_start: u32,
    pub cp_end: u32,
    pub byte_start: u32,
    pub byte_end: u32,
    pub compressed: bool,
}

#[derive(Debug, Clone)]
struct ClxParsed {
    plcpcd: Vec<u8>,
    prm_entries: Vec<PrmEntry>,
    prm_heap: Vec<u8>,
}

#[derive(Debug, Clone)]
struct PrmEntry {
    cb_offset: usize,
    grpprl_offset: usize,
    grpprl: Vec<u8>,
}

#[derive(Debug, Clone)]
struct FkpRun {
    start_fc: u32,
    end_fc: u32,
    grpprl: Vec<u8>,
}

pub fn parse_doc_file(path: &Path) -> Result<ParsedDocument, DocParseError> {
    let file = File::open(path)?;
    let mut container = CompoundFile::open(file)?;

    let word_document = read_stream(&mut container, "WordDocument")?;
    let fib = parse_fib(&word_document)?;
    let table_stream = read_stream(&mut container, fib.table_stream)?;

    let pieces = parse_piece_table(&table_stream, &fib, &word_document)?;
    let cp_limit = cp_limit_for_decode(&pieces, fib.ccp_text);
    let main_text = decode_main_text(&word_document, &pieces, cp_limit);

    let runs = parse_chpx_runs(
        &word_document,
        &table_stream,
        &fib,
        &pieces,
        &main_text,
        cp_limit,
    )?;

    // Keep the main story plus text box stories. LibreOffice exports tracked
    // changes from ccpTxbx / ccpHdrTxbx, but we still exclude footnotes,
    // headers, annotations, macros, and endnotes to avoid non-body regressions.
    let story_ranges = included_story_ranges(&fib, &pieces);
    let runs = filter_runs_to_story_ranges(runs, &story_ranges);

    let authors = parse_authors(&table_stream, &fib)?;
    let mut bookmarks = parse_bookmarks(&table_stream, &fib)?;
    bookmarks.extend(parse_annotation_mark_bookmarks(&table_stream, &fib)?);
    bookmarks.sort_by(|left, right| {
        (left.start_cp, left.end_cp, left.name.as_str()).cmp(&(
            right.start_cp,
            right.end_cp,
            right.name.as_str(),
        ))
    });

    let style_defaults = parse_style_defaults(&table_stream, &fib)?;

    Ok(ParsedDocument {
        runs,
        authors,
        bookmarks,
        style_defaults,
    })
}

pub fn probe_fc(path: &Path, fc: u32) -> Result<FcPieceProbe, DocParseError> {
    let file = File::open(path)?;
    let mut container = CompoundFile::open(file)?;

    let word_document = read_stream(&mut container, "WordDocument")?;
    let fib = parse_fib(&word_document)?;
    let table_stream = read_stream(&mut container, fib.table_stream)?;
    let pieces = parse_piece_table(&table_stream, &fib, &word_document)?;
    let cp_limit = cp_limit_for_decode(&pieces, fib.ccp_text);
    let cp = fc_to_cp(fc, &pieces);

    let hit_piece = pieces.iter().find_map(|piece| {
        let cp_len = piece.cp_end.saturating_sub(piece.cp_start);
        let stride = if piece.compressed { 1 } else { 2 };
        let byte_end = piece
            .byte_start
            .saturating_add(cp_len.saturating_mul(stride));
        if fc >= piece.byte_start && fc < byte_end {
            Some(FcPieceProbeHit {
                cp_start: piece.cp_start,
                cp_end: piece.cp_end,
                byte_start: piece.byte_start,
                byte_end,
                compressed: piece.compressed,
            })
        } else {
            None
        }
    });

    Ok(FcPieceProbe {
        table_stream: fib.table_stream,
        ccp_text: fib.ccp_text,
        cp_limit,
        fc,
        cp,
        piece_count: pieces.len(),
        hit_piece,
    })
}

fn read_stream<F: Read + Seek>(
    container: &mut CompoundFile<F>,
    name: &str,
) -> Result<Vec<u8>, DocParseError> {
    let mut stream = match container.open_stream(name) {
        Ok(stream) => stream,
        Err(_) => container
            .open_stream(&format!("/{name}"))
            .map_err(|_| DocParseError::MissingStream(name.to_string()))?,
    };

    let mut out = Vec::new();
    stream.read_to_end(&mut out)?;
    Ok(out)
}

fn parse_fib(word_document: &[u8]) -> Result<FibInfo, DocParseError> {
    if word_document.len() < FIB_BASE_LEN {
        return Err(DocParseError::InvalidFib(
            "WordDocument stream too short".to_string(),
        ));
    }

    let flags = read_u16(word_document, 10)?;
    let f_which_tbl_stream = (flags & (1 << 9)) != 0;
    let table_stream = if f_which_tbl_stream {
        "1Table"
    } else {
        "0Table"
    };

    let mut cursor = FIB_BASE_LEN;

    let csw = read_u16(word_document, cursor)? as usize;
    cursor = cursor.saturating_add(2 + csw.saturating_mul(2));

    let cslw = read_u16(word_document, cursor)? as usize;
    cursor += 2;
    if cslw < 4 {
        return Err(DocParseError::InvalidFib(
            "FibRgLw97 has fewer than 4 fields; cannot read ccpText".to_string(),
        ));
    }
    let ccp_text = read_u32(word_document, cursor + 3 * 4)?;
    let fib_lw_value = |index: usize| -> Result<u32, DocParseError> {
        if index < cslw {
            read_u32(word_document, cursor + index * 4)
        } else {
            Ok(0)
        }
    };
    let ccp_ftn = fib_lw_value(4)?;
    let ccp_hdr = fib_lw_value(5)?;
    let ccp_mcr = fib_lw_value(6)?;
    let ccp_atn = fib_lw_value(7)?;
    let ccp_edn = fib_lw_value(8)?;
    let ccp_txbx = fib_lw_value(9)?;
    let ccp_hdr_txbx = fib_lw_value(10)?;
    cursor = cursor.saturating_add(cslw.saturating_mul(4));

    let cb_rg_fc_lcb = read_u16(word_document, cursor)? as usize;
    cursor += 2;

    let mut fc_lcb_pairs = Vec::with_capacity(cb_rg_fc_lcb);
    for i in 0..cb_rg_fc_lcb {
        let base = cursor + i * 8;
        let fc = read_u32(word_document, base)?;
        let lcb = read_u32(word_document, base + 4)?;
        fc_lcb_pairs.push((fc, lcb));
    }

    Ok(FibInfo {
        table_stream,
        ccp_text,
        ccp_ftn,
        ccp_hdr,
        ccp_mcr,
        ccp_atn,
        ccp_edn,
        ccp_txbx,
        ccp_hdr_txbx,
        fc_lcb_pairs,
    })
}

fn parse_piece_table(
    table_stream: &[u8],
    fib: &FibInfo,
    word_document: &[u8],
) -> Result<Vec<Piece>, DocParseError> {
    let (fc_clx, lcb_clx) = fib
        .pair(PAIR_CLX)
        .ok_or_else(|| DocParseError::InvalidFib("fcClx/lcbClx missing".to_string()))?;

    if lcb_clx == 0 {
        return Err(DocParseError::InvalidFib(
            "CLX stream is empty; cannot map CP to FC".to_string(),
        ));
    }

    let clx = slice_table_range(table_stream, fc_clx, lcb_clx, "CLX")?;
    let parsed = parse_clx(clx)?;
    parse_plcpcd(
        &parsed.plcpcd,
        &parsed.prm_entries,
        &parsed.prm_heap,
        word_document,
    )
}

fn parse_clx(clx: &[u8]) -> Result<ClxParsed, DocParseError> {
    let mut cursor = 0;
    let mut prm_entries = Vec::<PrmEntry>::new();
    let mut prm_heap = Vec::<u8>::new();

    while cursor < clx.len() {
        let clxt = clx[cursor];
        cursor += 1;

        match clxt {
            0x01 => {
                if cursor + 2 > clx.len() {
                    return Err(DocParseError::InvalidClx);
                }
                let cb_offset = cursor;
                let cb_grpprl = u16::from_le_bytes([clx[cursor], clx[cursor + 1]]) as usize;
                let grpprl_offset = cursor + 2;
                cursor += 2;
                cursor = cursor.saturating_add(cb_grpprl);
                if cursor > clx.len() {
                    return Err(DocParseError::InvalidClx);
                }

                let start = cursor - cb_grpprl;
                let end = cursor;
                let grpprl = clx[start..end].to_vec();
                prm_heap.extend_from_slice(&(cb_grpprl as u16).to_le_bytes());
                prm_heap.extend_from_slice(&grpprl);
                prm_entries.push(PrmEntry {
                    cb_offset,
                    grpprl_offset,
                    grpprl,
                });
            }
            0x02 => {
                if cursor + 4 > clx.len() {
                    return Err(DocParseError::InvalidClx);
                }
                let lcb = u32::from_le_bytes([
                    clx[cursor],
                    clx[cursor + 1],
                    clx[cursor + 2],
                    clx[cursor + 3],
                ]) as usize;
                cursor += 4;

                if cursor + lcb > clx.len() {
                    return Err(DocParseError::InvalidClx);
                }
                return Ok(ClxParsed {
                    plcpcd: clx[cursor..cursor + lcb].to_vec(),
                    prm_entries,
                    prm_heap,
                });
            }
            _ => return Err(DocParseError::InvalidClx),
        }
    }

    Err(DocParseError::InvalidClx)
}

fn parse_plcpcd(
    plcpcd: &[u8],
    prm_entries: &[PrmEntry],
    prm_heap: &[u8],
    _word_document: &[u8],
) -> Result<Vec<Piece>, DocParseError> {
    if plcpcd.len() < 4 || (plcpcd.len() - 4) % 12 != 0 {
        return Err(DocParseError::InvalidClx);
    }

    let piece_count = (plcpcd.len() - 4) / 12;
    let cp_count = piece_count + 1;
    let cp_table_bytes = cp_count * 4;
    if cp_table_bytes + piece_count * 8 != plcpcd.len() {
        return Err(DocParseError::InvalidClx);
    }

    let mut pieces = Vec::with_capacity(piece_count);
    let debug_prm = env::var("DOC_RL_DEBUG_PRM")
        .ok()
        .as_deref()
        .is_some_and(|value| value == "1");
    let mut nonzero_prm = 0usize;
    let mut decoded_prm = 0usize;

    for i in 0..piece_count {
        let cp_start = u32::from_le_bytes([
            plcpcd[i * 4],
            plcpcd[i * 4 + 1],
            plcpcd[i * 4 + 2],
            plcpcd[i * 4 + 3],
        ]);
        let cp_end = u32::from_le_bytes([
            plcpcd[(i + 1) * 4],
            plcpcd[(i + 1) * 4 + 1],
            plcpcd[(i + 1) * 4 + 2],
            plcpcd[(i + 1) * 4 + 3],
        ]);

        if cp_end <= cp_start {
            continue;
        }

        let pcd_offset = cp_table_bytes + i * 8;
        let raw_fc = u32::from_le_bytes([
            plcpcd[pcd_offset + 2],
            plcpcd[pcd_offset + 3],
            plcpcd[pcd_offset + 4],
            plcpcd[pcd_offset + 5],
        ]);
        let prm = u16::from_le_bytes([plcpcd[pcd_offset + 6], plcpcd[pcd_offset + 7]]);

        let compressed = ((raw_fc >> 30) & 0x1) == 1;
        let fc = raw_fc & 0x3FFF_FFFF;
        let byte_start = if compressed { fc / 2 } else { fc };

        let prm_sprms = decode_piece_prm(prm, prm_entries, prm_heap);
        if prm != 0 {
            nonzero_prm += 1;
            if !prm_sprms.is_empty() {
                decoded_prm += 1;
            }
        }

        pieces.push(Piece {
            cp_start,
            cp_end,
            byte_start,
            compressed,
            prm_sprms,
        });
    }

    pieces.sort_by_key(|piece| (piece.cp_start, piece.cp_end));
    if debug_prm {
        eprintln!(
            "debug_prm: pieces={} nonzero_prm={} decoded_prm={}",
            pieces.len(),
            nonzero_prm,
            decoded_prm
        );
    }
    Ok(pieces)
}

fn decode_main_text(word_document: &[u8], pieces: &[Piece], ccp_text: u32) -> Vec<char> {
    let mut out = vec!['\0'; ccp_text as usize];

    for piece in pieces {
        if piece.cp_start >= ccp_text {
            continue;
        }

        let cp_start = piece.cp_start;
        let cp_end = piece.cp_end.min(ccp_text);
        if cp_start >= cp_end {
            continue;
        }

        let cp_len = (cp_end - cp_start) as usize;
        if cp_len == 0 {
            continue;
        }

        if piece.compressed {
            let byte_start = piece.byte_start as usize;
            if byte_start >= word_document.len() {
                continue;
            }
            let max_len = word_document.len().saturating_sub(byte_start);
            let cp_len = cp_len.min(max_len);
            if cp_len == 0 {
                continue;
            }
            let byte_end = byte_start + cp_len;
            let bytes = &word_document[byte_start..byte_end];
            let (decoded, _, _) = WINDOWS_1252.decode(bytes);
            for (idx, ch) in decoded.chars().take(cp_len).enumerate() {
                out[cp_start as usize + idx] = ch;
            }
        } else {
            let byte_start = piece.byte_start as usize;
            if byte_start >= word_document.len() {
                continue;
            }
            let max_units = word_document.len().saturating_sub(byte_start) / 2;
            let cp_len = cp_len.min(max_units);
            if cp_len == 0 {
                continue;
            }
            let byte_end = byte_start + cp_len * 2;
            let bytes = &word_document[byte_start..byte_end];
            for idx in 0..cp_len {
                let base = idx * 2;
                let unit = u16::from_le_bytes([bytes[base], bytes[base + 1]]);
                out[cp_start as usize + idx] = decode_utf16_unit(unit);
            }
        }
    }

    out
}

fn cp_limit_for_decode(pieces: &[Piece], ccp_text: u32) -> u32 {
    pieces
        .iter()
        .map(|piece| piece.cp_end)
        .max()
        .unwrap_or(ccp_text)
        .max(ccp_text)
}

fn main_story_cp_limit(pieces: &[Piece], ccp_text: u32) -> u32 {
    let slack = ccp_tail_slack();
    if slack == 0 {
        return ccp_text;
    }

    let max_cp = pieces
        .iter()
        .map(|piece| piece.cp_end)
        .max()
        .unwrap_or(ccp_text);
    let overflow = max_cp.saturating_sub(ccp_text);
    if overflow > 0 && overflow <= slack {
        max_cp
    } else {
        ccp_text
    }
}

fn included_story_ranges(fib: &FibInfo, pieces: &[Piece]) -> Vec<(u32, u32)> {
    let mut ranges = Vec::with_capacity(3);

    let main_end = main_story_range_end(fib, pieces);
    if main_end > 0 {
        ranges.push((0, main_end));
    }

    let text_box_start = fib
        .ccp_text
        .saturating_add(fib.ccp_ftn)
        .saturating_add(fib.ccp_hdr)
        .saturating_add(fib.ccp_mcr)
        .saturating_add(fib.ccp_atn)
        .saturating_add(fib.ccp_edn);
    let text_box_end = text_box_start.saturating_add(fib.ccp_txbx);
    if text_box_end > text_box_start {
        ranges.push((text_box_start, text_box_end));
    }

    let header_text_box_end = text_box_end.saturating_add(fib.ccp_hdr_txbx);
    if header_text_box_end > text_box_end {
        ranges.push((text_box_end, header_text_box_end));
    }

    ranges
}

fn main_story_range_end(fib: &FibInfo, pieces: &[Piece]) -> u32 {
    if fib.ccp_ftn != 0
        || fib.ccp_hdr != 0
        || fib.ccp_mcr != 0
        || fib.ccp_atn != 0
        || fib.ccp_edn != 0
        || fib.ccp_txbx != 0
        || fib.ccp_hdr_txbx != 0
    {
        fib.ccp_text
    } else {
        main_story_cp_limit(pieces, fib.ccp_text)
    }
}

fn filter_runs_to_story_ranges(runs: Vec<ChpxRun>, ranges: &[(u32, u32)]) -> Vec<ChpxRun> {
    if ranges.is_empty() {
        return Vec::new();
    }

    let mut filtered = Vec::with_capacity(runs.len());
    for run in runs {
        for &(range_start, range_end) in ranges {
            if run.end_cp <= range_start || run.start_cp >= range_end {
                continue;
            }

            let start_cp = run.start_cp.max(range_start);
            let end_cp = run.end_cp.min(range_end);
            if end_cp <= start_cp {
                continue;
            }

            let start_offset = start_cp.saturating_sub(run.start_cp) as usize;
            let len = end_cp.saturating_sub(start_cp) as usize;
            let text = run.text.chars().skip(start_offset).take(len).collect();

            filtered.push(ChpxRun {
                start_cp,
                end_cp,
                text,
                sprms: run.sprms.clone(),
                source_chpx_id: run.source_chpx_id,
            });
        }
    }

    filtered
}

fn ccp_tail_slack() -> u32 {
    std::env::var("DOC_RL_CCP_TAIL_SLACK")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(512)
}

fn parse_chpx_runs(
    word_document: &[u8],
    table_stream: &[u8],
    fib: &FibInfo,
    pieces: &[Piece],
    text_chars: &[char],
    cp_limit: u32,
) -> Result<Vec<ChpxRun>, DocParseError> {
    let Some((fc_plc, lcb_plc)) = fib.pair(PAIR_PLCF_BTE_CHPX) else {
        return Ok(Vec::new());
    };
    if lcb_plc == 0 {
        return Ok(Vec::new());
    }

    let plcf = slice_table_range(table_stream, fc_plc, lcb_plc, "PlcfBteChpx")?;
    if plcf.len() < 4 || (plcf.len() - 4) % 8 != 0 {
        return Ok(Vec::new());
    }

    let entry_count = (plcf.len() - 4) / 8;
    let fc_count = entry_count + 1;
    let fc_table_bytes = fc_count * 4;
    let mut pieces_by_file = pieces.to_vec();
    pieces_by_file.sort_by_key(|piece| (piece.byte_start, piece.cp_start, piece.cp_end));

    let mut runs = Vec::<ChpxRun>::new();
    let mut next_source_chpx_id = 0_u32;

    for i in 0..entry_count {
        let pn_raw = u32::from_le_bytes([
            plcf[fc_table_bytes + i * 4],
            plcf[fc_table_bytes + i * 4 + 1],
            plcf[fc_table_bytes + i * 4 + 2],
            plcf[fc_table_bytes + i * 4 + 3],
        ]);
        let pn = pn_raw & 0x003F_FFFF;

        let Some(page) = read_page(word_document, pn) else {
            continue;
        };
        let fkp_runs = parse_fkp_runs(page);

        for run in fkp_runs {
            let source_chpx_id = next_source_chpx_id;
            next_source_chpx_id = next_source_chpx_id.saturating_add(1);
            let cp_ranges = byte_ranges_to_cp_ranges(&pieces_by_file, run.start_fc, run.end_fc);
            if cp_ranges.is_empty() {
                continue;
            }

            let base_sprms = parse_grpprl(&run.grpprl);

            for (start_cp, end_cp) in cp_ranges {
                if start_cp >= end_cp || start_cp >= cp_limit {
                    continue;
                }

                let clamped_end = end_cp.min(cp_limit);
                if start_cp >= clamped_end {
                    continue;
                }

                let ranges = if piece_boundary_split_enabled() {
                    split_range_by_piece_boundaries(start_cp, clamped_end, pieces)
                } else {
                    vec![(start_cp, clamped_end)]
                };

                for (part_start, part_end) in ranges {
                    if part_start >= part_end {
                        continue;
                    }

                    let mut effective_sprms = base_sprms.clone();
                    if apply_piece_prm_overlay_enabled()
                        && let Some(piece) = piece_for_cp(part_start, pieces)
                    {
                        effective_sprms.extend(piece.prm_sprms.clone());
                    }

                    runs.push(ChpxRun {
                        start_cp: part_start,
                        end_cp: part_end,
                        text: slice_cp_text(text_chars, part_start, part_end),
                        sprms: effective_sprms,
                        source_chpx_id: Some(source_chpx_id),
                    });
                }
            }
        }
    }

    runs.sort_by_key(|run| (run.start_cp, run.end_cp));
    if dedup_chpx_runs_enabled() {
        runs.dedup_by(|left, right| {
            left.start_cp == right.start_cp
                && left.end_cp == right.end_cp
                && left.text == right.text
                && left.sprms == right.sprms
        });
    }

    if fill_piece_prm_gaps_enabled() {
        fill_piece_prm_gaps(&mut runs, pieces, text_chars, cp_limit);
        runs.sort_by_key(|run| (run.start_cp, run.end_cp));
        if dedup_chpx_runs_enabled() {
            runs.dedup_by(|left, right| {
                left.start_cp == right.start_cp
                    && left.end_cp == right.end_cp
                    && left.text == right.text
                    && left.sprms == right.sprms
            });
        }
    }

    Ok(runs)
}

fn byte_ranges_to_cp_ranges(
    pieces_by_file: &[Piece],
    start_byte_inclusive: u32,
    end_byte_exclusive: u32,
) -> Vec<(u32, u32)> {
    if end_byte_exclusive <= start_byte_inclusive {
        return Vec::new();
    }

    let start_byte = start_byte_inclusive as u64;
    let end_byte = end_byte_exclusive as u64;
    let mut out = Vec::new();

    for piece in pieces_by_file {
        let piece_start = piece.byte_start as u64;
        let stride = if piece.compressed { 1_u64 } else { 2_u64 };
        let piece_cp_len = piece.cp_end.saturating_sub(piece.cp_start) as u64;
        let piece_end = piece_start.saturating_add(piece_cp_len.saturating_mul(stride));

        if end_byte <= piece_start {
            break;
        }
        if start_byte >= piece_end {
            continue;
        }

        let range_start = piece_start.max(start_byte);
        let range_end = piece_end.min(end_byte);
        if range_end <= range_start {
            continue;
        }

        let start_rel = range_start - piece_start;
        let end_rel = range_end - piece_start;

        let cp_start_delta = if cp_start_round_up_enabled() {
            start_rel.div_ceil(stride)
        } else {
            start_rel / stride
        };
        let cp_start = piece.cp_start.saturating_add(cp_start_delta as u32);
        let cp_end = piece
            .cp_start
            .saturating_add(end_rel.div_ceil(stride) as u32)
            .min(piece.cp_end);

        if cp_end > cp_start {
            out.push((cp_start, cp_end));
        }
    }

    out
}

fn piece_boundary_split_enabled() -> bool {
    env::var("DOC_RL_SPLIT_BY_PIECE")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn fill_piece_prm_gaps_enabled() -> bool {
    env::var("DOC_RL_FILL_PIECE_PRM_GAPS")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn apply_piece_prm_overlay_enabled() -> bool {
    env::var("DOC_RL_APPLY_PIECE_PRM_OVERLAY")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn dedup_chpx_runs_enabled() -> bool {
    env::var("DOC_RL_DEDUP_CHPX_RUNS")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn cp_start_round_up_enabled() -> bool {
    env::var("DOC_RL_CP_START_ROUND_UP")
        .ok()
        .as_deref()
        .is_some_and(|value| value == "1")
}

fn fill_piece_prm_gaps(
    runs: &mut Vec<ChpxRun>,
    pieces: &[Piece],
    text_chars: &[char],
    ccp_text: u32,
) {
    let mut additions = Vec::<ChpxRun>::new();

    for piece in pieces {
        if piece.prm_sprms.is_empty() {
            continue;
        }

        let piece_start = piece.cp_start.min(ccp_text);
        let piece_end = piece.cp_end.min(ccp_text);
        if piece_start >= piece_end {
            continue;
        }

        let mut boundaries = vec![piece_start, piece_end];
        for run in runs.iter() {
            let overlap_start = piece_start.max(run.start_cp);
            let overlap_end = piece_end.min(run.end_cp);
            if overlap_start < overlap_end {
                boundaries.push(overlap_start);
                boundaries.push(overlap_end);
            }
        }

        boundaries.sort_unstable();
        boundaries.dedup();

        for pair in boundaries.windows(2) {
            let gap_start = pair[0];
            let gap_end = pair[1];
            if gap_start >= gap_end {
                continue;
            }

            let covered = runs
                .iter()
                .any(|run| run.start_cp <= gap_start && run.end_cp >= gap_end);
            if covered {
                continue;
            }

            additions.push(ChpxRun {
                start_cp: gap_start,
                end_cp: gap_end,
                text: slice_cp_text(text_chars, gap_start, gap_end),
                sprms: piece.prm_sprms.clone(),
                source_chpx_id: None,
            });
        }
    }

    runs.extend(additions);
}

fn decode_piece_prm(prm: u16, prm_entries: &[PrmEntry], prm_heap: &[u8]) -> Vec<Sprm> {
    if prm == 0 {
        return Vec::new();
    }

    // PRM variant 2: nPrm & 1, index into stored grpprl heap.
    if (prm & 1) == 1 {
        let offset = (prm >> 1) as usize;

        if let Some(entry) = prm_entries
            .iter()
            .find(|entry| entry.cb_offset == offset || entry.grpprl_offset == offset)
        {
            return parse_grpprl(&entry.grpprl);
        }

        if offset + 2 > prm_heap.len() {
            return Vec::new();
        }
        let len = u16::from_le_bytes([prm_heap[offset], prm_heap[offset + 1]]) as usize;
        let start = offset + 2;
        let end = start.saturating_add(len);
        if end > prm_heap.len() {
            return Vec::new();
        }
        return parse_grpprl(&prm_heap[start..end]);
    }

    // PRM variant 1: (nPrm & 0xFE) >> 1 indexes short-sprm table; high byte is operand.
    let sprm_idx = ((prm & 0x00FE) >> 1) as u8;
    let operand = ((prm >> 8) & 0x00FF) as u8;

    let opcode = match sprm_idx {
        // Mapped from LO short-PRM table per research notes.
        65 => Some(0x0800), // sprmCFStrikeRM / delete mark
        66 => Some(0x0801), // sprmCFRMark / insert mark
        _ => None,
    };

    opcode
        .map(|opcode| {
            vec![Sprm {
                opcode,
                operand: vec![operand],
            }]
        })
        .unwrap_or_default()
}

fn piece_for_cp(cp: u32, pieces: &[Piece]) -> Option<&Piece> {
    pieces
        .iter()
        .find(|piece| piece.cp_start <= cp && cp < piece.cp_end)
}

fn split_range_by_piece_boundaries(
    start_cp: u32,
    end_cp: u32,
    pieces: &[Piece],
) -> Vec<(u32, u32)> {
    if start_cp >= end_cp {
        return Vec::new();
    }

    let mut points = vec![start_cp, end_cp];
    for piece in pieces {
        if piece.cp_start > start_cp && piece.cp_start < end_cp {
            points.push(piece.cp_start);
        }
        if piece.cp_end > start_cp && piece.cp_end < end_cp {
            points.push(piece.cp_end);
        }
    }

    points.sort_unstable();
    points.dedup();
    points
        .windows(2)
        .filter_map(|pair| {
            let a = pair[0];
            let b = pair[1];
            (a < b).then_some((a, b))
        })
        .collect()
}

fn parse_fkp_runs(page: &[u8]) -> Vec<FkpRun> {
    if page.len() != DOC_PAGE_SIZE {
        return Vec::new();
    }

    let crun = page[DOC_PAGE_SIZE - 1] as usize;
    if crun == 0 {
        return Vec::new();
    }

    let rgfc_bytes = (crun + 1) * 4;
    if rgfc_bytes > DOC_PAGE_SIZE - 1 {
        return Vec::new();
    }

    let rgb_start = rgfc_bytes;
    if rgb_start + crun > DOC_PAGE_SIZE - 1 {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(crun);

    for i in 0..crun {
        let start_fc = u32::from_le_bytes([
            page[i * 4],
            page[i * 4 + 1],
            page[i * 4 + 2],
            page[i * 4 + 3],
        ]);
        let end_fc = u32::from_le_bytes([
            page[(i + 1) * 4],
            page[(i + 1) * 4 + 1],
            page[(i + 1) * 4 + 2],
            page[(i + 1) * 4 + 3],
        ]);
        if start_fc >= end_fc {
            continue;
        }

        let chpx_word_offset = page[rgb_start + i] as usize;
        let grpprl = read_grpprl(page, chpx_word_offset);

        out.push(FkpRun {
            start_fc,
            end_fc,
            grpprl,
        });
    }

    out
}

fn read_page(word_document: &[u8], pn: u32) -> Option<&[u8]> {
    let start = pn as usize * DOC_PAGE_SIZE;
    let end = start.checked_add(DOC_PAGE_SIZE)?;
    if end > word_document.len() {
        return None;
    }
    Some(&word_document[start..end])
}

fn read_grpprl(page: &[u8], chpx_word_offset: usize) -> Vec<u8> {
    if chpx_word_offset == 0 {
        return Vec::new();
    }

    let chpx_byte_offset = chpx_word_offset.saturating_mul(2);
    if chpx_byte_offset >= DOC_PAGE_SIZE - 1 {
        return Vec::new();
    }

    let cb = page[chpx_byte_offset] as usize;
    let start = chpx_byte_offset + 1;
    let end = (start + cb).min(DOC_PAGE_SIZE - 1);
    if start >= end {
        return Vec::new();
    }

    page[start..end].to_vec()
}

fn parse_grpprl(grpprl: &[u8]) -> Vec<Sprm> {
    let mut out = Vec::new();
    let mut offset = 0usize;

    while offset + 1 < grpprl.len() {
        let opcode = u16::from_le_bytes([grpprl[offset], grpprl[offset + 1]]);
        let Some(size) = sprm_total_size(grpprl, offset, opcode) else {
            break;
        };
        if size < 2 || offset + size > grpprl.len() {
            break;
        }

        out.push(Sprm {
            opcode,
            operand: grpprl[offset + 2..offset + size].to_vec(),
        });

        offset += size;
    }

    out
}

fn sprm_total_size(grpprl: &[u8], offset: usize, opcode: u16) -> Option<usize> {
    let size_code = ((opcode & 0xE000) >> 13) as u8;

    match size_code {
        0 | 1 => Some(3),
        2 | 4 | 5 => Some(4),
        3 => Some(6),
        6 => {
            let payload_len = if matches!(opcode, 0xD608 | 0xC615) {
                if offset + 3 >= grpprl.len() {
                    return None;
                }
                u16::from_le_bytes([grpprl[offset + 2], grpprl[offset + 3]]) as usize
            } else {
                *grpprl.get(offset + 2)? as usize
            };
            Some(payload_len + 3)
        }
        7 => Some(5),
        _ => None,
    }
}

fn parse_authors(table_stream: &[u8], fib: &FibInfo) -> Result<Vec<String>, DocParseError> {
    let Some((fc, lcb)) = fib.pair(PAIR_STTBF_RMARK) else {
        return Ok(Vec::new());
    };
    if lcb == 0 {
        return Ok(Vec::new());
    }

    let bytes = slice_table_range(table_stream, fc, lcb, "SttbfRMark")?;
    Ok(parse_sttb(bytes))
}

fn parse_bookmarks(table_stream: &[u8], fib: &FibInfo) -> Result<Vec<Bookmark>, DocParseError> {
    let names = match fib.pair(PAIR_STTBF_BKMK) {
        Some((fc, lcb)) if lcb > 0 => {
            parse_sttb(slice_table_range(table_stream, fc, lcb, "SttbfBkmk")?)
        }
        _ => Vec::new(),
    };

    let Some((fc_bkf, lcb_bkf)) = fib.pair(PAIR_PLCF_BKF) else {
        return Ok(Vec::new());
    };
    let Some((fc_bkl, lcb_bkl)) = fib.pair(PAIR_PLCF_BKL) else {
        return Ok(Vec::new());
    };
    if lcb_bkf == 0 || lcb_bkl == 0 {
        return Ok(Vec::new());
    }

    let bkf = slice_table_range(table_stream, fc_bkf, lcb_bkf, "PlcfBkf")?;
    let bkl = slice_table_range(table_stream, fc_bkl, lcb_bkl, "PlcfBkl")?;

    if bkf.len() < 4 || (bkf.len() - 4) % 8 != 0 || bkl.len() < 8 || bkl.len() % 4 != 0 {
        return Ok(Vec::new());
    }

    let entry_count = (bkf.len() - 4) / 8;
    let cp_count = entry_count + 1;

    let mut start_cps = Vec::with_capacity(cp_count);
    for i in 0..cp_count {
        start_cps.push(u32::from_le_bytes([
            bkf[i * 4],
            bkf[i * 4 + 1],
            bkf[i * 4 + 2],
            bkf[i * 4 + 3],
        ]));
    }

    let mut end_cps = Vec::with_capacity(bkl.len() / 4);
    for i in 0..(bkl.len() / 4) {
        end_cps.push(u32::from_le_bytes([
            bkl[i * 4],
            bkl[i * 4 + 1],
            bkl[i * 4 + 2],
            bkl[i * 4 + 3],
        ]));
    }

    let fbkf_base = cp_count * 4;

    let mut bookmarks = Vec::new();
    for i in 0..entry_count.min(names.len()) {
        let fbkf_offset = fbkf_base + i * 4;
        let ibkl = u16::from_le_bytes([bkf[fbkf_offset], bkf[fbkf_offset + 1]]) as usize;
        if ibkl >= end_cps.len() {
            continue;
        }

        bookmarks.push(Bookmark {
            name: names[i].clone(),
            start_cp: start_cps[i],
            end_cp: end_cps[ibkl],
        });
    }

    bookmarks.sort_by_key(|bookmark| (bookmark.start_cp, bookmark.end_cp));
    Ok(bookmarks)
}

fn parse_style_defaults(
    table_stream: &[u8],
    fib: &FibInfo,
) -> Result<StyleDefaults, DocParseError> {
    let Some((fc, lcb)) = fib.pair(PAIR_STSHF) else {
        return Ok(StyleDefaults::default());
    };
    if lcb < 18 {
        return Ok(StyleDefaults::default());
    }
    let bytes = slice_table_range(table_stream, fc, lcb, "Stshf")?;
    if bytes.len() < 18 {
        return Ok(StyleDefaults::default());
    }

    // STSHF layout: cbStshf (u16) + cbSTDBaseInFile (u16) + flags (u16)
    let flags = u16::from_le_bytes([bytes[4], bytes[5]]);
    let insertion_active = (flags & 0x0001) != 0;
    let deletion_active = (flags & 0x0002) != 0;

    Ok(StyleDefaults {
        insertion_active,
        deletion_active,
    })
}

fn parse_annotation_mark_bookmarks(
    table_stream: &[u8],
    fib: &FibInfo,
) -> Result<Vec<Bookmark>, DocParseError> {
    let Some((fc_bkf, lcb_bkf)) = fib.pair(PAIR_PLCF_ATNBKF) else {
        return Ok(Vec::new());
    };
    let Some((fc_bkl, lcb_bkl)) = fib.pair(PAIR_PLCF_ATNBKL) else {
        return Ok(Vec::new());
    };
    if lcb_bkf == 0 || lcb_bkl == 0 {
        return Ok(Vec::new());
    }

    let bkf = slice_table_range(table_stream, fc_bkf, lcb_bkf, "PlcfAtnBkf")?;
    let bkl = slice_table_range(table_stream, fc_bkl, lcb_bkl, "PlcfAtnBkl")?;
    if bkf.len() < 4 || (bkf.len() - 4) % 8 != 0 || bkl.len() < 8 || bkl.len() % 4 != 0 {
        return Ok(Vec::new());
    }

    let entry_count = (bkf.len() - 4) / 8;
    let cp_count = entry_count + 1;

    let mut start_cps = Vec::with_capacity(cp_count);
    for i in 0..cp_count {
        start_cps.push(u32::from_le_bytes([
            bkf[i * 4],
            bkf[i * 4 + 1],
            bkf[i * 4 + 2],
            bkf[i * 4 + 3],
        ]));
    }

    let mut end_cps = Vec::with_capacity(bkl.len() / 4);
    for i in 0..(bkl.len() / 4) {
        end_cps.push(u32::from_le_bytes([
            bkl[i * 4],
            bkl[i * 4 + 1],
            bkl[i * 4 + 2],
            bkl[i * 4 + 3],
        ]));
    }

    let fbkf_base = cp_count * 4;
    let mut out = Vec::new();

    for i in 0..entry_count {
        let fbkf_offset = fbkf_base + i * 4;
        let pair_idx = u16::from_le_bytes([bkf[fbkf_offset], bkf[fbkf_offset + 1]]) as usize;
        if pair_idx >= end_cps.len() {
            continue;
        }

        let handle = u16::from_le_bytes([bkf[fbkf_offset], bkf[fbkf_offset + 1]]);
        out.push(Bookmark {
            name: format!("_annotation_mark_{handle}"),
            start_cp: start_cps[i],
            end_cp: end_cps[pair_idx],
        });
    }

    out.sort_by_key(|bookmark| (bookmark.start_cp, bookmark.end_cp));
    Ok(out)
}

fn parse_sttb(bytes: &[u8]) -> Vec<String> {
    if bytes.len() < 6 {
        return Vec::new();
    }

    let f_extend = u16::from_le_bytes([bytes[0], bytes[1]]);
    let c_data = u16::from_le_bytes([bytes[2], bytes[3]]) as usize;
    let cb_extra = u16::from_le_bytes([bytes[4], bytes[5]]) as usize;

    let mut out = Vec::with_capacity(c_data);
    let mut cursor = 6;

    for _ in 0..c_data {
        if cursor >= bytes.len() {
            break;
        }

        let text = if f_extend == 0xFFFF {
            if cursor + 2 > bytes.len() {
                break;
            }
            let cch = u16::from_le_bytes([bytes[cursor], bytes[cursor + 1]]) as usize;
            cursor += 2;

            let byte_len = cch.saturating_mul(2);
            if cursor + byte_len > bytes.len() {
                break;
            }

            let mut s = String::new();
            for chunk in bytes[cursor..cursor + byte_len].chunks_exact(2) {
                let unit = u16::from_le_bytes([chunk[0], chunk[1]]);
                s.push(decode_utf16_unit(unit));
            }
            cursor += byte_len;
            s
        } else {
            let cch = bytes[cursor] as usize;
            cursor += 1;
            if cursor + cch > bytes.len() {
                break;
            }
            let (decoded, _, _) = WINDOWS_1252.decode(&bytes[cursor..cursor + cch]);
            cursor += cch;
            decoded.into_owned()
        };

        if cb_extra > 0 {
            if cursor + cb_extra > bytes.len() {
                break;
            }
            cursor += cb_extra;
        }

        out.push(text);
    }

    out
}

fn slice_table_range<'a>(
    table_stream: &'a [u8],
    fc: u32,
    lcb: u32,
    label: &'static str,
) -> Result<&'a [u8], DocParseError> {
    let start = fc as usize;
    let len = lcb as usize;
    let end = start.saturating_add(len);

    if start > table_stream.len() || end > table_stream.len() {
        return Err(DocParseError::InvalidTableRange { label, fc, lcb });
    }

    Ok(&table_stream[start..end])
}

fn fc_to_cp(fc: u32, pieces: &[Piece]) -> Option<u32> {
    for piece in pieces {
        let cp_len = piece.cp_end.saturating_sub(piece.cp_start);
        let stride = if piece.compressed { 1 } else { 2 };
        let byte_start = piece.byte_start;
        let byte_end = byte_start.saturating_add(cp_len.saturating_mul(stride));

        if fc < byte_start || fc > byte_end {
            continue;
        }

        let delta = fc.saturating_sub(byte_start);
        let cp_delta = if stride == 1 { delta } else { delta / 2 };
        let cp = piece.cp_start.saturating_add(cp_delta);
        if cp <= piece.cp_end {
            return Some(cp);
        }
    }
    None
}

fn slice_cp_text(text_chars: &[char], start_cp: u32, end_cp: u32) -> String {
    if start_cp >= end_cp {
        return String::new();
    }

    let start = start_cp as usize;
    let end = (end_cp as usize).min(text_chars.len());
    if start >= end {
        return String::new();
    }

    text_chars[start..end].iter().collect()
}

fn decode_utf16_unit(unit: u16) -> char {
    char::from_u32(unit as u32).unwrap_or('\u{FFFD}')
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, DocParseError> {
    if offset + 2 > bytes.len() {
        return Err(DocParseError::InvalidFib(
            "unexpected EOF while reading u16".to_string(),
        ));
    }
    Ok(u16::from_le_bytes([bytes[offset], bytes[offset + 1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, DocParseError> {
    if offset + 4 > bytes.len() {
        return Err(DocParseError::InvalidFib(
            "unexpected EOF while reading u32".to_string(),
        ));
    }
    Ok(u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{
        FibInfo, Piece, decode_utf16_unit, filter_runs_to_story_ranges, included_story_ranges,
        parse_grpprl, parse_sttb, sprm_total_size,
    };
    use crate::model::ChpxRun;

    #[test]
    fn sprm_total_size_handles_revision_opcodes() {
        assert_eq!(sprm_total_size(&[], 0, 0x0800), Some(3));
        assert_eq!(sprm_total_size(&[], 0, 0x0801), Some(3));
        assert_eq!(sprm_total_size(&[], 0, 0x4804), Some(4));
        assert_eq!(sprm_total_size(&[], 0, 0x6805), Some(6));
    }

    #[test]
    fn sprm_total_size_handles_type6_exceptions() {
        let grpprl = vec![0, 0, 4, 0];
        assert_eq!(sprm_total_size(&grpprl, 0, 0xD608), Some(7));
        assert_eq!(sprm_total_size(&grpprl, 0, 0xC615), Some(7));
    }

    #[test]
    fn grpprl_parser_reads_multiple_prls() {
        let grpprl = vec![
            0x01, 0x08, 1, // fRMark = 1
            0x04, 0x48, 2, 0, // ibstRMark = 2
        ];
        let sprms = parse_grpprl(&grpprl);
        assert_eq!(sprms.len(), 2);
        assert_eq!(sprms[0].opcode, 0x0801);
        assert_eq!(sprms[0].operand, vec![1]);
        assert_eq!(sprms[1].opcode, 0x4804);
        assert_eq!(sprms[1].operand, vec![2, 0]);
    }

    #[test]
    fn sttb_utf16_decodes_expected_strings() {
        let bytes = vec![
            0xFF, 0xFF, // fExtend
            0x02, 0x00, // cData
            0x00, 0x00, // cbExtra
            0x05, 0x00, b'A', 0, b'l', 0, b'i', 0, b'c', 0, b'e', 0, // "Alice"
            0x03, 0x00, b'B', 0, b'o', 0, b'b', 0, // "Bob"
        ];

        let names = parse_sttb(&bytes);
        assert_eq!(names, vec!["Alice".to_string(), "Bob".to_string()]);
    }

    #[test]
    fn utf16_unit_decoder_replaces_surrogates() {
        assert_eq!(decode_utf16_unit(0x0041), 'A');
        assert_eq!(decode_utf16_unit(0xD800), '\u{FFFD}');
    }

    #[test]
    fn included_story_ranges_keep_main_and_textbox_stories() {
        let fib = FibInfo {
            table_stream: "1Table",
            ccp_text: 100,
            ccp_ftn: 10,
            ccp_hdr: 20,
            ccp_mcr: 0,
            ccp_atn: 5,
            ccp_edn: 7,
            ccp_txbx: 11,
            ccp_hdr_txbx: 13,
            fc_lcb_pairs: Vec::new(),
        };

        let pieces = vec![Piece {
            cp_start: 0,
            cp_end: 900,
            byte_start: 0,
            compressed: true,
            prm_sprms: Vec::new(),
        }];

        assert_eq!(
            included_story_ranges(&fib, &pieces),
            vec![(0, 100), (142, 153), (153, 166)]
        );
    }

    #[test]
    fn story_range_filter_excludes_non_textbox_subdocuments() {
        let runs = vec![
            ChpxRun {
                start_cp: 98,
                end_cp: 102,
                text: "abcd".to_string(),
                sprms: Vec::new(),
                source_chpx_id: Some(1),
            },
            ChpxRun {
                start_cp: 110,
                end_cp: 114,
                text: "skip".to_string(),
                sprms: Vec::new(),
                source_chpx_id: Some(2),
            },
            ChpxRun {
                start_cp: 142,
                end_cp: 146,
                text: "txbx".to_string(),
                sprms: Vec::new(),
                source_chpx_id: Some(3),
            },
            ChpxRun {
                start_cp: 151,
                end_cp: 155,
                text: "tail".to_string(),
                sprms: Vec::new(),
                source_chpx_id: Some(4),
            },
        ];

        let filtered = filter_runs_to_story_ranges(runs, &[(0, 100), (142, 153), (153, 166)]);
        assert_eq!(
            filtered,
            vec![
                ChpxRun {
                    start_cp: 98,
                    end_cp: 100,
                    text: "ab".to_string(),
                    sprms: Vec::new(),
                    source_chpx_id: Some(1),
                },
                ChpxRun {
                    start_cp: 142,
                    end_cp: 146,
                    text: "txbx".to_string(),
                    sprms: Vec::new(),
                    source_chpx_id: Some(3),
                },
                ChpxRun {
                    start_cp: 151,
                    end_cp: 153,
                    text: "ta".to_string(),
                    sprms: Vec::new(),
                    source_chpx_id: Some(4),
                },
                ChpxRun {
                    start_cp: 153,
                    end_cp: 155,
                    text: "il".to_string(),
                    sprms: Vec::new(),
                    source_chpx_id: Some(4),
                },
            ]
        );
    }
}
