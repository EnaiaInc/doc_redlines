use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;

use crate::combine::can_combine;
use crate::dttm::Dttm;
use crate::model::{
    BuiltRedline, ChpxRun, ParsedDocument, RedlineSignature, RevisionEntry, RevisionType,
    SourceSegment, StackMetadata, StyleDefaults,
};
use crate::normalize::normalize_revision_text;
use crate::splitter::{
    DocumentTextIndex, extract_text_for_range, slice_chars, split_points_for_redline,
};
use crate::sprm::{RevisionSprms, ToggleOp, collect_revision_sprms};

#[derive(Debug, Clone)]
struct RevisionCandidate {
    signature: RedlineSignature,
    start_cp: u32,
    end_cp: u32,
    segments: Vec<SourceSegment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollectorMode {
    Stateless,
    Stateful,
    Event,
}

#[derive(Debug, Clone, Default)]
struct ActiveRevisionState {
    insertion_active: bool,
    deletion_active: bool,
    insertion_author: Option<u16>,
    insertion_timestamp: Option<Dttm>,
    deletion_author: Option<u16>,
    deletion_timestamp: Option<Dttm>,
}

#[derive(Debug, Clone, Copy)]
struct RunRevisionState {
    has_insertion: bool,
    has_deletion: bool,
    insertion_author: Option<u16>,
    insertion_timestamp: Option<Dttm>,
    deletion_author: Option<u16>,
    deletion_timestamp: Option<Dttm>,
    dual_insertion_first: Option<bool>,
}

#[derive(Debug, Clone)]
struct LoOverlapRunInfo {
    start_cp: u32,
    end_cp: u32,
    text: String,
    has_insertion: bool,
    has_deletion: bool,
    insertion_author: Option<u16>,
    insertion_timestamp: Option<Dttm>,
    deletion_author: Option<u16>,
    deletion_timestamp: Option<Dttm>,
}

#[derive(Debug, Clone)]
struct LoOverlapVisibleChar {
    ch: char,
    cp: u32,
    inserted: bool,
    run_idx: usize,
    offset_in_run: usize,
    run_len: usize,
}

#[derive(Debug, Clone)]
struct LoOverlapAliasSpan {
    start_cp: u32,
    end_cp: u32,
    text: String,
}

impl RevisionCandidate {
    fn into_redline(self) -> BuiltRedline {
        BuiltRedline {
            signature: self.signature,
            start_cp: self.start_cp,
            end_cp: self.end_cp,
            segments: self.segments,
        }
    }
}

pub fn extract_revisions(document: &ParsedDocument) -> Vec<RevisionEntry> {
    let mut runs = document.runs.clone();
    runs.sort_by_key(|run| (run.start_cp, run.end_cp));

    let redlines = build_redlines_for_debug(document);
    let text_index = DocumentTextIndex::from_runs(&runs);

    let mut out = Vec::new();

    for redline in &redlines {
        let split_points = split_points_for_redline(
            redline.start_cp,
            redline.end_cp,
            redline.signature.revision_type,
            redline.signature.stack.is_some(),
            &redline.segments,
            &runs,
            &document.bookmarks,
        );

        for pair in split_points.windows(2) {
            let start_cp = pair[0];
            let end_cp = pair[1];
            if start_cp >= end_cp {
                continue;
            }

            push_entry(&mut out, &redline, document, &text_index, start_cp, end_cp);
        }
    }

    out.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });

    if !lo_strict_enabled() {
        augment_lo_current_text_overlap_aliases(&mut out, document, &text_index, &runs);
        augment_short_fragment_contiguous_aliases(&mut out);
        augment_lowercase_continuation_aliases(&mut out);
        augment_mirrored_full_span_entries(&mut out);
        augment_alternate_content_duplicates(&mut out);
        augment_midword_adjacent_insertions(&mut out);
        augment_dual_bridge_entries(&mut out);
        augment_mirrored_deletion_prefix_clips(&mut out);
        augment_ordinal_suffix_deletion_tails(&mut out);
        augment_ordinal_suffix_prefix_aliases(&mut out);
        augment_open_quote_prefix_aliases(&mut out);
        augment_deleted_annotation_reference_entries(&mut out, document, &text_index);
        suppress_mid_paragraph_empty_insertions(&mut out);
        augment_whitespace_adjacent_insertions_across_empty_companions(&mut out);
        augment_punctuation_adjacent_entries(&mut out);
        augment_sentence_adjacent_insertions_across_timestamp_transition(&mut out);
        augment_defined_term_adjacent_insertions_after_short_prefix(&mut out);
        augment_midword_adjacent_insertions_across_timestamp_transition(&mut out);
        augment_short_token_insertions_across_timestamp_transition(&mut out);
        augment_label_amount_line_item_aliases(&mut out);
        augment_sentence_clause_aliases_from_tail_evidence(&mut out);
    }

    out
}

fn lo_strict_enabled() -> bool {
    env::var("DOC_RL_STRICT_LO")
        .ok()
        .as_deref()
        .is_some_and(|value| value == "1")
}

fn augment_lo_current_text_overlap_aliases(
    entries: &mut Vec<RevisionEntry>,
    document: &ParsedDocument,
    text_index: &DocumentTextIndex,
    runs: &[ChpxRun],
) {
    if !lo_current_text_overlap_alias_enabled() || runs.is_empty() {
        return;
    }

    let Some(blocks) = collect_lo_current_text_overlap_blocks(runs) else {
        return;
    };
    if blocks.is_empty() {
        return;
    }

    let mut existing = HashSet::<(RevisionType, String)>::new();
    for entry in entries.iter() {
        existing.insert((entry.revision_type, entry.text.clone()));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for (author_idx, block) in blocks {
        let author = document.authors.get(author_idx as usize).cloned();
        let block_start = block.first().map(|run| run.start_cp).unwrap_or(0);
        let hidden_ranges: Vec<(u32, u32)> = block
            .iter()
            .filter(|run| run.has_deletion && !run.has_insertion)
            .map(|run| (run.start_cp, run.end_cp))
            .collect();
        if hidden_ranges.is_empty() {
            continue;
        }

        let (deletion_aliases, mut insertion_aliases, deletion_barriers) =
            simulate_lo_current_text_overlap_block(&block, &hidden_ranges);
        merge_overlap_aliases_across_deleted_spans(&mut insertion_aliases, &deletion_barriers);
        let clipped_alias = simulate_lo_current_text_overlap_clipped_alias(&block);
        let range_mutation_alias = simulate_lo_current_text_overlap_range_mutation_alias(&block);

        for span in deletion_aliases {
            push_lo_current_text_overlap_entry(
                &mut additions,
                &mut existing,
                RevisionType::Deletion,
                author.clone(),
                block_start,
                span,
                text_index,
            );
        }
        for span in insertion_aliases {
            push_lo_current_text_overlap_entry(
                &mut additions,
                &mut existing,
                RevisionType::Insertion,
                author.clone(),
                block_start,
                span,
                text_index,
            );
        }
        if let Some(span) = clipped_alias {
            push_lo_current_text_overlap_entry(
                &mut additions,
                &mut existing,
                RevisionType::Insertion,
                author.clone(),
                block_start,
                span,
                text_index,
            );
        }
        if let Some(span) = range_mutation_alias {
            push_lo_current_text_overlap_entry(
                &mut additions,
                &mut existing,
                RevisionType::Insertion,
                author.clone(),
                block_start,
                span,
                text_index,
            );
        }
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn text_contains_structural_chars(text: &str) -> bool {
    text.chars().any(|ch| {
        matches!(
            ch,
            '\r' | '\u{0001}'
                | '\u{0003}'
                | '\u{0006}'
                | '\u{0007}'
                | '\u{0008}'
                | '\u{0013}'
                | '\u{0014}'
                | '\u{0015}'
                | '\u{FFF9}'
        )
    })
}

fn is_short_fragment(text: &str) -> bool {
    let len = text.chars().count();
    if len == 0 {
        return false;
    }
    if len <= 2 {
        return true;
    }
    let alnum = text.chars().filter(|ch| ch.is_ascii_alphanumeric()).count();
    alnum > 0 && alnum <= 2
}

fn augment_short_fragment_contiguous_aliases(entries: &mut Vec<RevisionEntry>) {
    if entries.len() < 2 {
        return;
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut by_sig: HashMap<
        (RevisionType, Option<String>, Option<String>, Option<u32>),
        Vec<&RevisionEntry>,
    > = HashMap::new();
    for entry in entries.iter() {
        by_sig
            .entry((
                entry.revision_type,
                entry.author.clone(),
                entry.timestamp.clone(),
                entry.paragraph_index,
            ))
            .or_default()
            .push(entry);
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for ((rev_type, author, timestamp, _para_idx), group) in by_sig {
        let mut best_by_start: BTreeMap<u32, &RevisionEntry> = BTreeMap::new();
        for entry in group {
            let key = entry.start_cp;
            let replace = match best_by_start.get(&key) {
                None => true,
                Some(existing_entry) => {
                    let existing_alnum = existing_entry
                        .text
                        .chars()
                        .filter(|ch| ch.is_ascii_alphanumeric())
                        .count();
                    let candidate_alnum = entry
                        .text
                        .chars()
                        .filter(|ch| ch.is_ascii_alphanumeric())
                        .count();
                    if candidate_alnum != existing_alnum {
                        candidate_alnum > existing_alnum
                    } else {
                        entry.text.chars().count() > existing_entry.text.chars().count()
                    }
                }
            };
            if replace {
                best_by_start.insert(key, entry);
            }
        }

        let mut starts: Vec<u32> = best_by_start.keys().copied().collect();
        starts.sort_unstable();

        let mut idx = 0;
        while idx < starts.len() {
            let start_cp = starts[idx];
            let Some(base) = best_by_start.get(&start_cp) else {
                idx += 1;
                continue;
            };
            let (Some(base_para), Some(base_offset)) = (base.paragraph_index, base.char_offset)
            else {
                idx += 1;
                continue;
            };

            let mut merged_text = base.text.clone();
            let mut merged_end = base.end_cp;
            let mut total_chars = merged_text.chars().count() as u32;
            let mut has_short = is_short_fragment(&base.text);
            let mut next_idx = idx + 1;

            while next_idx < starts.len() {
                let next_start = starts[next_idx];
                if next_start != merged_end {
                    break;
                }
                let Some(next) = best_by_start.get(&next_start) else {
                    break;
                };
                let Some(next_offset) = next.char_offset else {
                    break;
                };
                if next.paragraph_index != Some(base_para)
                    || next_offset != base_offset.saturating_add(total_chars)
                {
                    break;
                }

                merged_text.push_str(&next.text);
                merged_end = next.end_cp;
                total_chars = total_chars.saturating_add(next.text.chars().count() as u32);
                has_short |= is_short_fragment(&next.text);
                next_idx += 1;
            }

            if next_idx > idx + 1
                && has_short
                && text_has_alnum(&merged_text)
                && !text_contains_structural_chars(&merged_text)
            {
                let key = (rev_type, start_cp, merged_end, merged_text.clone());
                if !existing.contains(&key) {
                    existing.insert(key);
                    additions.push(RevisionEntry {
                        revision_type: rev_type,
                        text: merged_text,
                        author: author.clone(),
                        timestamp: timestamp.clone(),
                        start_cp,
                        end_cp: merged_end,
                        paragraph_index: Some(base_para),
                        char_offset: Some(base_offset),
                        context: base.context.clone(),
                    });
                }
            }

            idx = if next_idx > idx + 1 {
                next_idx
            } else {
                idx + 1
            };
        }
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_lowercase_continuation_aliases(entries: &mut Vec<RevisionEntry>) {
    if entries.len() < 2 {
        return;
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut by_start =
        HashMap::<(u32, Option<String>, Option<String>, Option<u32>), &RevisionEntry>::new();
    for entry in entries.iter() {
        if entry.revision_type != RevisionType::Insertion {
            continue;
        }
        by_start.insert(
            (
                entry.start_cp,
                entry.author.clone(),
                entry.timestamp.clone(),
                entry.paragraph_index,
            ),
            entry,
        );
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for left in entries.iter() {
        if left.revision_type != RevisionType::Insertion {
            continue;
        }
        if !left
            .text
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        let right_key = (
            left.end_cp,
            left.author.clone(),
            left.timestamp.clone(),
            left.paragraph_index,
        );
        let Some(right) = by_start.get(&right_key) else {
            continue;
        };
        if !right
            .text
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_lowercase())
        {
            continue;
        }
        if text_contains_structural_chars(&left.text) || text_contains_structural_chars(&right.text)
        {
            continue;
        }
        if left
            .text
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .count()
            < 8
            || right
                .text
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric())
                .count()
                < 8
        {
            continue;
        }

        let merged_text = format!("{}{}", left.text, right.text);
        let key = (
            RevisionType::Insertion,
            left.start_cp,
            right.end_cp,
            merged_text.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Insertion,
            text: merged_text,
            author: left.author.clone(),
            timestamp: left.timestamp.clone(),
            start_cp: left.start_cp,
            end_cp: right.end_cp,
            paragraph_index: left.paragraph_index,
            char_offset: left.char_offset,
            context: left.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn collect_lo_current_text_overlap_blocks(
    runs: &[ChpxRun],
) -> Option<Vec<(u16, Vec<LoOverlapRunInfo>)>> {
    let mut infos = Vec::<LoOverlapRunInfo>::new();
    for run in runs {
        let marks = collect_revision_sprms(run);
        infos.push(LoOverlapRunInfo {
            start_cp: run.start_cp,
            end_cp: run.end_cp,
            text: run.text.clone(),
            has_insertion: marks.has_insertion
                || marks.insertion_author.is_some()
                || marks.insertion_timestamp.is_some(),
            has_deletion: marks.has_deletion
                || marks.deletion_author.is_some()
                || marks.deletion_timestamp.is_some(),
            insertion_author: marks.insertion_author,
            insertion_timestamp: marks.insertion_timestamp,
            deletion_author: marks.deletion_author,
            deletion_timestamp: marks.deletion_timestamp,
        });
    }

    let mut blocks = Vec::<(u16, Vec<LoOverlapRunInfo>)>::new();
    let mut idx = 0;
    while idx < infos.len() {
        let mut end = idx + 1;
        while end < infos.len() && infos[end].start_cp <= infos[end - 1].end_cp {
            end += 1;
        }
        if let Some(author_idx) = lo_current_text_overlap_block_author(&infos[idx..end]) {
            blocks.push((author_idx, infos[idx..end].to_vec()));
        }
        idx = end;
    }

    Some(blocks)
}

fn lo_current_text_overlap_block_author(block: &[LoOverlapRunInfo]) -> Option<u16> {
    if block.is_empty() {
        return None;
    }

    let mut author_idx: Option<u16> = None;
    let mut has_dual_same_author = false;
    let mut has_hidden_pure_delete = false;

    for run in block {
        if run.has_insertion {
            let Some(author) = run.insertion_author else {
                return None;
            };
            if run.insertion_timestamp.is_some() {
                return None;
            }
            match author_idx {
                Some(existing) if existing != author => return None,
                None => author_idx = Some(author),
                _ => {}
            }
        }
        if run.has_deletion {
            let Some(author) = run.deletion_author else {
                return None;
            };
            if run.deletion_timestamp.is_some() {
                return None;
            }
            match author_idx {
                Some(existing) if existing != author => return None,
                None => author_idx = Some(author),
                _ => {}
            }
        }

        if run.has_insertion && run.has_deletion {
            if run.insertion_author == run.deletion_author && run.insertion_author.is_some() {
                has_dual_same_author = true;
            } else {
                return None;
            }
        }
        if run.has_deletion && !run.has_insertion {
            has_hidden_pure_delete = true;
        }
    }

    if !has_dual_same_author || !has_hidden_pure_delete {
        return None;
    }

    author_idx
}

fn simulate_lo_current_text_overlap_block(
    block: &[LoOverlapRunInfo],
    _hidden_ranges: &[(u32, u32)],
) -> (
    Vec<LoOverlapAliasSpan>,
    Vec<LoOverlapAliasSpan>,
    Vec<(u32, u32)>,
) {
    let mut base = Vec::<LoOverlapVisibleChar>::new();
    let mut cp_to_idx = HashMap::<u32, usize>::new();

    for (run_idx, run) in block.iter().enumerate() {
        if run.has_deletion && !run.has_insertion {
            continue;
        }

        let mut cp = run.start_cp;
        let run_len = run.text.chars().count();
        cp_to_idx.entry(run.start_cp).or_insert(base.len());
        for (offset_in_run, ch) in run.text.chars().enumerate() {
            cp_to_idx.entry(cp).or_insert(base.len());
            base.push(LoOverlapVisibleChar {
                ch,
                cp,
                inserted: run.has_insertion,
                run_idx,
                offset_in_run,
                run_len,
            });
            cp = cp.saturating_add(1);
        }
        cp_to_idx.insert(run.end_cp, base.len());
    }

    let mut current = base.clone();
    let mut deletion_aliases = Vec::<LoOverlapAliasSpan>::new();
    let mut deletion_barriers = Vec::<(u32, u32)>::new();

    for run in block {
        if !(run.has_insertion
            && run.has_deletion
            && run.insertion_author == run.deletion_author
            && run.insertion_author.is_some())
        {
            continue;
        }

        let Some(&fixed_start) = cp_to_idx.get(&run.start_cp) else {
            continue;
        };
        let Some(&fixed_end) = cp_to_idx.get(&run.end_cp) else {
            continue;
        };
        if fixed_start >= current.len() {
            continue;
        }
        let fixed_end = fixed_end.min(current.len());
        if fixed_end <= fixed_start {
            continue;
        }

        let removed: Vec<LoOverlapVisibleChar> = current.drain(fixed_start..fixed_end).collect();
        if removed.is_empty() || !removed.iter().any(|ch| ch.inserted) {
            continue;
        }

        let removed_text: String = removed.iter().map(|ch| ch.ch).collect();
        let normalized = normalize_revision_text(&removed_text);
        if !lo_current_text_overlap_alias_text_ok(&normalized, RevisionType::Deletion) {
            continue;
        }

        let start_cp = removed.first().map(|ch| ch.cp).unwrap_or(run.start_cp);
        let end_cp = removed
            .last()
            .map(|ch| ch.cp.saturating_add(1))
            .unwrap_or(run.end_cp);
        deletion_barriers.push((start_cp, end_cp));
        deletion_aliases.push(LoOverlapAliasSpan {
            start_cp,
            end_cp,
            text: normalized,
        });
    }

    let mut insertion_aliases = Vec::<LoOverlapAliasSpan>::new();
    let mut current_start_cp = None;
    let mut current_end_cp = None;
    let mut current_chars = Vec::<LoOverlapVisibleChar>::new();
    let mut prev_cp = None;
    for ch in &current {
        if !ch.inserted {
            flush_lo_current_text_overlap_span(
                &mut insertion_aliases,
                &mut current_start_cp,
                &mut current_end_cp,
                &mut current_chars,
            );
            prev_cp = None;
            continue;
        }

        if let Some(prev) = prev_cp
            && lo_current_text_overlap_crosses_barrier(prev, ch.cp, &deletion_barriers)
        {
            flush_lo_current_text_overlap_span(
                &mut insertion_aliases,
                &mut current_start_cp,
                &mut current_end_cp,
                &mut current_chars,
            );
        }

        current_start_cp.get_or_insert(ch.cp);
        current_end_cp = Some(ch.cp.saturating_add(1));
        current_chars.push(ch.clone());
        prev_cp = Some(ch.cp);
    }
    flush_lo_current_text_overlap_span(
        &mut insertion_aliases,
        &mut current_start_cp,
        &mut current_end_cp,
        &mut current_chars,
    );

    (deletion_aliases, insertion_aliases, deletion_barriers)
}

fn simulate_lo_current_text_overlap_clipped_alias(
    block: &[LoOverlapRunInfo],
) -> Option<LoOverlapAliasSpan> {
    let mut base = Vec::<LoOverlapVisibleChar>::new();
    let mut cp_to_idx = HashMap::<u32, usize>::new();

    for (run_idx, run) in block.iter().enumerate() {
        if run.has_deletion && !run.has_insertion {
            continue;
        }
        let mut cp = run.start_cp;
        let run_len = run.text.chars().count();
        cp_to_idx.entry(run.start_cp).or_insert(base.len());
        for (offset_in_run, ch) in run.text.chars().enumerate() {
            cp_to_idx.entry(cp).or_insert(base.len());
            base.push(LoOverlapVisibleChar {
                ch,
                cp,
                inserted: run.has_insertion,
                run_idx,
                offset_in_run,
                run_len,
            });
            cp = cp.saturating_add(1);
        }
        cp_to_idx.insert(run.end_cp, base.len());
    }

    let mut current = base.clone();
    for run in block {
        if !(run.has_insertion
            && run.has_deletion
            && run.insertion_author == run.deletion_author
            && run.insertion_author.is_some())
        {
            continue;
        }
        let Some(&fixed_start) = cp_to_idx.get(&run.start_cp) else {
            continue;
        };
        let Some(&fixed_end) = cp_to_idx.get(&run.end_cp) else {
            continue;
        };
        if fixed_start >= current.len() {
            continue;
        }
        let fixed_end = fixed_end.min(current.len());
        if fixed_end <= fixed_start {
            continue;
        }
        current.drain(fixed_start..fixed_end);
    }

    let mut span_start = None;
    let mut span_end = None;
    let mut span_chars = Vec::<LoOverlapVisibleChar>::new();
    for ch in &current {
        if !ch.inserted {
            continue;
        }
        span_start.get_or_insert(ch.cp);
        span_end = Some(ch.cp.saturating_add(1));
        span_chars.push(ch.clone());
    }

    let Some(start_cp) = span_start else {
        return None;
    };
    let Some(end_cp) = span_end else { return None };

    if !lo_current_text_overlap_span_respects_run_edges(&span_chars) {
        return None;
    }

    let text: String = span_chars.iter().map(|ch| ch.ch).collect();
    let normalized = normalize_revision_text(&text);
    if !lo_current_text_overlap_alias_text_ok(&normalized, RevisionType::Insertion) {
        return None;
    }

    Some(LoOverlapAliasSpan {
        start_cp,
        end_cp,
        text: normalized,
    })
}

fn merge_overlap_aliases_across_deleted_spans(
    aliases: &mut Vec<LoOverlapAliasSpan>,
    deletion_barriers: &[(u32, u32)],
) {
    if aliases.len() < 2 || deletion_barriers.is_empty() {
        return;
    }

    aliases.sort_by_key(|span| (span.start_cp, span.end_cp));
    let mut merged = Vec::<LoOverlapAliasSpan>::new();
    let mut i = 0usize;
    while i < aliases.len() {
        let mut current = aliases[i].clone();
        let mut j = i + 1;
        while j < aliases.len() {
            let next = &aliases[j];
            if next.start_cp < current.end_cp {
                j += 1;
                continue;
            }
            let gap_start = current.end_cp;
            let gap_end = next.start_cp;
            let gap_deleted = deletion_barriers
                .iter()
                .any(|(start, end)| *start <= gap_start && *end >= gap_end);
            if !gap_deleted {
                break;
            }
            if current
                .text
                .chars()
                .count()
                .saturating_add(next.text.chars().count())
                > 24
            {
                break;
            }
            current.end_cp = next.end_cp;
            current.text.push_str(&next.text);
            j += 1;
        }
        merged.push(current);
        i = j.max(i + 1);
    }

    aliases.extend(merged);
    aliases.sort_by_key(|span| (span.start_cp, span.end_cp));
    aliases.dedup_by(|left, right| {
        left.start_cp == right.start_cp && left.end_cp == right.end_cp && left.text == right.text
    });
}

fn simulate_lo_current_text_overlap_range_mutation_alias(
    block: &[LoOverlapRunInfo],
) -> Option<LoOverlapAliasSpan> {
    let mut insertion_chars = Vec::<LoOverlapVisibleChar>::new();
    let mut span_start = None;
    let mut span_end = None;

    for (run_idx, run) in block.iter().enumerate() {
        if !run.has_insertion {
            continue;
        }
        if span_start.is_none() {
            span_start = Some(run.start_cp);
        }
        span_end = Some(span_end.unwrap_or(run.end_cp).max(run.end_cp));
        let mut cp = run.start_cp;
        let run_len = run.text.chars().count();
        for (offset_in_run, ch) in run.text.chars().enumerate() {
            insertion_chars.push(LoOverlapVisibleChar {
                ch,
                cp,
                inserted: true,
                run_idx,
                offset_in_run,
                run_len,
            });
            cp = cp.saturating_add(1);
        }
    }

    let mut span_start = span_start?;
    let mut span_end = span_end?;

    let mut interior_deletions = Vec::<(u32, u32)>::new();
    for run in block.iter().filter(|run| run.has_deletion) {
        let del_start = run.start_cp;
        let del_end = run.end_cp;
        if del_end <= span_start || del_start >= span_end {
            continue;
        }
        if del_start <= span_start && del_end >= span_end {
            return None;
        }
        if del_start <= span_start && del_end < span_end {
            span_start = del_end;
            continue;
        }
        if del_start > span_start && del_end >= span_end {
            span_end = del_start;
            continue;
        }
        interior_deletions.push((del_start, del_end));
    }

    let mut filtered = Vec::<LoOverlapVisibleChar>::new();
    for ch in insertion_chars {
        if ch.cp < span_start || ch.cp >= span_end {
            continue;
        }
        let inside = interior_deletions
            .iter()
            .any(|(start, end)| ch.cp >= *start && ch.cp < *end);
        if inside {
            continue;
        }
        filtered.push(ch);
    }

    if filtered.is_empty() {
        return None;
    }
    if !lo_current_text_overlap_span_respects_run_edges(&filtered) {
        return None;
    }

    let text: String = filtered.iter().map(|ch| ch.ch).collect();
    let normalized = normalize_revision_text(&text);
    if !lo_current_text_overlap_alias_text_ok(&normalized, RevisionType::Insertion) {
        return None;
    }

    Some(LoOverlapAliasSpan {
        start_cp: span_start,
        end_cp: span_end,
        text: normalized,
    })
}

fn lo_current_text_overlap_crosses_barrier(
    left_cp: u32,
    right_cp: u32,
    barriers: &[(u32, u32)],
) -> bool {
    barriers
        .iter()
        .any(|(start_cp, _)| left_cp < *start_cp && *start_cp < right_cp)
}

fn flush_lo_current_text_overlap_span(
    out: &mut Vec<LoOverlapAliasSpan>,
    start_cp: &mut Option<u32>,
    end_cp: &mut Option<u32>,
    chars: &mut Vec<LoOverlapVisibleChar>,
) {
    let Some(span_start) = *start_cp else {
        chars.clear();
        *end_cp = None;
        return;
    };
    let Some(span_end) = *end_cp else {
        chars.clear();
        *start_cp = None;
        return;
    };

    if !lo_current_text_overlap_span_respects_run_edges(chars) {
        chars.clear();
        *start_cp = None;
        *end_cp = None;
        return;
    }

    let text: String = chars.iter().map(|ch| ch.ch).collect();
    let normalized = normalize_revision_text(&text);
    if lo_current_text_overlap_alias_text_ok(&normalized, RevisionType::Insertion) {
        out.push(LoOverlapAliasSpan {
            start_cp: span_start,
            end_cp: span_end,
            text: normalized,
        });
    }

    chars.clear();
    *start_cp = None;
    *end_cp = None;
}

fn lo_current_text_overlap_span_respects_run_edges(chars: &[LoOverlapVisibleChar]) -> bool {
    if !lo_overlap_edge_strict_enabled() {
        return true;
    }
    let mut per_run = HashMap::<usize, (usize, usize, usize, usize)>::new();
    for ch in chars {
        let entry = per_run.entry(ch.run_idx).or_insert((
            ch.offset_in_run,
            ch.offset_in_run,
            0,
            ch.run_len,
        ));
        entry.0 = entry.0.min(ch.offset_in_run);
        entry.1 = entry.1.max(ch.offset_in_run);
        entry.2 += 1;
        entry.3 = ch.run_len;
    }

    per_run
        .into_values()
        .all(|(min_offset, max_offset, count, run_len)| {
            let contiguous = max_offset.saturating_sub(min_offset).saturating_add(1) == count;
            let touches_edge = min_offset == 0 || max_offset + 1 == run_len;
            contiguous && touches_edge
        })
}

fn lo_overlap_edge_strict_enabled() -> bool {
    std::env::var("DOC_RL_OVERLAP_EDGE_STRICT")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn lo_current_text_overlap_alias_text_ok(text: &str, revision_type: RevisionType) -> bool {
    if text.is_empty() || !text_has_alnum(text) {
        return false;
    }

    let len = text.chars().count();
    match revision_type {
        RevisionType::Insertion => len <= 24,
        RevisionType::Deletion => len <= 8,
    }
}

fn push_lo_current_text_overlap_entry(
    additions: &mut Vec<RevisionEntry>,
    existing: &mut HashSet<(RevisionType, String)>,
    revision_type: RevisionType,
    author: Option<String>,
    block_start_cp: u32,
    span: LoOverlapAliasSpan,
    text_index: &DocumentTextIndex,
) {
    if existing.contains(&(revision_type, span.text.clone())) {
        return;
    }

    existing.insert((revision_type, span.text.clone()));
    additions.push(RevisionEntry {
        revision_type,
        text: span.text,
        author,
        timestamp: None,
        start_cp: span.start_cp,
        end_cp: span.end_cp,
        paragraph_index: Some(text_index.paragraph_index_at(span.start_cp.max(block_start_cp))),
        char_offset: Some(text_index.char_offset_at(span.start_cp.max(block_start_cp))),
        context: Some(normalize_revision_text(&text_index.context(
            span.start_cp,
            span.end_cp.max(span.start_cp.saturating_add(1)),
            20,
        ))),
    });
}

fn lo_current_text_overlap_alias_enabled() -> bool {
    env::var("DOC_RL_LO_CURRENT_TEXT_OVERLAP_ALIAS")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn push_entry(
    out: &mut Vec<RevisionEntry>,
    redline: &BuiltRedline,
    document: &ParsedDocument,
    text_index: &DocumentTextIndex,
    start_cp: u32,
    end_cp: u32,
) {
    let raw_text = extract_text_for_range(&redline.segments, start_cp, end_cp);
    let normalized = normalize_revision_text(&raw_text);
    let author = redline
        .signature
        .author_index
        .and_then(|idx| document.authors.get(idx as usize).cloned());
    let timestamp = redline.signature.timestamp.and_then(|ts| ts.to_iso8601());

    if emit_empty_insertion_companion(&redline.signature.revision_type, &raw_text) {
        out.push(RevisionEntry {
            revision_type: redline.signature.revision_type,
            text: String::new(),
            author: author.clone(),
            timestamp: timestamp.clone(),
            start_cp,
            end_cp: start_cp,
            paragraph_index: Some(text_index.paragraph_index_at(start_cp)),
            char_offset: Some(text_index.char_offset_at(start_cp)),
            context: Some(normalize_revision_text(
                &text_index.context(start_cp, end_cp, 20),
            )),
        });
    }

    if emit_empty_deletion_companion(&redline.signature.revision_type, &raw_text, &normalized) {
        out.push(RevisionEntry {
            revision_type: redline.signature.revision_type,
            text: String::new(),
            author: author.clone(),
            timestamp: timestamp.clone(),
            start_cp,
            end_cp: start_cp,
            paragraph_index: Some(text_index.paragraph_index_at(start_cp)),
            char_offset: Some(text_index.char_offset_at(start_cp)),
            context: Some(normalize_revision_text(
                &text_index.context(start_cp, end_cp, 20),
            )),
        });
    }

    out.push(RevisionEntry {
        revision_type: redline.signature.revision_type,
        text: normalized,
        author,
        timestamp,
        start_cp,
        end_cp,
        paragraph_index: Some(text_index.paragraph_index_at(start_cp)),
        char_offset: Some(text_index.char_offset_at(start_cp)),
        context: Some(normalize_revision_text(
            &text_index.context(start_cp, end_cp, 20),
        )),
    });
}

fn emit_empty_insertion_companion(kind: &RevisionType, raw_text: &str) -> bool {
    if *kind != RevisionType::Insertion {
        return false;
    }

    raw_text
        .chars()
        .next_back()
        .is_some_and(is_structural_tail_char)
}

fn emit_empty_deletion_companion(
    kind: &RevisionType,
    raw_text: &str,
    normalized_text: &str,
) -> bool {
    if !empty_deletion_companion_enabled() {
        return false;
    }
    if *kind != RevisionType::Deletion || normalized_text.is_empty() {
        return false;
    }

    raw_text
        .chars()
        .next_back()
        .is_some_and(is_structural_tail_char)
}

fn is_structural_tail_char(ch: char) -> bool {
    matches!(
        ch,
        '\r' | '\u{0007}' | '\u{000C}' | '\u{0001}' | '\u{0013}' | '\u{0014}' | '\u{0015}'
    )
}

fn empty_deletion_companion_enabled() -> bool {
    env::var("DOC_RL_EMPTY_DELETION_COMPANION")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn suppress_mid_paragraph_empty_insertions(entries: &mut Vec<RevisionEntry>) {
    entries.retain(|entry| {
        if entry.revision_type != RevisionType::Insertion {
            return true;
        }
        if !entry.text.is_empty() {
            return true;
        }

        entry.start_cp == entry.end_cp || entry.char_offset == Some(0)
    });
}

pub fn build_redlines_for_debug(document: &ParsedDocument) -> Vec<BuiltRedline> {
    let mut runs = document.runs.clone();
    runs.sort_by_key(|run| (run.start_cp, run.end_cp));

    let candidates = collect_candidates(&runs, &document.style_defaults);
    let built = build_redlines(candidates);
    if compress_redlines_enabled() {
        compress_redlines(built)
    } else {
        built
    }
}

fn collect_candidates(runs: &[ChpxRun], style_defaults: &StyleDefaults) -> Vec<RevisionCandidate> {
    let mut out = match collector_mode() {
        CollectorMode::Stateless => collect_candidates_stateless(runs),
        CollectorMode::Stateful => collect_candidates_stateful(runs, style_defaults),
        CollectorMode::Event => collect_candidates_event(runs, style_defaults),
    };

    // Match LO redline stack destruction ordering:
    // CP range order first, then earlier timestamp first, with insert before
    // delete only when timestamps tie.
    sort_candidates_for_lo_append(&mut out);

    out
}

fn collector_mode() -> CollectorMode {
    match env::var("DOC_RL_REV_COLLECTOR").ok().as_deref() {
        Some("stateless") => CollectorMode::Stateless,
        Some("stateful") => CollectorMode::Stateful,
        Some("event") => CollectorMode::Event,
        _ => CollectorMode::Stateless,
    }
}

fn compress_redlines_enabled() -> bool {
    env::var("DOC_RL_DISABLE_COMPRESS_REDLINES")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "1")
}

/// Quick extraction of insertion flag and CDttmRMark without fingerprint work.
/// Used only for the structural timestamp repair pre-pass.
fn quick_chpx_insertion(run: &ChpxRun) -> (bool, Option<Dttm>) {
    let mut has_insertion = false;
    let mut timestamp = None;

    for sprm in &run.sprms {
        match sprm.opcode {
            0x0801 => {
                has_insertion = match sprm.operand.first().copied().unwrap_or(0) {
                    0x00 | 0x80 => false,
                    _ => true,
                };
            }
            0x6805 => {
                if sprm.operand.len() >= 4 {
                    let raw = u32::from_le_bytes([
                        sprm.operand[0],
                        sprm.operand[1],
                        sprm.operand[2],
                        sprm.operand[3],
                    ]);
                    timestamp = Dttm::from_raw(raw);
                }
            }
            _ => {}
        }
    }

    (has_insertion, timestamp)
}

fn is_structural_run(run: &ChpxRun) -> bool {
    !run.text.is_empty() && run.text.chars().all(|ch| matches!(ch, '\r' | '\x07'))
}

/// Structural `\r`/`\x07` runs can carry a CHPX CDttmRMark from a formatting
/// change rather than the real insertion block. If the structural timestamp is
/// incompatible with both nearest non-structural insertion neighbors, while
/// those neighbors are compatible with each other, treat the structural run as
/// an outlier and repair it to the earlier neighbor timestamp.
fn compute_structural_ts_repairs(runs: &[ChpxRun]) -> HashMap<usize, Dttm> {
    let mut repairs = HashMap::new();

    for (idx, run) in runs.iter().enumerate() {
        if run.start_cp >= run.end_cp || !is_structural_run(run) {
            continue;
        }

        let (has_insertion, current_ts) = quick_chpx_insertion(run);
        if !has_insertion {
            continue;
        }
        let Some(current_ts) = current_ts else {
            continue;
        };

        let prev_ts = runs[..idx]
            .iter()
            .rev()
            .filter(|candidate| {
                candidate.start_cp < candidate.end_cp && !is_structural_run(candidate)
            })
            .find_map(|candidate| {
                let (has_insertion, timestamp) = quick_chpx_insertion(candidate);
                if has_insertion { timestamp } else { None }
            });
        let next_ts = runs[idx + 1..]
            .iter()
            .filter(|candidate| {
                candidate.start_cp < candidate.end_cp && !is_structural_run(candidate)
            })
            .find_map(|candidate| {
                let (has_insertion, timestamp) = quick_chpx_insertion(candidate);
                if has_insertion { timestamp } else { None }
            });

        if let (Some(prev_ts), Some(next_ts)) = (prev_ts, next_ts) {
            if !current_ts.compatible_with(prev_ts)
                && !current_ts.compatible_with(next_ts)
                && prev_ts.compatible_with(next_ts)
            {
                repairs.insert(idx, prev_ts.min(next_ts));
            }
        }
    }

    repairs
}

fn structural_ts_repair_enabled() -> bool {
    env::var("DOC_RL_STRUCTURAL_TS_REPAIR")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn timestamp_carry_forward_enabled() -> bool {
    env::var("DOC_RL_TIMESTAMP_CARRY_FORWARD")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn collect_candidates_stateless(runs: &[ChpxRun]) -> Vec<RevisionCandidate> {
    let mut out = Vec::new();
    let structural_repairs = if structural_ts_repair_enabled() {
        compute_structural_ts_repairs(runs)
    } else {
        HashMap::new()
    };
    let carry_forward = timestamp_carry_forward_enabled();
    let mut block_anchor_ts: Option<Dttm> = None;
    let mut last_seen_cp_end: Option<u32> = None;

    for (idx, run) in runs.iter().enumerate() {
        if run.start_cp >= run.end_cp {
            continue;
        }

        if let Some(prev_end) = last_seen_cp_end
            && run.start_cp > prev_end
        {
            block_anchor_ts = None;
        }
        last_seen_cp_end = Some(run.end_cp);

        let marks = collect_revision_sprms(run);
        let has_insertion = marks.has_insertion
            || marks.insertion_author.is_some()
            || marks.insertion_timestamp.is_some();
        let has_deletion = marks.has_deletion
            || marks.deletion_author.is_some()
            || marks.deletion_timestamp.is_some();

        let is_structural = is_structural_run(run);
        let participates_in_dual_anchor = has_insertion && has_deletion;
        let effective_insertion_timestamp = if carry_forward && has_insertion {
            if let Some(current_ts) = marks.insertion_timestamp {
                if is_structural {
                    marks.insertion_timestamp
                } else if participates_in_dual_anchor {
                    let anchored_ts = if let Some(anchor_ts) = block_anchor_ts {
                        if current_ts.compatible_with(anchor_ts) {
                            anchor_ts
                        } else {
                            block_anchor_ts = Some(current_ts);
                            current_ts
                        }
                    } else {
                        block_anchor_ts = Some(current_ts);
                        current_ts
                    };
                    Some(anchored_ts)
                } else {
                    marks.insertion_timestamp
                }
            } else {
                marks.insertion_timestamp
            }
        } else {
            marks.insertion_timestamp
        };
        if !has_insertion || (!participates_in_dual_anchor && !is_structural) {
            block_anchor_ts = None;
        }
        let effective_insertion_timestamp = if has_insertion && is_structural {
            structural_repairs
                .get(&idx)
                .copied()
                .or(effective_insertion_timestamp)
        } else {
            effective_insertion_timestamp
        };
        let state = RunRevisionState {
            has_insertion,
            has_deletion,
            insertion_author: marks.insertion_author,
            insertion_timestamp: effective_insertion_timestamp,
            deletion_author: marks.deletion_author,
            deletion_timestamp: marks.deletion_timestamp,
            dual_insertion_first: dual_insertion_first_from_marks(&marks),
        };
        let cancel_dual_micro_noop =
            should_cancel_dual_micro_noop(idx, runs, &state, effective_insertion_timestamp);

        let mut emit_start_cp = run.start_cp;
        let mut emit_end_cp = run.end_cp;
        let mut emit_text: Cow<'_, str> = Cow::Borrowed(run.text.as_str());
        if let Some((shifted_start, shifted_end, shifted_text)) =
            maybe_shift_timestampless_deletion_boundary(idx, runs, &state)
        {
            emit_start_cp = shifted_start;
            emit_end_cp = shifted_end;
            emit_text = Cow::Owned(shifted_text);
        }

        push_candidates_for_run(
            emit_start_cp,
            emit_end_cp,
            emit_text.as_ref(),
            run.source_chpx_id,
            marks.formatting_fingerprint,
            marks.formatting_sequence_fingerprint,
            state,
            cancel_dual_micro_noop,
            &mut out,
        );
    }

    out
}

fn collect_candidates_stateful(
    runs: &[ChpxRun],
    style_defaults: &StyleDefaults,
) -> Vec<RevisionCandidate> {
    let mut out = Vec::new();
    let mut active = ActiveRevisionState::default();
    let mut prev_end: Option<u32> = None;
    let signal_gate_enabled = stateful_signal_gate_enabled();
    let no_signal_carry_max_cp = stateful_no_signal_carry_max_cp();
    for (idx, run) in runs.iter().enumerate() {
        if run.start_cp >= run.end_cp {
            continue;
        }

        if let Some(previous_end) = prev_end
            && run.start_cp > previous_end
        {
            active = ActiveRevisionState::default();
        }
        prev_end = Some(run.end_cp);

        let marks = collect_revision_sprms(run);
        if !run_has_revision_signal(&marks) {
            if signal_gate_enabled {
                let span = run.end_cp.saturating_sub(run.start_cp);
                let has_active = active.insertion_active || active.deletion_active;
                if !has_active || span == 0 || span > no_signal_carry_max_cp {
                    active = ActiveRevisionState::default();
                    continue;
                }
            }
        } else {
            apply_revision_marks(&mut active, &marks, style_defaults);
        }

        let state = RunRevisionState {
            has_insertion: active.insertion_active,
            has_deletion: active.deletion_active,
            insertion_author: active.insertion_author,
            insertion_timestamp: active.insertion_timestamp,
            deletion_author: active.deletion_author,
            deletion_timestamp: active.deletion_timestamp,
            dual_insertion_first: dual_insertion_first_from_marks(&marks),
        };
        let cancel_dual_micro_noop =
            should_cancel_dual_micro_noop(idx, runs, &state, active.insertion_timestamp);

        push_candidates_for_run(
            run.start_cp,
            run.end_cp,
            run.text.as_str(),
            run.source_chpx_id,
            marks.formatting_fingerprint,
            marks.formatting_sequence_fingerprint,
            state,
            cancel_dual_micro_noop,
            &mut out,
        );
    }

    out
}

fn collect_candidates_event(
    runs: &[ChpxRun],
    style_defaults: &StyleDefaults,
) -> Vec<RevisionCandidate> {
    let mut out = Vec::new();
    let mut active = ActiveRevisionState::default();
    let mut prev_end: Option<u32> = None;

    for (idx, run) in runs.iter().enumerate() {
        if run.start_cp >= run.end_cp {
            continue;
        }

        if let Some(previous_end) = prev_end
            && run.start_cp > previous_end
        {
            active = ActiveRevisionState::default();
        }
        prev_end = Some(run.end_cp);

        let marks = collect_revision_sprms(run);

        // Metadata updates are position-scoped in WW8 import and should be
        // visible when a new mark opens at this boundary.
        if let Some(author) = marks.insertion_author {
            active.insertion_author = Some(author);
        }
        if let Some(timestamp) = marks.insertion_timestamp {
            active.insertion_timestamp = Some(timestamp);
        }
        if let Some(author) = marks.deletion_author {
            active.deletion_author = Some(author);
        }
        if let Some(timestamp) = marks.deletion_timestamp {
            active.deletion_timestamp = Some(timestamp);
        }

        if let Some(op) = marks.insertion_toggle {
            apply_toggle_op(
                &mut active.insertion_active,
                &mut active.insertion_author,
                &mut active.insertion_timestamp,
                op,
                style_defaults.insertion_active,
            );
        }
        if let Some(op) = marks.deletion_toggle {
            apply_toggle_op(
                &mut active.deletion_active,
                &mut active.deletion_author,
                &mut active.deletion_timestamp,
                op,
                style_defaults.deletion_active,
            );
        }

        // Some documents carry author/timestamp without explicit toggles on
        // every run. If this span has revision metadata, treat it as active.
        if !active.insertion_active
            && (marks.has_insertion
                || marks.insertion_author.is_some()
                || marks.insertion_timestamp.is_some())
        {
            active.insertion_active = true;
        }
        if !active.deletion_active
            && (marks.has_deletion
                || marks.deletion_author.is_some()
                || marks.deletion_timestamp.is_some())
        {
            active.deletion_active = true;
        }

        // Event-mode carries toggle state until an explicit close.

        let state = RunRevisionState {
            has_insertion: active.insertion_active,
            has_deletion: active.deletion_active,
            insertion_author: active.insertion_author,
            insertion_timestamp: active.insertion_timestamp,
            deletion_author: active.deletion_author,
            deletion_timestamp: active.deletion_timestamp,
            dual_insertion_first: dual_insertion_first_from_marks(&marks),
        };
        let cancel_dual_micro_noop =
            should_cancel_dual_micro_noop(idx, runs, &state, active.insertion_timestamp);

        push_candidates_for_run(
            run.start_cp,
            run.end_cp,
            run.text.as_str(),
            run.source_chpx_id,
            marks.formatting_fingerprint,
            marks.formatting_sequence_fingerprint,
            state,
            cancel_dual_micro_noop,
            &mut out,
        );
    }

    out
}

fn stateful_signal_gate_enabled() -> bool {
    env::var("DOC_RL_STATEFUL_SIGNAL_GATE")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn stateful_no_signal_carry_max_cp() -> u32 {
    env::var("DOC_RL_STATEFUL_NO_SIGNAL_CARRY_MAX_CP")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0)
}

fn run_has_revision_signal(marks: &RevisionSprms) -> bool {
    marks.insertion_toggle.is_some()
        || marks.deletion_toggle.is_some()
        || marks.insertion_author.is_some()
        || marks.insertion_timestamp.is_some()
        || marks.deletion_author.is_some()
        || marks.deletion_timestamp.is_some()
}

fn dual_insertion_first_from_marks(marks: &RevisionSprms) -> Option<bool> {
    if !marks.has_insertion || !marks.has_deletion {
        return None;
    }
    if marks.insertion_timestamp.is_some() || marks.deletion_timestamp.is_some() {
        return None;
    }
    match (marks.insertion_sprm_index, marks.deletion_sprm_index) {
        (Some(ins), Some(del)) => Some(ins <= del),
        _ => None,
    }
}

fn apply_revision_marks(
    active: &mut ActiveRevisionState,
    marks: &RevisionSprms,
    style_defaults: &StyleDefaults,
) {
    if let Some(op) = marks.insertion_toggle {
        apply_toggle_op(
            &mut active.insertion_active,
            &mut active.insertion_author,
            &mut active.insertion_timestamp,
            op,
            style_defaults.insertion_active,
        );
    }

    if let Some(op) = marks.deletion_toggle {
        apply_toggle_op(
            &mut active.deletion_active,
            &mut active.deletion_author,
            &mut active.deletion_timestamp,
            op,
            style_defaults.deletion_active,
        );
    }

    if let Some(author) = marks.insertion_author {
        active.insertion_author = Some(author);
    }
    if let Some(timestamp) = marks.insertion_timestamp {
        active.insertion_timestamp = Some(timestamp);
    }

    if let Some(author) = marks.deletion_author {
        active.deletion_author = Some(author);
    }
    if let Some(timestamp) = marks.deletion_timestamp {
        active.deletion_timestamp = Some(timestamp);
    }
}

fn apply_toggle_op(
    active: &mut bool,
    author: &mut Option<u16>,
    timestamp: &mut Option<Dttm>,
    op: ToggleOp,
    style_default: bool,
) {
    match op {
        ToggleOp::Set(value) => {
            *active = value;
            if !value {
                *author = None;
                *timestamp = None;
            }
        }
        ToggleOp::UseStyle => {
            *active = style_default;
            if !*active {
                *author = None;
                *timestamp = None;
            }
        }
        ToggleOp::InvertStyle => {
            *active = !style_default;
            if !*active {
                *author = None;
                *timestamp = None;
            }
        }
    }
}

fn push_candidates_for_run(
    run_start_cp: u32,
    run_end_cp: u32,
    run_text: &str,
    run_source_chpx_id: Option<u32>,
    formatting_fingerprint: u64,
    formatting_sequence_fingerprint: u64,
    state: RunRevisionState,
    cancel_dual_micro_noop: bool,
    out: &mut Vec<RevisionCandidate>,
) {
    if !state.has_insertion && !state.has_deletion {
        return;
    }

    if state.has_insertion && state.has_deletion {
        let insertion_first = match (
            state
                .insertion_timestamp
                .and_then(|ts| ts.to_naive_datetime()),
            state
                .deletion_timestamp
                .and_then(|ts| ts.to_naive_datetime()),
        ) {
            (Some(ins), Some(del)) => ins <= del,
            _ => state.dual_insertion_first.unwrap_or(true),
        };

        // Treat a 1-CP dual run with identical metadata on both sides as a
        // no-op overlap anchor. Keep a zero-text insertion candidate so
        // adjacent same-metadata insertions can bridge across it, then prune
        // the surviving mid-paragraph standalones later.
        if cancel_dual_micro_noop {
            out.push(RevisionCandidate {
                signature: RedlineSignature {
                    revision_type: RevisionType::Insertion,
                    author_index: state.insertion_author,
                    timestamp: state.insertion_timestamp,
                    stack: None,
                },
                start_cp: run_start_cp,
                end_cp: run_end_cp,
                segments: vec![SourceSegment {
                    start_cp: run_start_cp,
                    end_cp: run_end_cp,
                    text: String::new(),
                    formatting_fingerprint,
                    formatting_sequence_fingerprint,
                    source_chpx_id: run_source_chpx_id,
                    segment_author_index: state.insertion_author,
                    segment_timestamp: state.insertion_timestamp,
                }],
            });
            return;
        }

        // LibreOffice's broken-documents path drops the delete side when
        // deletion is timestamp-sorted before insertion on the same range.
        if !insertion_first
            && (state.insertion_timestamp.is_some() || state.deletion_timestamp.is_some())
        {
            out.push(RevisionCandidate {
                signature: RedlineSignature {
                    revision_type: RevisionType::Insertion,
                    author_index: state.insertion_author,
                    timestamp: state.insertion_timestamp,
                    stack: None,
                },
                start_cp: run_start_cp,
                end_cp: run_end_cp,
                segments: vec![SourceSegment {
                    start_cp: run_start_cp,
                    end_cp: run_end_cp,
                    text: run_text.to_string(),
                    formatting_fingerprint,
                    formatting_sequence_fingerprint,
                    source_chpx_id: run_source_chpx_id,
                    segment_author_index: state.insertion_author,
                    segment_timestamp: state.insertion_timestamp,
                }],
            });
            return;
        }

        if cancel_same_author_dual_enabled()
            && state.insertion_author.is_some()
            && state.insertion_author == state.deletion_author
        {
            return;
        }

        let insertion = RevisionCandidate {
            signature: RedlineSignature {
                revision_type: RevisionType::Insertion,
                author_index: state.insertion_author,
                timestamp: state.insertion_timestamp,
                stack: Some(Box::new(StackMetadata {
                    revision_type: RevisionType::Deletion,
                    author_index: state.deletion_author,
                    timestamp: state.deletion_timestamp,
                    next: None,
                })),
            },
            start_cp: run_start_cp,
            end_cp: run_end_cp,
            segments: vec![SourceSegment {
                start_cp: run_start_cp,
                end_cp: run_end_cp,
                text: run_text.to_string(),
                formatting_fingerprint,
                formatting_sequence_fingerprint,
                source_chpx_id: run_source_chpx_id,
                segment_author_index: state.insertion_author,
                segment_timestamp: state.insertion_timestamp,
            }],
        };

        let deletion = RevisionCandidate {
            signature: RedlineSignature {
                revision_type: RevisionType::Deletion,
                author_index: state.deletion_author,
                timestamp: state.deletion_timestamp,
                stack: Some(Box::new(StackMetadata {
                    revision_type: RevisionType::Insertion,
                    author_index: state.insertion_author,
                    timestamp: state.insertion_timestamp,
                    next: None,
                })),
            },
            start_cp: run_start_cp,
            end_cp: run_end_cp,
            segments: vec![SourceSegment {
                start_cp: run_start_cp,
                end_cp: run_end_cp,
                text: run_text.to_string(),
                formatting_fingerprint,
                formatting_sequence_fingerprint,
                source_chpx_id: run_source_chpx_id,
                segment_author_index: state.deletion_author,
                segment_timestamp: state.deletion_timestamp,
            }],
        };

        out.push(insertion);
        out.push(deletion);
        return;
    }

    if state.has_insertion {
        out.push(RevisionCandidate {
            signature: RedlineSignature {
                revision_type: RevisionType::Insertion,
                author_index: state.insertion_author,
                timestamp: state.insertion_timestamp,
                stack: None,
            },
            start_cp: run_start_cp,
            end_cp: run_end_cp,
            segments: vec![SourceSegment {
                start_cp: run_start_cp,
                end_cp: run_end_cp,
                text: run_text.to_string(),
                formatting_fingerprint,
                formatting_sequence_fingerprint,
                source_chpx_id: run_source_chpx_id,
                segment_author_index: state.insertion_author,
                segment_timestamp: state.insertion_timestamp,
            }],
        });
    }

    if state.has_deletion {
        out.push(RevisionCandidate {
            signature: RedlineSignature {
                revision_type: RevisionType::Deletion,
                author_index: state.deletion_author,
                timestamp: state.deletion_timestamp,
                stack: None,
            },
            start_cp: run_start_cp,
            end_cp: run_end_cp,
            segments: vec![SourceSegment {
                start_cp: run_start_cp,
                end_cp: run_end_cp,
                text: run_text.to_string(),
                formatting_fingerprint,
                formatting_sequence_fingerprint,
                source_chpx_id: run_source_chpx_id,
                segment_author_index: state.deletion_author,
                segment_timestamp: state.deletion_timestamp,
            }],
        });
    }
}

fn should_cancel_dual_micro_noop(
    idx: usize,
    runs: &[ChpxRun],
    state: &RunRevisionState,
    effective_insertion_timestamp: Option<Dttm>,
) -> bool {
    if !cancel_dual_micro_noop_enabled() {
        return false;
    }
    if !state.has_insertion || !state.has_deletion {
        return false;
    }
    if state.insertion_author != state.deletion_author
        || state.insertion_timestamp != state.deletion_timestamp
    {
        return false;
    }

    let run = &runs[idx];
    if run.end_cp.saturating_sub(run.start_cp) != 1 {
        return false;
    }

    let Some(prev) = idx.checked_sub(1).and_then(|prev_idx| runs.get(prev_idx)) else {
        return false;
    };
    let Some(next) = runs.get(idx + 1) else {
        return false;
    };
    if prev.end_cp != run.start_cp || next.start_cp != run.end_cp {
        return false;
    }

    same_meta_pure_insertion(prev, state.insertion_author, effective_insertion_timestamp)
        && same_meta_pure_insertion(next, state.insertion_author, effective_insertion_timestamp)
}

fn same_meta_pure_insertion(run: &ChpxRun, author: Option<u16>, timestamp: Option<Dttm>) -> bool {
    let marks = collect_revision_sprms(run);
    let has_insertion = marks.has_insertion
        || marks.insertion_author.is_some()
        || marks.insertion_timestamp.is_some();
    let has_deletion =
        marks.has_deletion || marks.deletion_author.is_some() || marks.deletion_timestamp.is_some();

    has_insertion
        && !has_deletion
        && marks.insertion_author == author
        && marks.insertion_timestamp == timestamp
}

fn maybe_shift_timestampless_deletion_boundary(
    idx: usize,
    runs: &[ChpxRun],
    state: &RunRevisionState,
) -> Option<(u32, u32, String)> {
    if !shift_timestampless_deletion_boundary_enabled() {
        return None;
    }
    if state.has_insertion || !state.has_deletion || state.deletion_timestamp.is_some() {
        return None;
    }

    let run = runs.get(idx)?;
    let prev = idx.checked_sub(1).and_then(|value| runs.get(value))?;
    let next = runs.get(idx + 1)?;
    if prev.end_cp != run.start_cp || next.start_cp != run.end_cp {
        return None;
    }

    let prev_marks = collect_revision_sprms(prev);
    let prev_has_insertion = prev_marks.has_insertion
        || prev_marks.insertion_author.is_some()
        || prev_marks.insertion_timestamp.is_some();
    let prev_has_deletion = prev_marks.has_deletion
        || prev_marks.deletion_author.is_some()
        || prev_marks.deletion_timestamp.is_some();
    if !prev_has_insertion || prev_has_deletion {
        return None;
    }

    let next_marks = collect_revision_sprms(next);
    let next_has_revision = next_marks.has_insertion
        || next_marks.has_deletion
        || next_marks.insertion_author.is_some()
        || next_marks.insertion_timestamp.is_some()
        || next_marks.deletion_author.is_some()
        || next_marks.deletion_timestamp.is_some();
    if next_has_revision {
        return None;
    }

    let run_len = run.text.chars().count();
    if run_len < shift_timestampless_deletion_boundary_min_len() {
        return None;
    }

    let mut text_chars = run.text.chars();
    let first_char = text_chars.next()?;
    let second_char = text_chars.next();
    let next_char = next.text.chars().next()?;
    let leading_ok = first_char.is_ascii_alphanumeric()
        || (first_char.is_whitespace()
            && second_char.is_some_and(|value| value.is_ascii_alphanumeric()));
    if !leading_ok || !next_char.is_ascii_alphanumeric() {
        return None;
    }

    let mut chars = run.text.chars();
    chars.next()?;
    let mut shifted_text: String = chars.collect();
    shifted_text.push(next_char);

    let shifted_start = run.start_cp.saturating_add(1);
    let shifted_end = run.end_cp.saturating_add(1);
    if shifted_start >= shifted_end {
        return None;
    }

    Some((shifted_start, shifted_end, shifted_text))
}

fn shift_timestampless_deletion_boundary_enabled() -> bool {
    env::var("DOC_RL_SHIFT_TIMELESS_DELETION_BOUNDARY")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn shift_timestampless_deletion_boundary_min_len() -> usize {
    env::var("DOC_RL_SHIFT_TIMELESS_DELETION_BOUNDARY_MIN_LEN")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8)
}

fn cancel_same_author_dual_enabled() -> bool {
    env::var("DOC_RL_CANCEL_SAME_AUTHOR_DUAL")
        .ok()
        .as_deref()
        .is_some_and(|value| value == "1")
}

fn cancel_dual_micro_noop_enabled() -> bool {
    env::var("DOC_RL_CANCEL_DUAL_MICRO_NOOP")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn build_redlines(candidates: Vec<RevisionCandidate>) -> Vec<BuiltRedline> {
    let mut redlines = Vec::<BuiltRedline>::new();

    for candidate in candidates {
        if let Some(index) = redlines.iter().rposition(|existing| {
            existing.signature.revision_type == candidate.signature.revision_type
        }) {
            let existing = &redlines[index];
            if can_combine(
                &existing.signature,
                existing.start_cp,
                existing.end_cp,
                &candidate.signature,
                candidate.start_cp,
                candidate.end_cp,
            ) {
                absorb(&mut redlines[index], candidate);
                if build_cascade_enabled() {
                    build_cascade_merge(&mut redlines, index);
                }
                continue;
            }
        }

        redlines.push(candidate.into_redline());
    }

    if resolve_insert_inside_delete_enabled() {
        resolve_insert_inside_delete(&mut redlines);
    }
    redlines
}

fn resolve_insert_inside_delete_enabled() -> bool {
    env::var("DOC_RL_RESOLVE_OVERLAP_INSIDE_DELETE")
        .ok()
        .as_deref()
        .is_some_and(|value| value == "1")
}

fn clip_segments(segments: &[SourceSegment], start_cp: u32, end_cp: u32) -> Vec<SourceSegment> {
    let mut out = Vec::new();
    if start_cp >= end_cp {
        return out;
    }
    for segment in segments {
        let overlap_start = start_cp.max(segment.start_cp);
        let overlap_end = end_cp.min(segment.end_cp);
        if overlap_start >= overlap_end {
            continue;
        }
        let local_start = (overlap_start - segment.start_cp) as usize;
        let local_end = (overlap_end - segment.start_cp) as usize;
        out.push(SourceSegment {
            start_cp: overlap_start,
            end_cp: overlap_end,
            text: slice_chars(&segment.text, local_start, local_end),
            formatting_fingerprint: segment.formatting_fingerprint,
            formatting_sequence_fingerprint: segment.formatting_sequence_fingerprint,
            source_chpx_id: segment.source_chpx_id,
            segment_author_index: segment.segment_author_index,
            segment_timestamp: segment.segment_timestamp,
        });
    }
    out
}

fn resolve_insert_inside_delete(redlines: &mut Vec<BuiltRedline>) {
    redlines.sort_by(compare_redline_for_lo_pass);
    let mut i = 0usize;
    while i < redlines.len() {
        if redlines[i].signature.revision_type != RevisionType::Insertion {
            i += 1;
            continue;
        }
        let ins_start = redlines[i].start_cp;
        let ins_end = redlines[i].end_cp;
        let mut j = 0usize;
        while j < redlines.len() {
            if j == i {
                j += 1;
                continue;
            }
            if redlines[j].signature.revision_type != RevisionType::Deletion {
                j += 1;
                continue;
            }
            let del_start = redlines[j].start_cp;
            let del_end = redlines[j].end_cp;
            if del_start < ins_start && ins_end < del_end {
                let left_segments = clip_segments(&redlines[j].segments, del_start, ins_start);
                let right_segments = clip_segments(&redlines[j].segments, ins_end, del_end);
                if !left_segments.is_empty() {
                    redlines[j].start_cp = del_start;
                    redlines[j].end_cp = ins_start;
                    redlines[j].segments = left_segments;
                    if !right_segments.is_empty() {
                        let mut right = redlines[j].clone();
                        right.start_cp = ins_end;
                        right.end_cp = del_end;
                        right.segments = right_segments;
                        redlines.insert(j + 1, right);
                        if j < i {
                            i += 1;
                        }
                    }
                } else if !right_segments.is_empty() {
                    redlines[j].start_cp = ins_end;
                    redlines[j].end_cp = del_end;
                    redlines[j].segments = right_segments;
                }
            }
            j += 1;
        }
        i += 1;
    }
}

/// After absorbing a candidate into redlines[index], check whether the now-extended
/// entry can merge backward with its predecessor.  Repeat until no more merges are
/// possible.  This replicates LibreOffice's AppendRedline cascade: after a backward
/// extension (CollideStart), LO re-evaluates adjacency with the previous entry, which
/// can unlock further merges that plain compress_redlines would not catch in time to
/// prevent an intervening incompatible candidate from claiming the boundary.
fn build_cascade_merge(redlines: &mut Vec<BuiltRedline>, mut index: usize) {
    loop {
        if index == 0 {
            break;
        }
        let prev = index - 1;
        if redlines[prev].signature.revision_type != redlines[index].signature.revision_type {
            break;
        }
        if !can_combine(
            &redlines[prev].signature,
            redlines[prev].start_cp,
            redlines[prev].end_cp,
            &redlines[index].signature,
            redlines[index].start_cp,
            redlines[index].end_cp,
        ) {
            break;
        }
        let merged = redlines.remove(index);
        absorb(
            &mut redlines[prev],
            RevisionCandidate {
                signature: merged.signature,
                start_cp: merged.start_cp,
                end_cp: merged.end_cp,
                segments: merged.segments,
            },
        );
        index = prev;
    }
}

fn build_cascade_enabled() -> bool {
    env::var("DOC_RL_BUILD_CASCADE")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn compress_redlines(redlines: Vec<BuiltRedline>) -> Vec<BuiltRedline> {
    let mut sorted = redlines;
    sorted.sort_by(compare_redline_for_lo_pass);

    let mut compressed = Vec::<BuiltRedline>::new();
    for redline in sorted {
        if let Some(last) = compressed.last_mut()
            && can_combine(
                &last.signature,
                last.start_cp,
                last.end_cp,
                &redline.signature,
                redline.start_cp,
                redline.end_cp,
            )
        {
            absorb(
                last,
                RevisionCandidate {
                    signature: redline.signature,
                    start_cp: redline.start_cp,
                    end_cp: redline.end_cp,
                    segments: redline.segments,
                },
            );
            continue;
        }

        compressed.push(redline);
    }

    compressed
}

fn sort_candidates_for_lo_append(candidates: &mut [RevisionCandidate]) {
    let mut indexed: Vec<(usize, RevisionCandidate)> =
        candidates.iter().cloned().enumerate().collect();

    indexed.sort_by(|(left_index, left), (right_index, right)| {
        compare_candidate_for_lo_stack(left, right).then_with(|| left_index.cmp(right_index))
    });

    for (index, (_, candidate)) in indexed.into_iter().enumerate() {
        candidates[index] = candidate;
    }
}

fn compare_candidate_for_lo_stack(left: &RevisionCandidate, right: &RevisionCandidate) -> Ordering {
    left.start_cp
        .cmp(&right.start_cp)
        .then_with(|| left.end_cp.cmp(&right.end_cp))
        .then_with(|| compare_lo_timestamp(left.signature.timestamp, right.signature.timestamp))
        .then_with(|| {
            compare_revision_type_tiebreak(
                left.signature.revision_type,
                right.signature.revision_type,
            )
        })
}

fn compare_redline_for_lo_pass(left: &BuiltRedline, right: &BuiltRedline) -> Ordering {
    left.start_cp
        .cmp(&right.start_cp)
        .then_with(|| left.end_cp.cmp(&right.end_cp))
}

fn compare_lo_timestamp(left: Option<Dttm>, right: Option<Dttm>) -> Ordering {
    match (
        left.and_then(|value| value.to_naive_datetime()),
        right.and_then(|value| value.to_naive_datetime()),
    ) {
        (Some(a), Some(b)) => a.cmp(&b),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_revision_type_tiebreak(left: RevisionType, right: RevisionType) -> Ordering {
    match (left, right) {
        (RevisionType::Insertion, RevisionType::Deletion) => Ordering::Less,
        (RevisionType::Deletion, RevisionType::Insertion) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

fn absorb(redline: &mut BuiltRedline, candidate: RevisionCandidate) {
    redline.start_cp = redline.start_cp.min(candidate.start_cp);
    redline.end_cp = redline.end_cp.max(candidate.end_cp);

    // Anchor signature timestamp to the earliest (min) value to match
    // LibreOffice's grouping behavior: the compatibility window is measured
    // from the oldest run in the group, so it only gets stricter over time.
    if let (Some(existing), Some(incoming)) =
        (redline.signature.timestamp, candidate.signature.timestamp)
    {
        redline.signature.timestamp = Some(existing.min(incoming));
    }

    redline.segments.extend(candidate.segments);
    redline
        .segments
        .sort_by_key(|segment| (segment.start_cp, segment.end_cp));
}

fn type_order(revision_type: RevisionType) -> u8 {
    match revision_type {
        RevisionType::Insertion => 0,
        RevisionType::Deletion => 1,
    }
}

fn augment_alternate_content_duplicates(entries: &mut Vec<RevisionEntry>) {
    if !alternate_content_duplicate_enabled() {
        return;
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for window in entries.windows(4) {
        let [a, b, c, d] = window else { continue };

        if a.revision_type != RevisionType::Insertion
            || b.revision_type != RevisionType::Insertion
            || c.revision_type != RevisionType::Insertion
            || d.revision_type != RevisionType::Insertion
        {
            continue;
        }
        if a.timestamp.is_some()
            || b.timestamp.is_some()
            || c.timestamp.is_some()
            || d.timestamp.is_some()
        {
            continue;
        }
        if a.author != b.author || a.author != c.author || a.author != d.author {
            continue;
        }
        if a.start_cp < alternate_content_duplicate_min_cp() {
            continue;
        }
        if b.start_cp.saturating_sub(a.end_cp) != 2
            || c.start_cp.saturating_sub(b.end_cp) != 2
            || d.start_cp.saturating_sub(c.end_cp) != 2
        {
            continue;
        }

        let a_len = a.text.chars().count();
        let b_len = b.text.chars().count();
        let c_len = c.text.chars().count();
        let d_len = d.text.chars().count();
        if a_len != 2 || b_len < 4 || c_len != 1 || d_len < 4 {
            continue;
        }
        if !a.text.chars().all(|ch| ch.is_ascii_alphabetic()) {
            continue;
        }
        if !b
            .text
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic())
        {
            continue;
        }
        if !d
            .text
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic())
        {
            continue;
        }
        if c.text.chars().all(|ch| ch.is_ascii_alphanumeric()) {
            continue;
        }

        let base = format!("{}{}{}{}", a.text, b.text, c.text, d.text);
        if base.trim().is_empty() {
            continue;
        }

        additions.push(RevisionEntry {
            revision_type: RevisionType::Insertion,
            text: format!("{base}{base}"),
            author: a.author.clone(),
            timestamp: None,
            start_cp: a.start_cp,
            end_cp: d.end_cp,
            paragraph_index: a.paragraph_index,
            char_offset: a.char_offset,
            context: a.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn alternate_content_duplicate_enabled() -> bool {
    env::var("DOC_RL_ALT_CONTENT_DUPLICATE")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn alternate_content_duplicate_min_cp() -> u32 {
    env::var("DOC_RL_ALT_CONTENT_DUPLICATE_MIN_CP")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(200_000)
}

fn augment_midword_adjacent_insertions(entries: &mut Vec<RevisionEntry>) {
    if !midword_adjacent_insertion_alias_enabled() || entries.len() < 2 {
        return;
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for window in entries.windows(2) {
        let [left, right] = window else { continue };
        if left.revision_type != RevisionType::Insertion
            || right.revision_type != RevisionType::Insertion
        {
            continue;
        }
        if left.author != right.author {
            continue;
        }
        if left.end_cp != right.start_cp {
            continue;
        }

        let (Some(left_para), Some(right_para), Some(left_offset), Some(right_offset)) = (
            left.paragraph_index,
            right.paragraph_index,
            left.char_offset,
            right.char_offset,
        ) else {
            continue;
        };
        if left_para != right_para {
            continue;
        }

        let left_len = left.text.chars().count() as u32;
        if right_offset != left_offset.saturating_add(left_len) {
            continue;
        }
        if left.text.chars().count() < 8 || right.text.chars().count() < 4 {
            continue;
        }

        let left_edge = left.text.chars().next_back();
        let right_edge = right.text.chars().next();
        let midword = matches!(left_edge, Some(ch) if ch.is_ascii_alphabetic())
            && matches!(right_edge, Some(ch) if ch.is_ascii_alphabetic());
        if !midword {
            continue;
        }

        let merged_text = format!("{}{}", left.text, right.text);
        let key = (
            RevisionType::Insertion,
            left.start_cp,
            right.end_cp,
            merged_text.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Insertion,
            text: merged_text,
            author: left.author.clone(),
            timestamp: left.timestamp.clone(),
            start_cp: left.start_cp,
            end_cp: right.end_cp,
            paragraph_index: left.paragraph_index,
            char_offset: left.char_offset,
            context: left.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_whitespace_adjacent_insertions_across_empty_companions(
    entries: &mut Vec<RevisionEntry>,
) {
    if entries.len() < 3 {
        return;
    }

    let mut by_end = HashMap::<(u32, Option<String>, Option<String>, Option<u32>), usize>::new();
    let mut by_start = HashMap::<(u32, Option<String>, Option<String>, Option<u32>), usize>::new();
    for (idx, entry) in entries.iter().enumerate() {
        if entry.revision_type != RevisionType::Insertion || !text_has_alnum(&entry.text) {
            continue;
        }

        let end_key = (
            entry.end_cp,
            entry.author.clone(),
            entry.timestamp.clone(),
            entry.paragraph_index,
        );
        match by_end.get(&end_key).copied() {
            Some(existing_idx)
                if text_quality_score(&entries[existing_idx].text)
                    >= text_quality_score(&entry.text) => {}
            _ => {
                by_end.insert(end_key, idx);
            }
        }

        let start_key = (
            entry.start_cp,
            entry.author.clone(),
            entry.timestamp.clone(),
            entry.paragraph_index,
        );
        match by_start.get(&start_key).copied() {
            Some(existing_idx)
                if text_quality_score(&entries[existing_idx].text)
                    >= text_quality_score(&entry.text) => {}
            _ => {
                by_start.insert(start_key, idx);
            }
        }
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for middle in entries.iter() {
        if middle.revision_type != RevisionType::Insertion {
            continue;
        }
        if !middle.text.is_empty() || middle.start_cp != middle.end_cp {
            continue;
        }

        let key = (
            middle.start_cp,
            middle.author.clone(),
            middle.timestamp.clone(),
            middle.paragraph_index,
        );
        let Some(&left_idx) = by_end.get(&key) else {
            continue;
        };
        let Some(&right_idx) = by_start.get(&key) else {
            continue;
        };
        let left = &entries[left_idx];
        let right = &entries[right_idx];
        if left.start_cp >= left.end_cp || right.start_cp >= right.end_cp {
            continue;
        }
        if left.revision_type != RevisionType::Insertion
            || right.revision_type != RevisionType::Insertion
        {
            continue;
        }
        if left.author != right.author || left.timestamp != right.timestamp {
            continue;
        }

        let (Some(left_para), Some(right_para), Some(left_offset), Some(right_offset)) = (
            left.paragraph_index,
            right.paragraph_index,
            left.char_offset,
            right.char_offset,
        ) else {
            continue;
        };
        if left_para != right_para {
            continue;
        }

        let left_len = left.text.chars().count() as u32;
        if right_offset != left_offset.saturating_add(left_len) {
            continue;
        }
        if left.text.chars().filter(|ch| ch.is_alphanumeric()).count() < 8
            || right.text.chars().filter(|ch| ch.is_alphanumeric()).count() < 8
        {
            continue;
        }
        if !left
            .text
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        if !right
            .text
            .chars()
            .find(|ch| !ch.is_whitespace())
            .is_some_and(|ch| ch.is_ascii_lowercase())
        {
            continue;
        }

        let merged_text = format!("{}{}", left.text, right.text);
        let key = (
            RevisionType::Insertion,
            left.start_cp,
            right.end_cp,
            merged_text.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Insertion,
            text: merged_text,
            author: left.author.clone(),
            timestamp: left.timestamp.clone(),
            start_cp: left.start_cp,
            end_cp: right.end_cp,
            paragraph_index: left.paragraph_index,
            char_offset: left.char_offset,
            context: left.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_punctuation_adjacent_entries(entries: &mut Vec<RevisionEntry>) {
    if entries.len() < 2 {
        return;
    }

    let mut by_start = HashMap::<
        (
            RevisionType,
            u32,
            Option<String>,
            Option<String>,
            Option<u32>,
        ),
        usize,
    >::new();
    for (idx, entry) in entries.iter().enumerate() {
        let key = (
            entry.revision_type,
            entry.start_cp,
            entry.author.clone(),
            entry.timestamp.clone(),
            entry.paragraph_index,
        );
        match by_start.get(&key).copied() {
            Some(existing_idx)
                if text_quality_score(&entries[existing_idx].text)
                    >= text_quality_score(&entry.text) => {}
            _ => {
                by_start.insert(key, idx);
            }
        }
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for left in entries.iter() {
        let key = (
            left.revision_type,
            left.end_cp,
            left.author.clone(),
            left.timestamp.clone(),
            left.paragraph_index,
        );
        let Some(&right_idx) = by_start.get(&key) else {
            continue;
        };
        let right = &entries[right_idx];
        if !punctuation_adjacent_merge_boundary(left, right) {
            continue;
        }

        let merged_text = format!("{}{}", left.text, right.text);
        let key = (
            left.revision_type,
            left.start_cp,
            right.end_cp,
            merged_text.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: left.revision_type,
            text: merged_text,
            author: left.author.clone(),
            timestamp: left.timestamp.clone(),
            start_cp: left.start_cp,
            end_cp: right.end_cp,
            paragraph_index: left.paragraph_index,
            char_offset: left.char_offset,
            context: left.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_sentence_adjacent_insertions_across_timestamp_transition(
    entries: &mut Vec<RevisionEntry>,
) {
    if entries.len() < 2 {
        return;
    }

    let mut by_start = HashMap::<(u32, Option<String>, Option<u32>), usize>::new();
    for (idx, entry) in entries.iter().enumerate() {
        if entry.revision_type != RevisionType::Insertion {
            continue;
        }
        let key = (entry.start_cp, entry.author.clone(), entry.paragraph_index);
        match by_start.get(&key).copied() {
            Some(existing_idx)
                if text_quality_score(&entries[existing_idx].text)
                    >= text_quality_score(&entry.text) => {}
            _ => {
                by_start.insert(key, idx);
            }
        }
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for left in entries.iter() {
        if left.revision_type != RevisionType::Insertion {
            continue;
        }
        let key = (left.end_cp, left.author.clone(), left.paragraph_index);
        let Some(&right_idx) = by_start.get(&key) else {
            continue;
        };
        let right = &entries[right_idx];
        if right.revision_type != RevisionType::Insertion || left.timestamp == right.timestamp {
            continue;
        }

        let (Some(left_para), Some(right_para), Some(left_offset), Some(right_offset)) = (
            left.paragraph_index,
            right.paragraph_index,
            left.char_offset,
            right.char_offset,
        ) else {
            continue;
        };
        if left_para != right_para {
            continue;
        }

        let left_len = left.text.chars().count() as u32;
        if right_offset != left_offset.saturating_add(left_len) {
            continue;
        }
        if !left
            .text
            .trim_end()
            .chars()
            .next_back()
            .is_some_and(|ch| matches!(ch, '.' | '!' | '?' | ';' | ':'))
        {
            continue;
        }
        if !right
            .text
            .chars()
            .find(|ch| !ch.is_whitespace())
            .is_some_and(|ch| ch.is_ascii_uppercase())
        {
            continue;
        }
        if !text_has_alnum(&left.text) || !text_has_alnum(&right.text) {
            continue;
        }

        let merged_text = format!("{}{}", left.text, right.text);
        let key = (
            RevisionType::Insertion,
            left.start_cp,
            right.end_cp,
            merged_text.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Insertion,
            text: merged_text,
            author: left.author.clone(),
            timestamp: left.timestamp.clone(),
            start_cp: left.start_cp,
            end_cp: right.end_cp,
            paragraph_index: left.paragraph_index,
            char_offset: left.char_offset,
            context: left.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_defined_term_adjacent_insertions_after_short_prefix(entries: &mut Vec<RevisionEntry>) {
    if entries.len() < 3 {
        return;
    }

    let mut by_start = HashMap::<(u32, Option<String>, Option<u32>), usize>::new();
    let mut by_end = HashMap::<(u32, Option<String>, Option<u32>), usize>::new();
    for (idx, entry) in entries.iter().enumerate() {
        if entry.revision_type != RevisionType::Insertion {
            continue;
        }
        let start_key = (entry.start_cp, entry.author.clone(), entry.paragraph_index);
        match by_start.get(&start_key).copied() {
            Some(existing_idx)
                if text_quality_score(&entries[existing_idx].text)
                    >= text_quality_score(&entry.text) => {}
            _ => {
                by_start.insert(start_key, idx);
            }
        }
        let end_key = (entry.end_cp, entry.author.clone(), entry.paragraph_index);
        match by_end.get(&end_key).copied() {
            Some(existing_idx)
                if text_quality_score(&entries[existing_idx].text)
                    >= text_quality_score(&entry.text) => {}
            _ => {
                by_end.insert(end_key, idx);
            }
        }
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for middle in entries.iter() {
        if middle.revision_type != RevisionType::Insertion {
            continue;
        }

        let left_key = (
            middle.start_cp,
            middle.author.clone(),
            middle.paragraph_index,
        );
        let Some(&left_idx) = by_end.get(&left_key) else {
            continue;
        };
        let left = &entries[left_idx];
        if left.revision_type != RevisionType::Insertion {
            continue;
        }

        let right_key = (middle.end_cp, middle.author.clone(), middle.paragraph_index);
        let Some(&right_idx) = by_start.get(&right_key) else {
            continue;
        };
        let right = &entries[right_idx];
        if right.revision_type != RevisionType::Insertion {
            continue;
        }
        if left.timestamp != middle.timestamp || middle.timestamp == right.timestamp {
            continue;
        }

        let (
            Some(left_para),
            Some(middle_para),
            Some(right_para),
            Some(left_offset),
            Some(middle_offset),
            Some(right_offset),
        ) = (
            left.paragraph_index,
            middle.paragraph_index,
            right.paragraph_index,
            left.char_offset,
            middle.char_offset,
            right.char_offset,
        )
        else {
            continue;
        };
        if left_para != middle_para || middle_para != right_para {
            continue;
        }

        let left_len = left.text.chars().count() as u32;
        let middle_len = middle.text.chars().count() as u32;
        if middle_offset != left_offset.saturating_add(left_len)
            || right_offset != middle_offset.saturating_add(middle_len)
        {
            continue;
        }

        if left
            .text
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .count()
            < 3
            || left
                .text
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric())
                .count()
                > 5
        {
            continue;
        }
        if !left
            .text
            .chars()
            .next()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        if !left
            .text
            .chars()
            .find(|ch| !ch.is_whitespace())
            .is_some_and(|ch| ch.is_ascii_uppercase())
        {
            continue;
        }
        if !middle
            .text
            .chars()
            .find(|ch| !ch.is_whitespace())
            .is_some_and(|ch| ch.is_ascii_lowercase())
        {
            continue;
        }
        if !middle
            .text
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        if middle
            .text
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .count()
            < 20
            || right
                .text
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric())
                .count()
                < 20
        {
            continue;
        }
        if !right
            .text
            .chars()
            .find(|ch| !ch.is_whitespace())
            .is_some_and(|ch| ch.is_ascii_uppercase())
        {
            continue;
        }

        let merged_text = format!("{}{}", middle.text, right.text);
        let key = (
            RevisionType::Insertion,
            middle.start_cp,
            right.end_cp,
            merged_text.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Insertion,
            text: merged_text,
            author: middle.author.clone(),
            timestamp: right.timestamp.clone(),
            start_cp: middle.start_cp,
            end_cp: right.end_cp,
            paragraph_index: middle.paragraph_index,
            char_offset: middle.char_offset,
            context: middle.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_midword_adjacent_insertions_across_timestamp_transition(
    entries: &mut Vec<RevisionEntry>,
) {
    if entries.len() < 2 {
        return;
    }

    let mut by_start = HashMap::<(u32, Option<String>, Option<u32>), usize>::new();
    for (idx, entry) in entries.iter().enumerate() {
        if entry.revision_type != RevisionType::Insertion {
            continue;
        }
        let key = (entry.start_cp, entry.author.clone(), entry.paragraph_index);
        match by_start.get(&key).copied() {
            Some(existing_idx)
                if text_quality_score(&entries[existing_idx].text)
                    >= text_quality_score(&entry.text) => {}
            _ => {
                by_start.insert(key, idx);
            }
        }
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for left in entries.iter() {
        if left.revision_type != RevisionType::Insertion {
            continue;
        }
        let key = (left.end_cp, left.author.clone(), left.paragraph_index);
        let Some(&right_idx) = by_start.get(&key) else {
            continue;
        };
        let right = &entries[right_idx];
        if right.revision_type != RevisionType::Insertion || left.timestamp == right.timestamp {
            continue;
        }

        let (Some(left_para), Some(right_para), Some(left_offset), Some(right_offset)) = (
            left.paragraph_index,
            right.paragraph_index,
            left.char_offset,
            right.char_offset,
        ) else {
            continue;
        };
        if left_para != right_para {
            continue;
        }

        let left_len = left.text.chars().count() as u32;
        if right_offset != left_offset.saturating_add(left_len) {
            continue;
        }
        if left.text.chars().filter(|ch| ch.is_alphanumeric()).count() < 8
            || right.text.chars().filter(|ch| ch.is_alphanumeric()).count() < 4
        {
            continue;
        }
        if text_boundary_transition(&left.text, &right.text) {
            continue;
        }

        let left_last = left.text.chars().rev().find(|ch| !ch.is_whitespace());
        let right_first = right.text.chars().find(|ch| !ch.is_whitespace());
        if !matches!(left_last, Some(ch) if ch.is_ascii_lowercase())
            || !matches!(right_first, Some(ch) if ch.is_ascii_lowercase())
        {
            continue;
        }

        let merged_text = format!("{}{}", left.text, right.text);
        let key = (
            RevisionType::Insertion,
            left.start_cp,
            right.end_cp,
            merged_text.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Insertion,
            text: merged_text,
            author: left.author.clone(),
            timestamp: left.timestamp.clone(),
            start_cp: left.start_cp,
            end_cp: right.end_cp,
            paragraph_index: left.paragraph_index,
            char_offset: left.char_offset,
            context: left.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_short_token_insertions_across_timestamp_transition(entries: &mut Vec<RevisionEntry>) {
    if entries.len() < 3 {
        return;
    }

    let mut by_start = HashMap::<(u32, Option<String>, Option<u32>), usize>::new();
    for (idx, entry) in entries.iter().enumerate() {
        if entry.revision_type != RevisionType::Insertion {
            continue;
        }
        let key = (entry.start_cp, entry.author.clone(), entry.paragraph_index);
        match by_start.get(&key).copied() {
            Some(existing_idx)
                if text_quality_score(&entries[existing_idx].text)
                    >= text_quality_score(&entry.text) => {}
            _ => {
                by_start.insert(key, idx);
            }
        }
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for left in entries.iter() {
        if left.revision_type != RevisionType::Insertion {
            continue;
        }
        let middle_key = (left.end_cp, left.author.clone(), left.paragraph_index);
        let Some(&middle_idx) = by_start.get(&middle_key) else {
            continue;
        };
        let middle = &entries[middle_idx];
        if middle.revision_type != RevisionType::Insertion {
            continue;
        }
        let right_key = (middle.end_cp, left.author.clone(), left.paragraph_index);
        let Some(&right_idx) = by_start.get(&right_key) else {
            continue;
        };
        let right = &entries[right_idx];
        if right.revision_type != RevisionType::Insertion {
            continue;
        }
        if left.timestamp == middle.timestamp && middle.timestamp == right.timestamp {
            continue;
        }
        if left.timestamp == right.timestamp {
            continue;
        }

        let (
            Some(left_para),
            Some(middle_para),
            Some(right_para),
            Some(left_offset),
            Some(middle_offset),
            Some(right_offset),
        ) = (
            left.paragraph_index,
            middle.paragraph_index,
            right.paragraph_index,
            left.char_offset,
            middle.char_offset,
            right.char_offset,
        )
        else {
            continue;
        };
        if left_para != middle_para || middle_para != right_para {
            continue;
        }

        let left_len = left.text.chars().count() as u32;
        let middle_len = middle.text.chars().count() as u32;
        if middle_offset != left_offset.saturating_add(left_len)
            || right_offset != middle_offset.saturating_add(middle_len)
        {
            continue;
        }

        if left
            .text
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .count()
            < 20
            || right
                .text
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric())
                .count()
                < 12
        {
            continue;
        }
        if !left
            .text
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        let middle_alnum = middle
            .text
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .count();
        if middle_alnum == 0 || middle_alnum > 4 {
            continue;
        }
        if !middle.text.chars().any(|ch| ch.is_ascii_digit()) {
            continue;
        }
        if !right
            .text
            .chars()
            .find(|ch| !ch.is_whitespace())
            .is_some_and(|ch| ch.is_ascii_lowercase())
        {
            continue;
        }

        let merged_text = format!("{}{}{}", left.text, middle.text, right.text);
        let key = (
            RevisionType::Insertion,
            left.start_cp,
            right.end_cp,
            merged_text.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Insertion,
            text: merged_text,
            author: left.author.clone(),
            timestamp: left.timestamp.clone(),
            start_cp: left.start_cp,
            end_cp: right.end_cp,
            paragraph_index: left.paragraph_index,
            char_offset: left.char_offset,
            context: left.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_label_amount_line_item_aliases(entries: &mut Vec<RevisionEntry>) {
    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for entry in entries.iter() {
        if entry.revision_type != RevisionType::Insertion {
            continue;
        }
        let Some((label, amount)) = split_label_amount_alias(&entry.text) else {
            continue;
        };

        for alias in [label, amount] {
            let key = (
                RevisionType::Insertion,
                entry.start_cp,
                entry.end_cp,
                alias.clone(),
            );
            if existing.contains(&key) {
                continue;
            }

            existing.insert(key);
            additions.push(RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: alias,
                author: entry.author.clone(),
                timestamp: entry.timestamp.clone(),
                start_cp: entry.start_cp,
                end_cp: entry.end_cp,
                paragraph_index: entry.paragraph_index,
                char_offset: entry.char_offset,
                context: entry.context.clone(),
            });
        }
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_sentence_clause_aliases_from_tail_evidence(entries: &mut Vec<RevisionEntry>) {
    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for entry in entries.iter() {
        if entry.revision_type != RevisionType::Insertion || !text_has_alnum(&entry.text) {
            continue;
        }
        let Some((left_clause, right_clause)) =
            split_sentence_aliases_from_tail_evidence(entry, entries)
        else {
            continue;
        };

        for alias in [left_clause, right_clause] {
            let key = (
                RevisionType::Insertion,
                entry.start_cp,
                entry.end_cp,
                alias.clone(),
            );
            if existing.contains(&key) {
                continue;
            }

            existing.insert(key);
            additions.push(RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: alias,
                author: entry.author.clone(),
                timestamp: entry.timestamp.clone(),
                start_cp: entry.start_cp,
                end_cp: entry.end_cp,
                paragraph_index: entry.paragraph_index,
                char_offset: entry.char_offset,
                context: entry.context.clone(),
            });
        }
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn midword_adjacent_insertion_alias_enabled() -> bool {
    env::var("DOC_RL_MIDWORD_ADJ_INSERTION_ALIAS")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn augment_mirrored_deletion_prefix_clips(entries: &mut Vec<RevisionEntry>) {
    if !mirrored_deletion_prefix_clip_enabled() || entries.len() < 3 {
        return;
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();

    for (idx, left) in entries.iter().enumerate() {
        if left.revision_type != RevisionType::Deletion || !text_has_alnum(&left.text) {
            continue;
        }

        let mirrored = entries.iter().any(|candidate| {
            candidate.revision_type == RevisionType::Insertion
                && candidate.start_cp == left.start_cp
                && candidate.end_cp == left.end_cp
                && candidate.text == left.text
        });
        if !mirrored {
            continue;
        }

        let clip_chars = left.text.chars().count();
        if clip_chars < 3 {
            continue;
        }

        let mut seek_cp = left.end_cp;
        let mut right = None;
        for candidate in entries.iter().skip(idx + 1) {
            if candidate.start_cp < seek_cp {
                continue;
            }
            if candidate.start_cp > seek_cp {
                break;
            }

            if candidate.revision_type == RevisionType::Insertion {
                if text_has_alnum(&candidate.text) {
                    right = None;
                    break;
                }
                seek_cp = candidate.end_cp;
                continue;
            }

            if candidate.revision_type == RevisionType::Deletion
                && candidate.author == left.author
                && candidate.timestamp == left.timestamp
                && text_has_alnum(&candidate.text)
            {
                right = Some(candidate);
            }
            break;
        }

        let Some(right) = right else {
            continue;
        };

        let right_chars: Vec<char> = right.text.chars().collect();
        if clip_chars >= right_chars.len() {
            continue;
        }

        if !right_chars[clip_chars - 1].is_ascii_alphabetic()
            || !right_chars[clip_chars].is_ascii_alphabetic()
        {
            continue;
        }

        let alias: String = right_chars[clip_chars..].iter().collect();
        if alias.trim().is_empty() {
            continue;
        }

        let key = (
            RevisionType::Deletion,
            right.start_cp,
            right.end_cp,
            alias.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Deletion,
            text: alias,
            author: right.author.clone(),
            timestamp: right.timestamp.clone(),
            start_cp: right.start_cp,
            end_cp: right.end_cp,
            paragraph_index: right.paragraph_index,
            char_offset: right.char_offset,
            context: right.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn mirrored_deletion_prefix_clip_enabled() -> bool {
    env::var("DOC_RL_MIRRORED_DELETION_PREFIX_CLIP")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn augment_ordinal_suffix_deletion_tails(entries: &mut Vec<RevisionEntry>) {
    if entries.len() < 4 {
        return;
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mirrored_insertions = entries
        .iter()
        .filter(|entry| entry.revision_type == RevisionType::Insertion)
        .map(|entry| (entry.start_cp, entry.end_cp, entry.text.clone()))
        .collect::<HashSet<_>>();

    let raw_deletions: Vec<&RevisionEntry> = entries
        .iter()
        .filter(|entry| entry.revision_type == RevisionType::Deletion)
        .collect();
    let deletions = collapse_same_span_variants(raw_deletions);

    let mut by_start =
        HashMap::<(u32, Option<String>, Option<String>, Option<u32>), Vec<&RevisionEntry>>::new();
    let mut by_end =
        HashMap::<(u32, Option<String>, Option<String>, Option<u32>), Vec<&RevisionEntry>>::new();
    for entry in &deletions {
        by_start
            .entry((
                entry.start_cp,
                entry.author.clone(),
                entry.timestamp.clone(),
                entry.paragraph_index,
            ))
            .or_default()
            .push(*entry);
        by_end
            .entry((
                entry.end_cp,
                entry.author.clone(),
                entry.timestamp.clone(),
                entry.paragraph_index,
            ))
            .or_default()
            .push(*entry);
    }

    let mut additions = Vec::<RevisionEntry>::new();

    for suffix in &deletions {
        if !is_isolated_ordinal_suffix(&suffix.text) {
            continue;
        }
        if !mirrored_insertions.contains(&(suffix.start_cp, suffix.end_cp, suffix.text.clone())) {
            continue;
        }

        let meta_key = (
            suffix.start_cp,
            suffix.author.clone(),
            suffix.timestamp.clone(),
            suffix.paragraph_index,
        );
        let has_digit_prefix = by_end.get(&meta_key).is_some_and(|candidates| {
            candidates.iter().any(|candidate| {
                candidate
                    .text
                    .trim_end()
                    .chars()
                    .next_back()
                    .is_some_and(|ch| ch.is_ascii_digit())
            })
        });
        if !has_digit_prefix {
            continue;
        }

        let mut current_cp = suffix.end_cp;
        let mut merged_text = String::new();
        let mut fragments = 0usize;
        let mut saw_alnum = false;

        loop {
            let start_key = (
                current_cp,
                suffix.author.clone(),
                suffix.timestamp.clone(),
                suffix.paragraph_index,
            );
            let Some(candidates) = by_start.get(&start_key) else {
                break;
            };

            let mut best: Option<&RevisionEntry> = None;
            for candidate in candidates {
                if candidate.end_cp <= current_cp {
                    continue;
                }
                if fragments == 0 && !candidate.text.chars().all(|ch| ch.is_whitespace()) {
                    continue;
                }
                match best {
                    Some(existing_best) => {
                        let candidate_key = (text_quality_score(&candidate.text), candidate.end_cp);
                        let best_key = (
                            text_quality_score(&existing_best.text),
                            existing_best.end_cp,
                        );
                        if candidate_key > best_key {
                            best = Some(candidate);
                        }
                    }
                    None => best = Some(candidate),
                }
            }

            let Some(best) = best else {
                break;
            };

            merged_text.push_str(&best.text);
            current_cp = best.end_cp;
            fragments += 1;
            saw_alnum |= text_has_alnum(&best.text);
        }

        if fragments < 3 || !saw_alnum {
            continue;
        }
        if !merged_text
            .chars()
            .next()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        if text_quality_score(&merged_text).0 < 24 {
            continue;
        }
        if !merged_text
            .trim_end()
            .chars()
            .next_back()
            .is_some_and(|ch| matches!(ch, '.' | ';' | ':' | ',' | ')'))
        {
            continue;
        }

        let key = (
            RevisionType::Deletion,
            suffix.end_cp,
            current_cp,
            merged_text.clone(),
        );
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Deletion,
            text: merged_text,
            author: suffix.author.clone(),
            timestamp: suffix.timestamp.clone(),
            start_cp: suffix.end_cp,
            end_cp: current_cp,
            paragraph_index: suffix.paragraph_index,
            char_offset: suffix
                .char_offset
                .map(|offset| offset.saturating_add(suffix.text.chars().count() as u32)),
            context: suffix.context.clone(),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_ordinal_suffix_prefix_aliases(entries: &mut Vec<RevisionEntry>) {
    if entries.len() < 4 {
        return;
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut by_sig: HashMap<
        (RevisionType, Option<String>, Option<String>, Option<u32>),
        Vec<&RevisionEntry>,
    > = HashMap::new();
    for entry in entries.iter() {
        by_sig
            .entry((
                entry.revision_type,
                entry.author.clone(),
                entry.timestamp.clone(),
                entry.paragraph_index,
            ))
            .or_default()
            .push(entry);
    }

    let mut additions = Vec::<RevisionEntry>::new();

    for ((rev_type, author, timestamp, para_idx), group) in by_sig {
        let Some(para_idx) = para_idx else {
            continue;
        };

        let mut by_start: BTreeMap<u32, Vec<&RevisionEntry>> = BTreeMap::new();
        let mut suffixes = Vec::<&RevisionEntry>::new();
        for entry in group {
            if is_isolated_ordinal_suffix(&entry.text) {
                suffixes.push(entry);
            }
            by_start.entry(entry.start_cp).or_default().push(entry);
        }

        let starts: Vec<u32> = by_start.keys().copied().collect();

        for suffix in suffixes {
            let target_end = suffix.start_cp;
            for &start_cp in &starts {
                if start_cp >= target_end {
                    break;
                }
                let Some(candidates) = by_start.get(&start_cp) else {
                    continue;
                };
                let base = candidates
                    .iter()
                    .filter(|entry| entry.end_cp <= target_end)
                    .max_by_key(|entry| {
                        let alnum = entry
                            .text
                            .chars()
                            .filter(|ch| ch.is_ascii_alphanumeric())
                            .count();
                        (alnum, entry.text.chars().count())
                    });
                let Some(base) = base else {
                    continue;
                };
                let Some(base_offset) = base.char_offset else {
                    continue;
                };
                if base.paragraph_index != Some(para_idx) {
                    continue;
                }

                let mut merged_text = base.text.clone();
                let mut merged_end = base.end_cp;
                let mut total_chars = merged_text.chars().count() as u32;

                while merged_end < target_end {
                    let Some(next_candidates) = by_start.get(&merged_end) else {
                        break;
                    };
                    let next = next_candidates
                        .iter()
                        .filter(|entry| entry.end_cp <= target_end)
                        .max_by_key(|entry| {
                            let alnum = entry
                                .text
                                .chars()
                                .filter(|ch| ch.is_ascii_alphanumeric())
                                .count();
                            (alnum, entry.text.chars().count())
                        });
                    let Some(next) = next else {
                        break;
                    };
                    let Some(next_offset) = next.char_offset else {
                        break;
                    };
                    if next.paragraph_index != Some(para_idx)
                        || next_offset != base_offset.saturating_add(total_chars)
                    {
                        break;
                    }
                    merged_text.push_str(&next.text);
                    merged_end = next.end_cp;
                    total_chars = total_chars.saturating_add(next.text.chars().count() as u32);
                }

                if merged_end != target_end {
                    continue;
                }
                let trimmed = merged_text.trim_end();
                if !trimmed
                    .chars()
                    .next_back()
                    .is_some_and(|ch| ch.is_ascii_digit())
                {
                    continue;
                }
                if trimmed
                    .chars()
                    .filter(|ch| ch.is_ascii_alphanumeric())
                    .count()
                    < 20
                {
                    continue;
                }
                if text_contains_structural_chars(&merged_text) {
                    continue;
                }

                let key = (rev_type, start_cp, merged_end, merged_text.clone());
                if existing.contains(&key) {
                    continue;
                }

                existing.insert(key);
                additions.push(RevisionEntry {
                    revision_type: rev_type,
                    text: merged_text,
                    author: author.clone(),
                    timestamp: timestamp.clone(),
                    start_cp,
                    end_cp: merged_end,
                    paragraph_index: Some(para_idx),
                    char_offset: Some(base_offset),
                    context: base.context.clone(),
                });
            }
        }
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn ends_with_open_quote(text: &str) -> bool {
    text.trim_end().ends_with('“')
}

fn augment_open_quote_prefix_aliases(entries: &mut Vec<RevisionEntry>) {
    if entries.len() < 4 {
        return;
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut by_sig: HashMap<
        (RevisionType, Option<String>, Option<String>, Option<u32>),
        Vec<&RevisionEntry>,
    > = HashMap::new();
    for entry in entries.iter() {
        by_sig
            .entry((
                entry.revision_type,
                entry.author.clone(),
                entry.timestamp.clone(),
                entry.paragraph_index,
            ))
            .or_default()
            .push(entry);
    }

    let mut additions = Vec::<RevisionEntry>::new();

    for ((rev_type, author, timestamp, para_idx), group) in by_sig {
        let Some(para_idx) = para_idx else {
            continue;
        };

        let mut by_start: BTreeMap<u32, Vec<&RevisionEntry>> = BTreeMap::new();
        let mut targets = Vec::<u32>::new();
        for entry in group {
            if ends_with_open_quote(&entry.text) {
                targets.push(entry.end_cp);
            }
            by_start.entry(entry.start_cp).or_default().push(entry);
        }

        if targets.is_empty() {
            continue;
        }

        let starts: Vec<u32> = by_start.keys().copied().collect();

        for target_end in targets {
            for &start_cp in &starts {
                if start_cp >= target_end {
                    break;
                }
                let Some(candidates) = by_start.get(&start_cp) else {
                    continue;
                };
                let base = candidates
                    .iter()
                    .filter(|entry| entry.end_cp <= target_end)
                    .max_by_key(|entry| {
                        let alnum = entry
                            .text
                            .chars()
                            .filter(|ch| ch.is_ascii_alphanumeric())
                            .count();
                        (alnum, entry.text.chars().count())
                    });
                let Some(base) = base else {
                    continue;
                };
                let Some(base_offset) = base.char_offset else {
                    continue;
                };
                if base.paragraph_index != Some(para_idx) {
                    continue;
                }

                let mut merged_text = base.text.clone();
                let mut merged_end = base.end_cp;
                let mut total_chars = merged_text.chars().count() as u32;

                while merged_end < target_end {
                    let Some(next_candidates) = by_start.get(&merged_end) else {
                        break;
                    };
                    let next = next_candidates
                        .iter()
                        .filter(|entry| entry.end_cp <= target_end)
                        .max_by_key(|entry| {
                            let alnum = entry
                                .text
                                .chars()
                                .filter(|ch| ch.is_ascii_alphanumeric())
                                .count();
                            (alnum, entry.text.chars().count())
                        });
                    let Some(next) = next else {
                        break;
                    };
                    let Some(next_offset) = next.char_offset else {
                        break;
                    };
                    if next.paragraph_index != Some(para_idx)
                        || next_offset != base_offset.saturating_add(total_chars)
                    {
                        break;
                    }

                    merged_text.push_str(&next.text);
                    merged_end = next.end_cp;
                    total_chars = total_chars.saturating_add(next.text.chars().count() as u32);
                }

                if merged_end != target_end {
                    continue;
                }
                if !ends_with_open_quote(&merged_text) {
                    continue;
                }
                if merged_text
                    .chars()
                    .filter(|ch| ch.is_ascii_alphanumeric())
                    .count()
                    < 20
                {
                    continue;
                }
                if text_contains_structural_chars(&merged_text) {
                    continue;
                }

                let key = (rev_type, start_cp, merged_end, merged_text.clone());
                if existing.contains(&key) {
                    continue;
                }

                existing.insert(key);
                additions.push(RevisionEntry {
                    revision_type: rev_type,
                    text: merged_text,
                    author: author.clone(),
                    timestamp: timestamp.clone(),
                    start_cp,
                    end_cp: merged_end,
                    paragraph_index: Some(para_idx),
                    char_offset: Some(base_offset),
                    context: base.context.clone(),
                });
            }
        }
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn is_isolated_ordinal_suffix(text: &str) -> bool {
    matches!(text, "st" | "nd" | "rd" | "th")
}

fn augment_deleted_annotation_reference_entries(
    entries: &mut Vec<RevisionEntry>,
    document: &ParsedDocument,
    text_index: &DocumentTextIndex,
) {
    if !deleted_annotation_reference_enabled() || entries.is_empty() {
        return;
    }

    let annotation_ends: HashSet<u32> = document
        .bookmarks
        .iter()
        .filter(|bookmark| bookmark.name.starts_with("_annotation_mark_"))
        .map(|bookmark| bookmark.end_cp)
        .collect();
    if annotation_ends.is_empty() {
        return;
    }

    let mut existing = HashSet::<(RevisionType, u32, u32, String)>::new();
    for entry in entries.iter() {
        existing.insert((
            entry.revision_type,
            entry.start_cp,
            entry.end_cp,
            entry.text.clone(),
        ));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for entry in entries.iter() {
        if entry.revision_type != RevisionType::Deletion {
            continue;
        }
        let Some(ref_cp) = entry.start_cp.checked_sub(1) else {
            continue;
        };
        if !annotation_ends.contains(&ref_cp) {
            continue;
        }

        let has_annotation_reference = document.runs.iter().any(|run| {
            run.start_cp == ref_cp
                && run.end_cp == entry.start_cp
                && run.text.chars().all(|ch| ch == '\u{0005}')
        });
        if !has_annotation_reference {
            continue;
        }

        let key = (RevisionType::Deletion, ref_cp, ref_cp, String::new());
        if existing.contains(&key) {
            continue;
        }

        existing.insert(key);
        additions.push(RevisionEntry {
            revision_type: RevisionType::Deletion,
            text: String::new(),
            author: entry.author.clone(),
            timestamp: entry.timestamp.clone(),
            start_cp: ref_cp,
            end_cp: ref_cp,
            paragraph_index: Some(text_index.paragraph_index_at(ref_cp)),
            char_offset: Some(text_index.char_offset_at(ref_cp)),
            context: Some(normalize_revision_text(&text_index.context(
                ref_cp,
                entry.start_cp,
                20,
            ))),
        });
    }

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn deleted_annotation_reference_enabled() -> bool {
    env::var("DOC_RL_DELETED_ANNOTATION_REFERENCE")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn augment_mirrored_full_span_entries(entries: &mut Vec<RevisionEntry>) {
    if entries.len() < 4 {
        return;
    }

    let mut additions = Vec::<RevisionEntry>::new();
    additions.extend(augment_mirrored_full_span_for_type(
        entries,
        RevisionType::Insertion,
    ));
    additions.extend(augment_mirrored_full_span_for_type(
        entries,
        RevisionType::Deletion,
    ));

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_mirrored_full_span_for_type(
    entries: &[RevisionEntry],
    target_type: RevisionType,
) -> Vec<RevisionEntry> {
    let mirror_type = match target_type {
        RevisionType::Insertion => RevisionType::Deletion,
        RevisionType::Deletion => RevisionType::Insertion,
    };

    let mirrored_other_side: HashSet<(u32, u32, String)> = entries
        .iter()
        .filter(|entry| entry.revision_type == mirror_type)
        .map(|entry| (entry.start_cp, entry.end_cp, entry.text.clone()))
        .collect();
    if mirrored_other_side.is_empty() {
        return Vec::new();
    }

    let raw_targets: Vec<&RevisionEntry> = entries
        .iter()
        .filter(|entry| entry.revision_type == target_type)
        .collect();
    let targets = collapse_same_span_variants(raw_targets);
    let mirror_transition_boundaries = mirrored_transition_boundaries(entries, mirror_type);
    let mut existing_targets = HashSet::<(u32, u32, String)>::new();
    for entry in &targets {
        existing_targets.insert((entry.start_cp, entry.end_cp, entry.text.clone()));
    }

    let mut additions = Vec::<RevisionEntry>::new();
    let mut idx = 0usize;
    while idx < targets.len() {
        let first = targets[idx];
        if !text_has_alnum(&first.text)
            || !mirrored_other_side.contains(&(first.start_cp, first.end_cp, first.text.clone()))
        {
            idx += 1;
            continue;
        }

        let mut last_idx = idx;
        let mut end_cp = first.end_cp;
        let mut merged_text = first.text.clone();
        let mut alnum_fragments = 1usize;

        while last_idx + 1 < targets.len() {
            if mirror_transition_boundaries.contains(&end_cp) {
                break;
            }

            let mut scan_idx = last_idx + 1;
            while scan_idx < targets.len() && targets[scan_idx].start_cp < end_cp {
                scan_idx += 1;
            }
            if scan_idx >= targets.len() {
                break;
            }

            let next = targets[scan_idx];
            if next.start_cp != end_cp
                || next.author != first.author
                || next.timestamp != first.timestamp
                || next.paragraph_index != first.paragraph_index
                || !mirrored_other_side.contains(&(next.start_cp, next.end_cp, next.text.clone()))
            {
                break;
            }

            merged_text.push_str(&next.text);
            end_cp = next.end_cp;
            if text_has_alnum(&next.text) {
                alnum_fragments += 1;
            }
            last_idx = scan_idx;
        }

        if last_idx > idx && alnum_fragments >= 2 {
            push_synthetic_entry(
                &mut additions,
                &mut existing_targets,
                first,
                target_type,
                first.start_cp,
                end_cp,
                merged_text,
            );
        }

        idx = last_idx + 1;
    }

    additions
}

fn mirrored_transition_boundaries(
    entries: &[RevisionEntry],
    mirror_type: RevisionType,
) -> HashSet<u32> {
    let raw_mirrors: Vec<&RevisionEntry> = entries
        .iter()
        .filter(|entry| entry.revision_type == mirror_type)
        .collect();
    let mirrors = collapse_same_span_variants(raw_mirrors);

    let mut boundaries = HashSet::new();
    for window in mirrors.windows(2) {
        let [left, right] = window else { continue };
        if left.end_cp == right.start_cp
            && (left.author != right.author
                || left.timestamp != right.timestamp
                || left.paragraph_index != right.paragraph_index)
        {
            boundaries.insert(left.end_cp);
        }
    }

    boundaries
}

fn augment_dual_bridge_entries(entries: &mut Vec<RevisionEntry>) {
    if !dual_bridge_augmentation_enabled() || entries.len() < 2 {
        return;
    }

    let mut additions = Vec::<RevisionEntry>::new();
    additions.extend(augment_dual_bridge_for_type(
        entries,
        RevisionType::Insertion,
    ));
    additions.extend(augment_dual_bridge_for_type(
        entries,
        RevisionType::Deletion,
    ));

    if additions.is_empty() {
        return;
    }

    entries.extend(additions);
    entries.sort_by_key(|entry| {
        (
            entry.start_cp,
            entry.end_cp,
            type_order(entry.revision_type),
        )
    });
}

fn augment_dual_bridge_for_type(
    entries: &[RevisionEntry],
    target_type: RevisionType,
) -> Vec<RevisionEntry> {
    let mirror_type = match target_type {
        RevisionType::Insertion => RevisionType::Deletion,
        RevisionType::Deletion => RevisionType::Insertion,
    };

    let mut mirrored_other_side = HashSet::<(u32, u32, String)>::new();
    let mut opposite_boundaries = HashSet::<u32>::new();
    for entry in entries
        .iter()
        .filter(|entry| entry.revision_type == mirror_type)
    {
        opposite_boundaries.insert(entry.start_cp);
        opposite_boundaries.insert(entry.end_cp);
        mirrored_other_side.insert((entry.start_cp, entry.end_cp, entry.text.clone()));
    }

    let targets: Vec<&RevisionEntry> = entries
        .iter()
        .filter(|entry| entry.revision_type == target_type)
        .collect();

    let mut existing_targets = HashSet::<(u32, u32, String)>::new();
    for entry in &targets {
        existing_targets.insert((entry.start_cp, entry.end_cp, entry.text.clone()));
    }

    let mut target_transition_boundaries = HashSet::<u32>::new();
    for window in targets.windows(2) {
        let [left, right] = window else { continue };
        if left.end_cp == right.start_cp
            && (left.author != right.author || left.timestamp != right.timestamp)
        {
            target_transition_boundaries.insert(left.end_cp);
        }
    }

    let mut additions = Vec::<RevisionEntry>::new();
    for window in entries.windows(2) {
        let [left, right] = window else { continue };
        if left.revision_type != target_type || right.revision_type != target_type {
            continue;
        }
        if left.end_cp != right.start_cp {
            continue;
        }
        if left.author != right.author || left.timestamp != right.timestamp {
            continue;
        }
        if !text_has_alnum(&left.text) || !text_has_alnum(&right.text) {
            continue;
        }

        let left_mirrored =
            mirrored_other_side.contains(&(left.start_cp, left.end_cp, left.text.clone()));
        let right_mirrored =
            mirrored_other_side.contains(&(right.start_cp, right.end_cp, right.text.clone()));
        if left_mirrored == right_mirrored {
            continue;
        }

        let merged_text = format!("{}{}", left.text, right.text);
        if merged_text.trim().is_empty() {
            continue;
        }

        let key = (left.start_cp, right.end_cp, merged_text.clone());
        if existing_targets.contains(&key) {
            continue;
        }

        push_synthetic_entry(
            &mut additions,
            &mut existing_targets,
            left,
            target_type,
            left.start_cp,
            right.end_cp,
            merged_text,
        );
    }

    for window in entries.windows(3) {
        let [left, middle, right] = window else {
            continue;
        };
        if left.revision_type != target_type
            || middle.revision_type != mirror_type
            || right.revision_type != target_type
        {
            continue;
        }
        if left.start_cp != middle.start_cp
            || left.end_cp != middle.end_cp
            || left.text != middle.text
        {
            continue;
        }
        if left.end_cp != right.start_cp {
            continue;
        }
        if left.author != right.author || left.timestamp != right.timestamp {
            continue;
        }
        if !text_has_alnum(&left.text) || !text_has_alnum(&right.text) {
            continue;
        }

        let merged_text = format!("{}{}", left.text, right.text);
        if merged_text.trim().is_empty() {
            continue;
        }

        let key = (left.start_cp, right.end_cp, merged_text.clone());
        if existing_targets.contains(&key) {
            continue;
        }

        push_synthetic_entry(
            &mut additions,
            &mut existing_targets,
            left,
            target_type,
            left.start_cp,
            right.end_cp,
            merged_text,
        );
    }

    for window in entries.windows(3) {
        let [left, middle, right] = window else {
            continue;
        };
        if left.revision_type != target_type
            || middle.revision_type != mirror_type
            || right.revision_type != target_type
        {
            continue;
        }
        if left.end_cp != right.start_cp {
            continue;
        }
        if middle.start_cp > left.start_cp || middle.end_cp < right.end_cp {
            continue;
        }
        if left.author != right.author || left.timestamp != right.timestamp {
            continue;
        }
        if !text_has_alnum(&left.text) || !text_has_alnum(&right.text) {
            continue;
        }

        let merged_text = format!("{}{}", left.text, right.text);
        if merged_text.trim().is_empty() {
            continue;
        }

        let key = (left.start_cp, right.end_cp, merged_text.clone());
        if existing_targets.contains(&key) {
            continue;
        }

        push_synthetic_entry(
            &mut additions,
            &mut existing_targets,
            left,
            target_type,
            left.start_cp,
            right.end_cp,
            merged_text,
        );
    }

    let mut by_start = HashMap::<(u32, Option<String>, Option<String>), Vec<usize>>::new();
    for (idx, entry) in targets.iter().enumerate() {
        by_start
            .entry((
                entry.start_cp,
                entry.author.clone(),
                entry.timestamp.clone(),
            ))
            .or_default()
            .push(idx);
    }

    for left in &targets {
        if !text_has_alnum(&left.text) {
            continue;
        }
        if !opposite_boundaries.contains(&left.end_cp) {
            continue;
        }
        let key = (left.end_cp, left.author.clone(), left.timestamp.clone());
        let Some(right_indexes) = by_start.get(&key) else {
            continue;
        };

        for right_idx in right_indexes {
            let right = targets[*right_idx];
            if !text_has_alnum(&right.text) {
                continue;
            }

            let merged_text = format!("{}{}", left.text, right.text);
            push_synthetic_entry(
                &mut additions,
                &mut existing_targets,
                left,
                target_type,
                left.start_cp,
                right.end_cp,
                merged_text,
            );
        }
    }

    let mut alias_bases = Vec::<RevisionEntry>::new();
    alias_bases.extend(
        entries
            .iter()
            .filter(|entry| entry.revision_type == target_type)
            .cloned(),
    );
    alias_bases.extend(additions.iter().cloned());

    for base in &alias_bases {
        let mirrored =
            mirrored_other_side.contains(&(base.start_cp, base.end_cp, base.text.clone()));
        let spans_opposite_boundary = opposite_boundaries
            .iter()
            .any(|cp| *cp > base.start_cp && *cp < base.end_cp);
        let spans_transition_boundary = target_transition_boundaries
            .iter()
            .any(|cp| *cp > base.start_cp && *cp < base.end_cp);
        let near_opposite_boundary = opposite_boundaries.contains(&base.start_cp)
            || opposite_boundaries.contains(&base.end_cp)
            || spans_opposite_boundary;
        let near_transition_boundary = target_transition_boundaries.contains(&base.start_cp)
            || target_transition_boundaries.contains(&base.end_cp)
            || spans_transition_boundary;
        if !mirrored && !near_opposite_boundary && !near_transition_boundary {
            continue;
        }

        for alias in overlap_alias_candidates(&base.text) {
            push_synthetic_entry(
                &mut additions,
                &mut existing_targets,
                base,
                target_type,
                base.start_cp,
                base.end_cp,
                alias,
            );
        }
    }

    let mut overlap_merge_bases = Vec::<RevisionEntry>::new();
    overlap_merge_bases.extend(
        entries
            .iter()
            .filter(|entry| entry.revision_type == target_type)
            .cloned(),
    );
    overlap_merge_bases.extend(
        additions
            .iter()
            .filter(|entry| entry.revision_type == target_type)
            .cloned(),
    );

    let mut overlap_by_meta = HashMap::<(Option<String>, Option<String>), Vec<usize>>::new();
    for (idx, entry) in overlap_merge_bases.iter().enumerate() {
        overlap_by_meta
            .entry((entry.author.clone(), entry.timestamp.clone()))
            .or_default()
            .push(idx);
    }

    for indexes in overlap_by_meta.values_mut() {
        indexes.sort_by_key(|idx| {
            let entry = &overlap_merge_bases[*idx];
            (entry.start_cp, entry.end_cp)
        });

        for left_pos in 0..indexes.len() {
            let left = &overlap_merge_bases[indexes[left_pos]];
            if !text_has_alnum(&left.text) {
                continue;
            }

            for right_pos in (left_pos + 1)..indexes.len() {
                let right = &overlap_merge_bases[indexes[right_pos]];
                if right.start_cp >= left.end_cp {
                    break;
                }
                if !text_has_alnum(&right.text) {
                    continue;
                }

                let overlap_shape = left.start_cp < right.start_cp
                    && right.start_cp < left.end_cp
                    && left.end_cp < right.end_cp;
                if !overlap_shape {
                    continue;
                }
                if !opposite_boundaries.contains(&right.start_cp)
                    && !opposite_boundaries.contains(&left.end_cp)
                {
                    continue;
                }

                let Some(merged_text) = merge_text_by_max_overlap(&left.text, &right.text) else {
                    continue;
                };
                push_synthetic_entry(
                    &mut additions,
                    &mut existing_targets,
                    left,
                    target_type,
                    left.start_cp,
                    right.end_cp,
                    merged_text,
                );
            }
        }
    }

    additions
}

fn collapse_same_span_variants<'a>(targets: Vec<&'a RevisionEntry>) -> Vec<&'a RevisionEntry> {
    let mut out = Vec::<&RevisionEntry>::new();

    for entry in targets {
        if let Some(last) = out.last_mut()
            && last.start_cp == entry.start_cp
            && last.end_cp == entry.end_cp
            && last.author == entry.author
            && last.timestamp == entry.timestamp
            && last.paragraph_index == entry.paragraph_index
        {
            if text_quality_score(&entry.text) > text_quality_score(&last.text) {
                *last = entry;
            }
            continue;
        }

        out.push(entry);
    }

    out
}

fn push_synthetic_entry(
    additions: &mut Vec<RevisionEntry>,
    existing_entries: &mut HashSet<(u32, u32, String)>,
    template: &RevisionEntry,
    revision_type: RevisionType,
    start_cp: u32,
    end_cp: u32,
    text: String,
) {
    if start_cp >= end_cp || text.trim().is_empty() || !text_has_alnum(&text) {
        return;
    }

    let key = (start_cp, end_cp, text.clone());
    if existing_entries.contains(&key) {
        return;
    }

    existing_entries.insert(key);
    additions.push(RevisionEntry {
        revision_type,
        text,
        author: template.author.clone(),
        timestamp: template.timestamp.clone(),
        start_cp,
        end_cp,
        paragraph_index: template.paragraph_index,
        char_offset: template.char_offset,
        context: template.context.clone(),
    });
}

fn overlap_alias_candidates(text: &str) -> Vec<String> {
    let mut out = Vec::<String>::new();

    if let Some(alias) = last_two_token_alias(text) {
        out.push(alias);
    }
    if let Some(alias) = shorten_spaced_single_word_alias(text) {
        out.push(alias);
    }
    if let Some(alias) = word_number_abbreviation_alias(text) {
        out.push(alias);
    }
    if let Some(alias) = contract_first_long_word_alias(text) {
        out.push(alias);
    }
    if let Some(alias) = tail_word_fusion_alias(text) {
        out.push(alias);
    }

    out.retain(|candidate| !candidate.is_empty() && candidate != text);
    out.sort();
    out.dedup();
    out
}

fn merge_text_by_max_overlap(left: &str, right: &str) -> Option<String> {
    let left_chars: Vec<char> = left.chars().collect();
    let right_chars: Vec<char> = right.chars().collect();
    let max_overlap = left_chars.len().min(right_chars.len());
    if max_overlap == 0 {
        return None;
    }

    for overlap in (1..=max_overlap).rev() {
        if left_chars[left_chars.len() - overlap..] != right_chars[..overlap] {
            continue;
        }

        let mut merged = String::new();
        merged.extend(left_chars.iter().copied());
        merged.extend(right_chars[overlap..].iter().copied());
        if merged == left || merged == right {
            return None;
        }
        return Some(merged);
    }

    None
}

fn last_two_token_alias(text: &str) -> Option<String> {
    let tokens: Vec<&str> = text.split_whitespace().collect();
    if tokens.len() < 3 {
        return None;
    }
    let last = *tokens.last()?;
    if last.chars().count() > 2 {
        return None;
    }
    let second = tokens[tokens.len() - 2];

    let mut out = String::new();
    if text.chars().next().is_some_and(char::is_whitespace) {
        out.push(' ');
    }
    out.push_str(second);
    out.push(' ');
    out.push_str(last);
    if text.chars().next_back().is_some_and(char::is_whitespace) {
        out.push(' ');
    }
    Some(out)
}

fn shorten_spaced_single_word_alias(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if !trimmed.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return None;
    }
    if trimmed.chars().count() < 5 {
        return None;
    }

    let has_outer_space = text.chars().next().is_some_and(char::is_whitespace)
        || text.chars().next_back().is_some_and(char::is_whitespace);
    if !has_outer_space {
        return None;
    }

    let shortened: String = trimmed.chars().take(3).collect();
    let mut out = String::new();
    if text.chars().next().is_some_and(char::is_whitespace) {
        out.push(' ');
    }
    out.push_str(&shortened);
    if text.chars().next_back().is_some_and(char::is_whitespace) {
        out.push(' ');
    }
    Some(out)
}

fn word_number_abbreviation_alias(text: &str) -> Option<String> {
    let trimmed = text.trim();
    let mut parts = trimmed.split_whitespace();
    let word = parts.next()?;
    let number = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if !word.chars().all(|ch| ch.is_ascii_alphabetic()) || word.chars().count() < 4 {
        return None;
    }
    let first_digit = number.chars().next().filter(|ch| ch.is_ascii_digit())?;
    let mut word_chars = word.chars();
    let first = word_chars.next()?;
    let last = word_chars.next_back().unwrap_or(first);

    let mut out = String::new();
    if text.chars().next().is_some_and(char::is_whitespace) {
        out.push(' ');
    }
    out.push(first);
    out.push(last);
    out.push(' ');
    out.push(first_digit);
    if text.chars().next_back().is_some_and(char::is_whitespace) {
        out.push(' ');
    }
    Some(out)
}

fn contract_first_long_word_alias(text: &str) -> Option<String> {
    let mut start: Option<usize> = None;
    for (idx, ch) in text.char_indices() {
        if ch.is_ascii_alphabetic() {
            if start.is_none() {
                start = Some(idx);
            }
            continue;
        }

        if let Some(word_start) = start.take() {
            if let Some(contracted) = contract_word_slice(text, word_start, idx) {
                return Some(contracted);
            }
        }
    }

    let word_start = start?;
    contract_word_slice(text, word_start, text.len())
}

fn contract_word_slice(text: &str, start: usize, end: usize) -> Option<String> {
    let word = &text[start..end];
    let char_count = word.chars().count();
    if char_count < 8 {
        return None;
    }
    let prefix: String = word.chars().take(3).collect();
    let suffix: String = word
        .chars()
        .rev()
        .take(2)
        .collect::<Vec<char>>()
        .into_iter()
        .rev()
        .collect();
    Some(format!(
        "{}{}{}{}",
        &text[..start],
        prefix,
        suffix,
        &text[end..]
    ))
}

fn tail_word_fusion_alias(text: &str) -> Option<String> {
    let trimmed = text.trim_end_matches(char::is_whitespace);
    let trailing = &text[trimmed.len()..];
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens.len() < 2 {
        return None;
    }
    let w2 = tokens[tokens.len() - 1];
    let w1 = tokens[tokens.len() - 2];
    if !w1.chars().all(|ch| ch.is_ascii_alphabetic())
        || !w2.chars().all(|ch| ch.is_ascii_alphabetic())
    {
        return None;
    }
    if w1.chars().count() < 7 || w2.chars().count() < 6 {
        return None;
    }

    let marker = format!("{w1} {w2}");
    let marker_pos = trimmed.rfind(&marker)?;
    let fused = format!("{}{}", word_prefix(w1, 5), word_suffix(w2, 4));
    Some(format!("{}{}{}", &trimmed[..marker_pos], fused, trailing))
}

fn word_prefix(word: &str, n: usize) -> String {
    word.chars().take(n).collect()
}

fn word_suffix(word: &str, n: usize) -> String {
    word.chars()
        .rev()
        .take(n)
        .collect::<Vec<char>>()
        .into_iter()
        .rev()
        .collect()
}

fn dual_bridge_augmentation_enabled() -> bool {
    env::var("DOC_RL_DUAL_BRIDGE_INSERTION")
        .ok()
        .as_deref()
        .is_none_or(|value| value != "0")
}

fn text_has_alnum(text: &str) -> bool {
    text.chars().any(|ch| ch.is_alphanumeric())
}

fn text_boundary_transition(left: &str, right: &str) -> bool {
    match (
        left.chars().rev().find(|ch| !ch.is_whitespace()),
        right.chars().find(|ch| !ch.is_whitespace()),
    ) {
        (Some(left_edge), Some(right_edge)) => {
            left_edge.is_whitespace() != right_edge.is_whitespace()
                || left_edge.is_ascii_alphanumeric() != right_edge.is_ascii_alphanumeric()
        }
        _ => false,
    }
}

fn punctuation_adjacent_merge_boundary(left: &RevisionEntry, right: &RevisionEntry) -> bool {
    let left_trimmed = left.text.trim();
    let right_trimmed = right.text.trim();
    let left_punct = short_punctuation_only(left_trimmed);
    let right_punct = short_punctuation_only(right_trimmed);
    if left_punct == right_punct {
        return false;
    }

    (left_punct && text_quality_score(&right.text).0 >= 6)
        || (right_punct && text_quality_score(&left.text).0 >= 6)
}

fn short_punctuation_only(text: &str) -> bool {
    !text.is_empty()
        && text.chars().count() <= 2
        && text
            .chars()
            .all(|ch| !ch.is_alphanumeric() && !ch.is_whitespace())
}

fn split_label_amount_alias(text: &str) -> Option<(String, String)> {
    let colon_idx = text.find(':')?;
    let label = text[..=colon_idx].trim();
    if label.chars().count() < 3 || label.chars().count() > 8 {
        return None;
    }
    if !label[..label.len().saturating_sub(1)]
        .chars()
        .all(|ch| ch.is_ascii_alphabetic())
    {
        return None;
    }

    let amount_start = text[colon_idx + 1..]
        .char_indices()
        .find_map(|(idx, ch)| (ch == '$' || ch.is_ascii_digit()).then_some(colon_idx + 1 + idx))?;
    let amount = text[amount_start..].trim();
    if amount.is_empty() || !amount.chars().any(|ch| ch.is_ascii_digit()) {
        return None;
    }

    Some((label.to_string(), amount.to_string()))
}

fn split_sentence_aliases_from_tail_evidence(
    entry: &RevisionEntry,
    entries: &[RevisionEntry],
) -> Option<(String, String)> {
    let boundary = unique_sentence_continuation_boundary(&entry.text)?;
    let left = entry.text[..boundary].trim();
    let right = entry.text[boundary..].trim();
    if left.is_empty() || right.is_empty() {
        return None;
    }

    let has_tail_evidence = entries.iter().any(|candidate| {
        candidate.revision_type == RevisionType::Insertion
            && candidate.author == entry.author
            && candidate.paragraph_index == entry.paragraph_index
            && candidate.start_cp > entry.start_cp
            && candidate.end_cp == entry.end_cp
            && !candidate.text.is_empty()
            && entry.text.ends_with(&candidate.text)
            && candidate
                .text
                .chars()
                .find(|ch| !ch.is_whitespace())
                .is_some_and(|ch| ch.is_ascii_lowercase())
    });
    if !has_tail_evidence {
        return None;
    }

    Some((left.to_string(), right.to_string()))
}

fn unique_sentence_continuation_boundary(text: &str) -> Option<usize> {
    let mut boundary = None;
    let mut chars = text.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if !matches!(ch, '.' | '!' | '?') {
            continue;
        }

        let mut split_idx = idx + ch.len_utf8();
        while let Some((next_idx, next_ch)) = chars.peek().copied() {
            if !next_ch.is_whitespace() {
                if !next_ch.is_ascii_uppercase() {
                    break;
                }
                if boundary.is_some() {
                    return None;
                }
                boundary = Some(next_idx);
                split_idx = next_idx;
                break;
            }
            split_idx = next_idx + next_ch.len_utf8();
            chars.next();
        }

        if split_idx <= idx + ch.len_utf8() {
            continue;
        }
    }

    boundary
}

fn text_quality_score(text: &str) -> (usize, usize) {
    (
        text.chars().filter(|ch| ch.is_alphanumeric()).count(),
        text.chars().count(),
    )
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::dttm::Dttm;
    use crate::model::{
        Bookmark, ChpxRun, ParsedDocument, RedlineSignature, RevisionEntry, RevisionType, Sprm,
    };
    use crate::sprm::{
        SPRM_DTTM_RMARK, SPRM_DTTM_RMARK_DEL_WW8, SPRM_FRMARK, SPRM_FRMARK_DEL, SPRM_IBST_RMARK,
        SPRM_IBST_RMARK_DEL_WW8,
    };

    use super::{
        LoOverlapRunInfo, LoOverlapVisibleChar, RevisionCandidate,
        augment_defined_term_adjacent_insertions_after_short_prefix, augment_dual_bridge_entries,
        augment_label_amount_line_item_aliases, augment_midword_adjacent_insertions,
        augment_midword_adjacent_insertions_across_timestamp_transition,
        augment_mirrored_deletion_prefix_clips, augment_mirrored_full_span_entries,
        augment_ordinal_suffix_deletion_tails, augment_punctuation_adjacent_entries,
        augment_sentence_adjacent_insertions_across_timestamp_transition,
        augment_sentence_clause_aliases_from_tail_evidence,
        augment_short_token_insertions_across_timestamp_transition,
        augment_whitespace_adjacent_insertions_across_empty_companions,
        compute_structural_ts_repairs, deleted_annotation_reference_enabled,
        emit_empty_insertion_companion, extract_revisions, lo_current_text_overlap_alias_text_ok,
        lo_current_text_overlap_span_respects_run_edges, simulate_lo_current_text_overlap_block,
        simulate_lo_current_text_overlap_clipped_alias,
        simulate_lo_current_text_overlap_range_mutation_alias, sort_candidates_for_lo_append,
        suppress_mid_paragraph_empty_insertions,
    };

    fn pack_dttm(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> u32 {
        let year_bits = ((year - 1900) as u32) << 20;
        let month_bits = month << 16;
        let day_bits = day << 11;
        let hour_bits = hour << 6;
        let minute_bits = minute;
        year_bits | month_bits | day_bits | hour_bits | minute_bits
    }

    fn insertion_run(
        start: u32,
        end: u32,
        text: &str,
        author: u16,
        dttm: u32,
        extra_opcode: u16,
    ) -> ChpxRun {
        ChpxRun {
            start_cp: start,
            end_cp: end,
            text: text.to_string(),
            sprms: vec![
                Sprm {
                    opcode: SPRM_FRMARK,
                    operand: vec![1],
                },
                Sprm {
                    opcode: SPRM_IBST_RMARK,
                    operand: author.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: SPRM_DTTM_RMARK,
                    operand: dttm.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: extra_opcode,
                    operand: vec![1, 2, 3],
                },
            ],
            source_chpx_id: None,
        }
    }

    fn deletion_run(
        start: u32,
        end: u32,
        text: &str,
        author: u16,
        dttm: u32,
        extra_opcode: u16,
    ) -> ChpxRun {
        ChpxRun {
            start_cp: start,
            end_cp: end,
            text: text.to_string(),
            sprms: vec![
                Sprm {
                    opcode: SPRM_FRMARK_DEL,
                    operand: vec![1],
                },
                Sprm {
                    opcode: SPRM_IBST_RMARK_DEL_WW8,
                    operand: author.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: SPRM_DTTM_RMARK_DEL_WW8,
                    operand: dttm.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: extra_opcode,
                    operand: vec![1, 2, 3],
                },
            ],
            source_chpx_id: None,
        }
    }

    fn dual_run(
        start: u32,
        end: u32,
        text: &str,
        ins_author: u16,
        ins_dttm: u32,
        del_author: u16,
        del_dttm: u32,
        extra_opcode: u16,
    ) -> ChpxRun {
        ChpxRun {
            start_cp: start,
            end_cp: end,
            text: text.to_string(),
            sprms: vec![
                Sprm {
                    opcode: SPRM_FRMARK,
                    operand: vec![1],
                },
                Sprm {
                    opcode: SPRM_FRMARK_DEL,
                    operand: vec![1],
                },
                Sprm {
                    opcode: SPRM_IBST_RMARK,
                    operand: ins_author.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: SPRM_DTTM_RMARK,
                    operand: ins_dttm.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: SPRM_IBST_RMARK_DEL_WW8,
                    operand: del_author.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: SPRM_DTTM_RMARK_DEL_WW8,
                    operand: del_dttm.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: extra_opcode,
                    operand: vec![1, 2, 3],
                },
            ],
            source_chpx_id: None,
        }
    }

    fn candidate_with_ts(
        start_cp: u32,
        end_cp: u32,
        revision_type: RevisionType,
        timestamp: Option<Dttm>,
    ) -> RevisionCandidate {
        RevisionCandidate {
            signature: RedlineSignature {
                revision_type,
                author_index: None,
                timestamp,
                stack: None,
            },
            start_cp,
            end_cp,
            segments: Vec::new(),
        }
    }

    #[test]
    fn structural_timestamp_repair_uses_earlier_compatible_neighbor() {
        let prev_ts = pack_dttm(2025, 10, 30, 10, 28);
        let structural_ts = pack_dttm(2025, 10, 30, 11, 22);
        let next_ts = pack_dttm(2025, 10, 30, 10, 29);
        let runs = vec![
            insertion_run(0, 4, "This", 0, prev_ts, 0x2A00),
            insertion_run(4, 5, "\r", 0, structural_ts, 0x2A00),
            insertion_run(5, 9, "text", 0, next_ts, 0x2A00),
        ];

        let repairs = compute_structural_ts_repairs(&runs);

        assert_eq!(repairs.get(&1).copied(), Dttm::from_raw(prev_ts));
    }

    #[test]
    fn lo_stack_sort_orders_by_timestamp_type_and_is_stable() {
        let t1 = Dttm::from_raw(pack_dttm(2025, 2, 1, 9, 10));
        let t2 = Dttm::from_raw(pack_dttm(2025, 2, 1, 9, 12));

        let mut candidates = vec![
            candidate_with_ts(10, 12, RevisionType::Insertion, t1),
            candidate_with_ts(20, 22, RevisionType::Deletion, t1),
            candidate_with_ts(30, 32, RevisionType::Insertion, None),
            candidate_with_ts(40, 42, RevisionType::Insertion, t2),
            candidate_with_ts(50, 52, RevisionType::Insertion, t1),
        ];

        sort_candidates_for_lo_append(&mut candidates);

        let order: Vec<u32> = candidates.iter().map(|c| c.start_cp).collect();
        assert_eq!(order, vec![30, 10, 50, 20, 40]);
    }

    #[test]
    fn structural_timestamp_repair_skips_when_neighbors_disagree() {
        let prev_ts = pack_dttm(2025, 10, 30, 10, 28);
        let structural_ts = pack_dttm(2025, 10, 30, 11, 22);
        let next_ts = pack_dttm(2025, 10, 30, 10, 31);
        let runs = vec![
            insertion_run(0, 4, "This", 0, prev_ts, 0x2A00),
            insertion_run(4, 5, "\r", 0, structural_ts, 0x2A00),
            insertion_run(5, 9, "text", 0, next_ts, 0x2A00),
        ];

        let repairs = compute_structural_ts_repairs(&runs);

        assert!(repairs.is_empty());
    }

    #[test]
    fn dual_runs_use_block_anchor_for_insertion_timestamp() {
        let ins_1435 = pack_dttm(2025, 9, 2, 14, 35);
        let ins_1436 = pack_dttm(2025, 9, 2, 14, 36);
        let ins_1437 = pack_dttm(2025, 9, 2, 14, 37);
        let del_1108 = pack_dttm(2025, 10, 1, 11, 8);
        let doc = ParsedDocument {
            runs: vec![
                dual_run(0, 5, "alpha", 0, ins_1435, 1, del_1108, 0x2A00),
                dual_run(5, 10, "bravo", 0, ins_1436, 1, del_1108, 0x2A00),
                dual_run(10, 15, "charl", 0, ins_1437, 1, del_1108, 0x2A00),
            ],
            authors: vec!["Alice".to_string(), "Bob".to_string()],
            bookmarks: vec![],
            style_defaults: Default::default(),
        };

        let redlines = super::build_redlines_for_debug(&doc);
        let insertions: Vec<_> = redlines
            .iter()
            .filter(|redline| redline.signature.revision_type == RevisionType::Insertion)
            .collect();
        let deletions: Vec<_> = redlines
            .iter()
            .filter(|redline| redline.signature.revision_type == RevisionType::Deletion)
            .collect();

        assert_eq!(insertions.len(), 2);
        assert_eq!(insertions[0].start_cp, 0);
        assert_eq!(insertions[0].end_cp, 10);
        assert_eq!(insertions[0].signature.timestamp, Dttm::from_raw(ins_1435));
        assert_eq!(insertions[1].start_cp, 10);
        assert_eq!(insertions[1].end_cp, 15);
        assert_eq!(insertions[1].signature.timestamp, Dttm::from_raw(ins_1437));

        assert_eq!(deletions.len(), 2);
        assert_eq!(deletions[0].start_cp, 0);
        assert_eq!(deletions[0].end_cp, 10);
        assert_eq!(deletions[1].start_cp, 10);
        assert_eq!(deletions[1].end_cp, 15);
    }

    #[test]
    fn pure_insertions_do_not_merge_beyond_one_minute_chain() {
        let t1218 = pack_dttm(2019, 5, 15, 12, 18);
        let t1219 = pack_dttm(2019, 5, 15, 12, 19);
        let t1220 = pack_dttm(2019, 5, 15, 12, 20);
        let t1221 = pack_dttm(2019, 5, 15, 12, 21);

        let doc = ParsedDocument {
            runs: vec![
                insertion_run(0, 4, "aaaa", 0, t1218, 0x2A00),
                insertion_run(4, 8, "bbbb", 0, t1219, 0x2A00),
                insertion_run(8, 12, "cccc", 0, t1220, 0x2A00),
                insertion_run(12, 16, "dddd", 0, t1221, 0x2A00),
            ],
            authors: vec!["Alice".to_string()],
            bookmarks: vec![],
            style_defaults: Default::default(),
        };

        let redlines = super::build_redlines_for_debug(&doc);
        let insertions: Vec<_> = redlines
            .iter()
            .filter(|redline| redline.signature.revision_type == RevisionType::Insertion)
            .collect();

        assert_eq!(insertions.len(), 2);
        assert_eq!(insertions[0].start_cp, 0);
        assert_eq!(insertions[0].end_cp, 8);
        assert_eq!(insertions[0].signature.timestamp, Dttm::from_raw(t1218));
        assert_eq!(insertions[1].start_cp, 8);
        assert_eq!(insertions[1].end_cp, 16);
        assert_eq!(insertions[1].signature.timestamp, Dttm::from_raw(t1220));
    }

    #[test]
    fn merges_by_metadata_and_splits_short_formatting_runs() {
        let dttm = pack_dttm(2025, 1, 15, 10, 30);
        let doc = ParsedDocument {
            runs: vec![
                insertion_run(0, 2, "ab", 0, dttm, 0x2A00),
                insertion_run(2, 4, "CD", 0, dttm, 0x2A01),
            ],
            authors: vec!["Alice".to_string()],
            bookmarks: vec![],
            style_defaults: Default::default(),
        };

        let out = extract_revisions(&doc);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].text, "ab");
        assert_eq!(out[1].text, "CD");
        assert_eq!(out[0].author.as_deref(), Some("Alice"));
    }

    #[test]
    fn structural_and_bookmark_boundaries_split_output() {
        let dttm = pack_dttm(2025, 1, 15, 10, 30);
        let run = insertion_run(0, 8, "ab\rdefgh", 0, dttm, 0x2A00);
        let doc = ParsedDocument {
            runs: vec![run],
            authors: vec!["Alice".to_string()],
            bookmarks: vec![Bookmark {
                name: "_cp_change_12".to_string(),
                start_cp: 4,
                end_cp: 6,
            }],
            style_defaults: Default::default(),
        };

        let out = extract_revisions(&doc);
        let segments: Vec<(u32, u32)> = out
            .iter()
            .filter(|entry| !entry.text.is_empty())
            .map(|entry| (entry.start_cp, entry.end_cp))
            .collect();
        assert_eq!(segments, vec![(0, 3), (3, 4), (4, 6), (6, 8)]);
        assert!(
            out.iter()
                .any(|entry| entry.text.is_empty() && entry.start_cp == entry.end_cp)
        );
        assert_eq!(
            out.iter()
                .find(|entry| !entry.text.is_empty())
                .unwrap()
                .text,
            "ab "
        );
    }

    #[test]
    fn dual_run_emits_insertion_and_deletion_entries() {
        let ins_dttm = pack_dttm(2025, 1, 10, 9, 0);
        let del_dttm = pack_dttm(2025, 1, 10, 9, 1);
        let run = ChpxRun {
            start_cp: 0,
            end_cp: 3,
            text: "xyz".to_string(),
            sprms: vec![
                Sprm {
                    opcode: SPRM_FRMARK,
                    operand: vec![1],
                },
                Sprm {
                    opcode: SPRM_FRMARK_DEL,
                    operand: vec![1],
                },
                Sprm {
                    opcode: SPRM_IBST_RMARK,
                    operand: 0_u16.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: SPRM_DTTM_RMARK,
                    operand: ins_dttm.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: SPRM_IBST_RMARK_DEL_WW8,
                    operand: 1_u16.to_le_bytes().to_vec(),
                },
                Sprm {
                    opcode: SPRM_DTTM_RMARK_DEL_WW8,
                    operand: del_dttm.to_le_bytes().to_vec(),
                },
            ],
            source_chpx_id: None,
        };

        let doc = ParsedDocument {
            runs: vec![run],
            authors: vec!["Alice".to_string(), "Bob".to_string()],
            bookmarks: vec![],
            style_defaults: Default::default(),
        };

        let out = extract_revisions(&doc);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].revision_type, RevisionType::Insertion);
        assert_eq!(out[1].revision_type, RevisionType::Deletion);
    }

    fn candidate_with_timestamp(
        start_cp: u32,
        end_cp: u32,
        revision_type: RevisionType,
        dttm_raw: u32,
    ) -> RevisionCandidate {
        RevisionCandidate {
            signature: RedlineSignature {
                revision_type,
                author_index: Some(0),
                timestamp: Dttm::from_raw(dttm_raw),
                stack: None,
            },
            start_cp,
            end_cp,
            segments: vec![],
        }
    }

    #[test]
    fn lo_candidate_order_uses_timestamp_then_insert_delete_tiebreak() {
        let ts_0900 = pack_dttm(2025, 1, 10, 9, 0);
        let ts_0901 = pack_dttm(2025, 1, 10, 9, 1);
        let mut candidates = vec![
            candidate_with_timestamp(0, 3, RevisionType::Deletion, ts_0900),
            candidate_with_timestamp(0, 3, RevisionType::Insertion, ts_0901),
            candidate_with_timestamp(0, 3, RevisionType::Insertion, ts_0900),
        ];

        sort_candidates_for_lo_append(&mut candidates);

        let ordered: Vec<(RevisionType, u32)> = candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.signature.revision_type,
                    candidate.signature.timestamp.unwrap().raw,
                )
            })
            .collect();

        assert_eq!(
            ordered,
            vec![
                (RevisionType::Insertion, ts_0900),
                (RevisionType::Deletion, ts_0900),
                (RevisionType::Insertion, ts_0901),
            ]
        );
    }

    #[test]
    fn one_cp_dual_noop_bridges_adjacent_insertions() {
        let doc = ParsedDocument {
            runs: vec![
                ChpxRun {
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
                            operand: 0_u16.to_le_bytes().to_vec(),
                        },
                    ],
                    source_chpx_id: None,
                },
                ChpxRun {
                    start_cp: 3,
                    end_cp: 4,
                    text: "x".to_string(),
                    sprms: vec![
                        Sprm {
                            opcode: SPRM_FRMARK,
                            operand: vec![1],
                        },
                        Sprm {
                            opcode: SPRM_FRMARK_DEL,
                            operand: vec![1],
                        },
                        Sprm {
                            opcode: SPRM_IBST_RMARK,
                            operand: 0_u16.to_le_bytes().to_vec(),
                        },
                        Sprm {
                            opcode: SPRM_IBST_RMARK_DEL_WW8,
                            operand: 0_u16.to_le_bytes().to_vec(),
                        },
                    ],
                    source_chpx_id: None,
                },
                ChpxRun {
                    start_cp: 4,
                    end_cp: 7,
                    text: "def".to_string(),
                    sprms: vec![
                        Sprm {
                            opcode: SPRM_FRMARK,
                            operand: vec![1],
                        },
                        Sprm {
                            opcode: SPRM_IBST_RMARK,
                            operand: 0_u16.to_le_bytes().to_vec(),
                        },
                    ],
                    source_chpx_id: None,
                },
            ],
            authors: vec!["Alice".to_string()],
            bookmarks: vec![],
            style_defaults: Default::default(),
        };

        let out = extract_revisions(&doc);
        let insertions: Vec<_> = out
            .iter()
            .filter(|entry| entry.revision_type == RevisionType::Insertion)
            .collect();
        let deletions: Vec<_> = out
            .iter()
            .filter(|entry| entry.revision_type == RevisionType::Deletion)
            .collect();

        assert_eq!(insertions.len(), 1);
        assert_eq!(insertions[0].text, "abcdef");
        assert!(deletions.is_empty());
    }

    #[test]
    fn one_cp_dual_noop_requires_insertion_neighbors_on_both_sides() {
        let doc = ParsedDocument {
            runs: vec![
                ChpxRun {
                    start_cp: 0,
                    end_cp: 2,
                    text: "ab".to_string(),
                    sprms: vec![
                        Sprm {
                            opcode: SPRM_FRMARK_DEL,
                            operand: vec![1],
                        },
                        Sprm {
                            opcode: SPRM_IBST_RMARK_DEL_WW8,
                            operand: 0_u16.to_le_bytes().to_vec(),
                        },
                    ],
                    source_chpx_id: None,
                },
                ChpxRun {
                    start_cp: 2,
                    end_cp: 3,
                    text: "x".to_string(),
                    sprms: vec![
                        Sprm {
                            opcode: SPRM_FRMARK,
                            operand: vec![1],
                        },
                        Sprm {
                            opcode: SPRM_FRMARK_DEL,
                            operand: vec![1],
                        },
                        Sprm {
                            opcode: SPRM_IBST_RMARK,
                            operand: 0_u16.to_le_bytes().to_vec(),
                        },
                        Sprm {
                            opcode: SPRM_IBST_RMARK_DEL_WW8,
                            operand: 0_u16.to_le_bytes().to_vec(),
                        },
                    ],
                    source_chpx_id: None,
                },
                ChpxRun {
                    start_cp: 3,
                    end_cp: 6,
                    text: "cde".to_string(),
                    sprms: vec![
                        Sprm {
                            opcode: SPRM_FRMARK,
                            operand: vec![1],
                        },
                        Sprm {
                            opcode: SPRM_IBST_RMARK,
                            operand: 0_u16.to_le_bytes().to_vec(),
                        },
                    ],
                    source_chpx_id: None,
                },
            ],
            authors: vec!["Alice".to_string()],
            bookmarks: vec![],
            style_defaults: Default::default(),
        };

        let out = extract_revisions(&doc);
        assert!(out.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 2
                && entry.end_cp == 3
                && entry.text == "x"
        }));
        assert!(out.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 2
                && entry.end_cp == 3
                && entry.text == "x"
        }));
    }

    #[test]
    fn mid_paragraph_empty_insertions_are_suppressed() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: String::new(),
                author: Some("Author".to_string()),
                timestamp: None,
                start_cp: 10,
                end_cp: 11,
                paragraph_index: Some(3),
                char_offset: Some(14),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: String::new(),
                author: Some("Author".to_string()),
                timestamp: None,
                start_cp: 20,
                end_cp: 21,
                paragraph_index: Some(4),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: String::new(),
                author: Some("Author".to_string()),
                timestamp: None,
                start_cp: 25,
                end_cp: 25,
                paragraph_index: Some(4),
                char_offset: Some(9),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: " ".to_string(),
                author: Some("Author".to_string()),
                timestamp: None,
                start_cp: 30,
                end_cp: 31,
                paragraph_index: Some(4),
                char_offset: Some(9),
                context: None,
            },
        ];

        suppress_mid_paragraph_empty_insertions(&mut entries);

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].start_cp, 20);
        assert_eq!(entries[1].start_cp, 25);
        assert_eq!(entries[2].text, " ");
    }

    #[test]
    fn insertion_structural_tail_emits_empty_companion() {
        assert!(emit_empty_insertion_companion(
            &RevisionType::Insertion,
            "inserted paragraph\r",
        ));
        assert!(emit_empty_insertion_companion(
            &RevisionType::Insertion,
            "\r",
        ));
        assert!(!emit_empty_insertion_companion(
            &RevisionType::Insertion,
            "plain inserted text",
        ));
        assert!(!emit_empty_insertion_companion(
            &RevisionType::Deletion,
            "deleted paragraph\r",
        ));
    }

    #[test]
    fn dual_bridge_augmentation_adds_joined_insertion() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: " with the ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-23T11:18:00".to_string()),
                start_cp: 100,
                end_cp: 110,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Tenant’s".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-23T11:18:00".to_string()),
                start_cp: 110,
                end_cp: 118,
                paragraph_index: Some(1),
                char_offset: Some(10),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Tenant’s".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-07-26T16:00:00".to_string()),
                start_cp: 110,
                end_cp: 118,
                paragraph_index: Some(1),
                char_offset: Some(10),
                context: None,
            },
        ];

        augment_dual_bridge_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 100
                && entry.end_cp == 118
                && entry.text == " with the Tenant’s"
        }));
    }

    #[test]
    fn mirrored_full_span_augmentation_restores_uniform_stacked_sentence() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Security ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 10,
                end_cp: 19,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Security ".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 10,
                end_cp: 19,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Deposit ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 19,
                end_cp: 27,
                paragraph_index: Some(1),
                char_offset: Some(9),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Deposit ".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 19,
                end_cp: 27,
                paragraph_index: Some(1),
                char_offset: Some(9),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "shall remain.".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 27,
                end_cp: 40,
                paragraph_index: Some(1),
                char_offset: Some(17),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "shall remain.".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 27,
                end_cp: 40,
                paragraph_index: Some(1),
                char_offset: Some(17),
                context: None,
            },
        ];

        augment_mirrored_full_span_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 10
                && entry.end_cp == 40
                && entry.text == "Security Deposit shall remain."
        }));
        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 10
                && entry.end_cp == 40
                && entry.text == "Security Deposit shall remain."
        }));
    }

    #[test]
    fn mirrored_full_span_augmentation_bridges_punctuation_only_connector() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Tenant".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 50,
                end_cp: 56,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Tenant".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 50,
                end_cp: 56,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "’".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 56,
                end_cp: 57,
                paragraph_index: Some(1),
                char_offset: Some(6),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "’".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 56,
                end_cp: 57,
                paragraph_index: Some(1),
                char_offset: Some(6),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "s option".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 57,
                end_cp: 65,
                paragraph_index: Some(1),
                char_offset: Some(7),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "s option".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 57,
                end_cp: 65,
                paragraph_index: Some(1),
                char_offset: Some(7),
                context: None,
            },
        ];

        augment_mirrored_full_span_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 50
                && entry.end_cp == 65
                && entry.text == "Tenant’s option"
        }));
        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 50
                && entry.end_cp == 65
                && entry.text == "Tenant’s option"
        }));
    }

    #[test]
    fn mirrored_full_span_augmentation_ignores_same_span_ocr_variants() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "with Landlord".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 70,
                end_cp: 83,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "with Lanrd".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 70,
                end_cp: 83,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "with Landlord".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 70,
                end_cp: 83,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "with Lanrd".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 70,
                end_cp: 83,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "’".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 83,
                end_cp: 84,
                paragraph_index: Some(1),
                char_offset: Some(13),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "’".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 83,
                end_cp: 84,
                paragraph_index: Some(1),
                char_offset: Some(13),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "s cost contribution capped at".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 84,
                end_cp: 113,
                paragraph_index: Some(1),
                char_offset: Some(14),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "capped at".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 84,
                end_cp: 113,
                paragraph_index: Some(1),
                char_offset: Some(14),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "s cost contribution capped at".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 84,
                end_cp: 113,
                paragraph_index: Some(1),
                char_offset: Some(14),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "capped at".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 84,
                end_cp: 113,
                paragraph_index: Some(1),
                char_offset: Some(14),
                context: None,
            },
        ];

        augment_mirrored_full_span_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 70
                && entry.end_cp == 113
                && entry.text == "with Landlord’s cost contribution capped at"
        }));
        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 70
                && entry.end_cp == 113
                && entry.text == "with Landlord’s cost contribution capped at"
        }));
    }

    #[test]
    fn mirrored_full_span_augmentation_skips_same_start_overlap_variants() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Tenant shall ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 100,
                end_cp: 113,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "renew ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 113,
                end_cp: 119,
                paragraph_index: Some(1),
                char_offset: Some(13),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "today.".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 119,
                end_cp: 125,
                paragraph_index: Some(1),
                char_offset: Some(19),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Tenant shall ".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 100,
                end_cp: 113,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Tenant shall renew ".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 100,
                end_cp: 119,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "renew ".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 113,
                end_cp: 119,
                paragraph_index: Some(1),
                char_offset: Some(13),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "today.".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 119,
                end_cp: 125,
                paragraph_index: Some(1),
                char_offset: Some(19),
                context: None,
            },
        ];

        augment_mirrored_full_span_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 100
                && entry.end_cp == 125
                && entry.text == "Tenant shall renew today."
        }));
    }

    #[test]
    fn mirrored_full_span_augmentation_respects_opposite_side_transition_boundaries() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Alpha ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 200,
                end_cp: 206,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Beta".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:05:00".to_string()),
                start_cp: 206,
                end_cp: 210,
                paragraph_index: Some(1),
                char_offset: Some(6),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: " Gamma".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-01-01T09:00:00".to_string()),
                start_cp: 210,
                end_cp: 216,
                paragraph_index: Some(1),
                char_offset: Some(10),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Alpha ".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 200,
                end_cp: 206,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Beta".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 206,
                end_cp: 210,
                paragraph_index: Some(1),
                char_offset: Some(6),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: " Gamma".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-01-02T09:00:00".to_string()),
                start_cp: 210,
                end_cp: 216,
                paragraph_index: Some(1),
                char_offset: Some(10),
                context: None,
            },
        ];

        augment_mirrored_full_span_entries(&mut entries);

        assert!(!entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 200
                && entry.end_cp == 216
                && entry.text == "Alpha Beta Gamma"
        }));
    }

    #[test]
    fn dual_bridge_augmentation_bridges_across_mirrored_deletion() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "e".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-23T11:18:00".to_string()),
                start_cp: 200,
                end_cp: 201,
                paragraph_index: Some(2),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "e".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-07-26T16:00:00".to_string()),
                start_cp: 200,
                end_cp: 201,
                paragraph_index: Some(2),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "xisting ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-23T11:18:00".to_string()),
                start_cp: 201,
                end_cp: 208,
                paragraph_index: Some(2),
                char_offset: Some(1),
                context: None,
            },
        ];

        augment_dual_bridge_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 200
                && entry.end_cp == 208
                && entry.text == "existing "
        }));
    }

    #[test]
    fn dual_bridge_augmentation_adds_joined_deletion() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Test Perso".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-23T11:18:00".to_string()),
                start_cp: 300,
                end_cp: 313,
                paragraph_index: Some(3),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Test Person".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-07-24T11:18:00".to_string()),
                start_cp: 300,
                end_cp: 314,
                paragraph_index: Some(3),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "d".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-23T11:18:00".to_string()),
                start_cp: 313,
                end_cp: 314,
                paragraph_index: Some(3),
                char_offset: Some(13),
                context: None,
            },
        ];

        augment_dual_bridge_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 300
                && entry.end_cp == 314
                && entry.text == "Test Person"
        }));
    }

    #[test]
    fn midword_adjacent_insertion_alias_merges_contiguous_fragments() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "installed as part of Tenant’s ini".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-09-02T14:09:00".to_string()),
                start_cp: 2790,
                end_cp: 2907,
                paragraph_index: Some(108),
                char_offset: Some(15),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "tial occupancy. ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-09-02T14:11:00".to_string()),
                start_cp: 2907,
                end_cp: 2923,
                paragraph_index: Some(108),
                char_offset: Some(48),
                context: None,
            },
        ];

        augment_midword_adjacent_insertions(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 2790
                && entry.end_cp == 2923
                && entry.text == "installed as part of Tenant’s initial occupancy. "
        }));
    }

    #[test]
    fn midword_adjacent_insertion_alias_skips_sentence_boundary() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: ", with the votes allocated to the other Members being reduced accordingly"
                    .to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-03-19T15:29:00".to_string()),
                start_cp: 21691,
                end_cp: 21764,
                paragraph_index: Some(37),
                char_offset: Some(1021),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: ".  In no event shall Landlord hold less than forty percent".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-03-19T15:29:00".to_string()),
                start_cp: 21764,
                end_cp: 21820,
                paragraph_index: Some(37),
                char_offset: Some(1094),
                context: None,
            },
        ];

        augment_midword_adjacent_insertions(&mut entries);

        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn whitespace_adjacent_insertions_merge_across_empty_companion() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "floor common areas shall be at no cost for the lease term and any "
                    .to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-04-01T11:37:00".to_string()),
                start_cp: 8410,
                end_cp: 8477,
                paragraph_index: Some(44),
                char_offset: Some(132),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-04-01T11:37:00".to_string()),
                start_cp: 8477,
                end_cp: 8477,
                paragraph_index: Some(44),
                char_offset: Some(198),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "extension terms thereof. ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-04-01T11:37:00".to_string()),
                start_cp: 8477,
                end_cp: 8502,
                paragraph_index: Some(44),
                char_offset: Some(198),
                context: None,
            },
        ];

        augment_whitespace_adjacent_insertions_across_empty_companions(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 8410
                && entry.end_cp == 8502
                && entry.text
                    == "floor common areas shall be at no cost for the lease term and any extension terms thereof. "
        }));
    }

    #[test]
    fn whitespace_adjacent_insertions_merge_across_empty_companion_with_interleaved_overlap() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: " floor common areas shall be at no cost for the lease term and any "
                    .to_string(),
                author: Some("Test Author".to_string()),
                timestamp: Some("2025-04-01T11:37:00".to_string()),
                start_cp: 8428,
                end_cp: 8477,
                paragraph_index: Some(44),
                char_offset: Some(99),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "s shall be at no cost for the lease term and any extension terms thereof. "
                    .to_string(),
                author: Some("Test Author".to_string()),
                timestamp: Some("2025-04-01T11:37:00".to_string()),
                start_cp: 8429,
                end_cp: 8502,
                paragraph_index: Some(44),
                char_offset: Some(117),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "".to_string(),
                author: Some("Test Author".to_string()),
                timestamp: Some("2025-04-01T11:37:00".to_string()),
                start_cp: 8477,
                end_cp: 8477,
                paragraph_index: Some(44),
                char_offset: Some(166),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "extension terms thereof. ".to_string(),
                author: Some("Test Author".to_string()),
                timestamp: Some("2025-04-01T11:37:00".to_string()),
                start_cp: 8477,
                end_cp: 8502,
                paragraph_index: Some(44),
                char_offset: Some(166),
                context: None,
            },
        ];

        augment_whitespace_adjacent_insertions_across_empty_companions(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 8428
                && entry.end_cp == 8502
                && entry.text
                    == " floor common areas shall be at no cost for the lease term and any extension terms thereof. "
        }));
    }

    #[test]
    fn punctuation_adjacent_entries_emit_combined_alias() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Additionally".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2021-11-02T15:46:00".to_string()),
                start_cp: 3274,
                end_cp: 3286,
                paragraph_index: Some(0),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: ", ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2021-11-02T15:46:00".to_string()),
                start_cp: 3286,
                end_cp: 3288,
                paragraph_index: Some(0),
                char_offset: Some(12),
                context: None,
            },
        ];

        augment_punctuation_adjacent_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 3274
                && entry.end_cp == 3288
                && entry.text == "Additionally, "
        }));
    }

    #[test]
    fn sentence_adjacent_insertions_across_timestamp_transition_emit_alias() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Property Management is on-site. ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-06-25T14:01:00".to_string()),
                start_cp: 1416,
                end_cp: 1448,
                paragraph_index: Some(0),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "On-site personnel".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-06-25T14:03:00".to_string()),
                start_cp: 1448,
                end_cp: 1465,
                paragraph_index: Some(0),
                char_offset: Some(32),
                context: None,
            },
        ];

        augment_sentence_adjacent_insertions_across_timestamp_transition(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 1416
                && entry.end_cp == 1465
                && entry.text == "Property Management is on-site. On-site personnel"
        }));
    }

    #[test]
    fn defined_term_adjacent_insertions_after_short_prefix_emit_alias() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: " Not".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-10-22T07:52:00".to_string()),
                start_cp: 10,
                end_cp: 14,
                paragraph_index: Some(0),
                char_offset: Some(10),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "withstanding the foregoing, if Landlord does not cause the ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-10-22T07:52:00".to_string()),
                start_cp: 14,
                end_cp: 73,
                paragraph_index: Some(0),
                char_offset: Some(14),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Premises Delivery Date to occur on or before ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-10-22T07:54:00".to_string()),
                start_cp: 73,
                end_cp: 118,
                paragraph_index: Some(0),
                char_offset: Some(73),
                context: None,
            },
        ];

        augment_defined_term_adjacent_insertions_after_short_prefix(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 14
                && entry.end_cp == 118
                && entry.timestamp.as_deref() == Some("2025-10-22T07:54:00")
                && entry.text
                    == "withstanding the foregoing, if Landlord does not cause the Premises Delivery Date to occur on or before "
        }));
    }

    #[test]
    fn short_token_insertions_across_timestamp_transition_emit_alias() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "with prior Landlord approval to complete the improvements. Tenant shall be charged a "
                    .to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-08T19:40:00".to_string()),
                start_cp: 4624,
                end_cp: 4709,
                paragraph_index: Some(0),
                char_offset: Some(4624),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "3% ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-08T19:43:00".to_string()),
                start_cp: 4709,
                end_cp: 4712,
                paragraph_index: Some(0),
                char_offset: Some(4709),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "supervision fee for the improvements.".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-08T19:41:00".to_string()),
                start_cp: 4712,
                end_cp: 4749,
                paragraph_index: Some(0),
                char_offset: Some(4712),
                context: None,
            },
        ];

        augment_short_token_insertions_across_timestamp_transition(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 4624
                && entry.end_cp == 4749
                && entry.text
                    == "with prior Landlord approval to complete the improvements. Tenant shall be charged a 3% supervision fee for the improvements."
        }));
    }

    #[test]
    fn midword_adjacent_insertions_across_timestamp_transition_emit_alias() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "On-site personnel includes one senior property manager, assistant property manager, building engineer and d".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-06-25T14:01:00".to_string()),
                start_cp: 1479,
                end_cp: 1586,
                paragraph_index: Some(0),
                char_offset: Some(63),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "ay porter.  ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-06-25T14:03:00".to_string()),
                start_cp: 1586,
                end_cp: 1598,
                paragraph_index: Some(0),
                char_offset: Some(170),
                context: None,
            },
        ];

        augment_midword_adjacent_insertions_across_timestamp_transition(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 1479
                && entry.end_cp == 1598
                && entry.text.ends_with("day porter.  ")
        }));
    }

    #[test]
    fn label_amount_line_item_aliases_emit_label_and_amount() {
        let mut entries = vec![RevisionEntry {
            revision_type: RevisionType::Insertion,
            text: "CAM:          $0.07/SF ".to_string(),
            author: Some("Author".to_string()),
            timestamp: Some("2021-11-03T11:39:00".to_string()),
            start_cp: 1468,
            end_cp: 1491,
            paragraph_index: Some(0),
            char_offset: Some(0),
            context: None,
        }];

        augment_label_amount_line_item_aliases(&mut entries);

        assert!(entries.iter().any(|entry| entry.text == "CAM:"));
        assert!(entries.iter().any(|entry| entry.text == "$0.07/SF"));
    }

    #[test]
    fn sentence_clause_aliases_from_tail_evidence_emit_both_sentences() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "/25: Example Property Management is on-site. On-site personnel includes one senior property manager, assistant property manager, building engineer and day porter.  ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-06-25T14:01:00".to_string()),
                start_cp: 1416,
                end_cp: 1598,
                paragraph_index: Some(14),
                char_offset: Some(16),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "ay porter.  ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-06-25T14:03:00".to_string()),
                start_cp: 1586,
                end_cp: 1598,
                paragraph_index: Some(14),
                char_offset: Some(186),
                context: None,
            },
        ];

        augment_sentence_clause_aliases_from_tail_evidence(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.text == "/25: Example Property Management is on-site."
        }));
        assert!(entries.iter().any(|entry| {
            entry.text == "On-site personnel includes one senior property manager, assistant property manager, building engineer and day porter."
        }));
    }

    #[test]
    fn mirrored_deletion_prefix_clip_alias_drops_mirrored_dual_length() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "as follows".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-02-23T00:00:00".to_string()),
                start_cp: 3345,
                end_cp: 3355,
                paragraph_index: Some(20),
                char_offset: Some(702),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "as follows".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-02-23T00:00:00".to_string()),
                start_cp: 3345,
                end_cp: 3355,
                paragraph_index: Some(20),
                char_offset: Some(702),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: ":".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-02-23T00:00:00".to_string()),
                start_cp: 3355,
                end_cp: 3356,
                paragraph_index: Some(20),
                char_offset: Some(712),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "at Fair Market Value (FMV) as mutually determined by Lessor and Lessee,"
                    .to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-02-23T00:00:00".to_string()),
                start_cp: 3356,
                end_cp: 3427,
                paragraph_index: Some(20),
                char_offset: Some(713),
                context: None,
            },
        ];

        augment_mirrored_deletion_prefix_clips(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 3356
                && entry.end_cp == 3427
                && entry.text == "rket Value (FMV) as mutually determined by Lessor and Lessee,"
        }));
    }

    #[test]
    fn ordinal_suffix_deletion_tail_alias_merges_tail_after_superscript_pair() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "Subject to the initial lease-up of the contiguous vacant available space on the 8"
                    .to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2022-11-09T13:19:00".to_string()),
                start_cp: 6121,
                end_cp: 6202,
                paragraph_index: Some(1),
                char_offset: Some(10),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "th".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2022-10-21T09:51:00".to_string()),
                start_cp: 6202,
                end_cp: 6204,
                paragraph_index: Some(1),
                char_offset: Some(91),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "th".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2022-11-09T13:19:00".to_string()),
                start_cp: 6202,
                end_cp: 6204,
                paragraph_index: Some(1),
                char_offset: Some(91),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: " ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2022-11-09T13:19:00".to_string()),
                start_cp: 6204,
                end_cp: 6205,
                paragraph_index: Some(1),
                char_offset: Some(93),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "floor, Tenant".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2022-11-09T13:19:00".to_string()),
                start_cp: 6205,
                end_cp: 6218,
                paragraph_index: Some(1),
                char_offset: Some(94),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "’s Right of First Offer, as desc".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2022-11-09T13:19:00".to_string()),
                start_cp: 6218,
                end_cp: 6250,
                paragraph_index: Some(1),
                char_offset: Some(107),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "ribed in the Addendum to Office Lease, shall remain in effect ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2022-11-09T13:19:00".to_string()),
                start_cp: 6250,
                end_cp: 6312,
                paragraph_index: Some(1),
                char_offset: Some(139),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "and be revised to apply to the Expansion Premises.".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2022-11-09T13:19:00".to_string()),
                start_cp: 6312,
                end_cp: 6362,
                paragraph_index: Some(1),
                char_offset: Some(201),
                context: None,
            },
        ];

        augment_ordinal_suffix_deletion_tails(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 6204
                && entry.end_cp == 6362
                && entry.text
                    == " floor, Tenant’s Right of First Offer, as described in the Addendum to Office Lease, shall remain in effect and be revised to apply to the Expansion Premises."
        }));
    }

    #[test]
    fn extract_revisions_emits_empty_deleted_annotation_reference() {
        assert!(deleted_annotation_reference_enabled());

        let doc = ParsedDocument {
            runs: vec![
                ChpxRun {
                    start_cp: 0,
                    end_cp: 5,
                    text: "plain".to_string(),
                    sprms: vec![],
                    source_chpx_id: Some(1),
                },
                ChpxRun {
                    start_cp: 5,
                    end_cp: 6,
                    text: "\u{0005}".to_string(),
                    sprms: vec![],
                    source_chpx_id: Some(2),
                },
                deletion_run(6, 12, "delete", 0, pack_dttm(2025, 1, 1, 9, 0), 0x2A00),
            ],
            authors: vec!["Author".to_string()],
            bookmarks: vec![Bookmark {
                name: "_annotation_mark_4".to_string(),
                start_cp: 1,
                end_cp: 5,
            }],
            style_defaults: Default::default(),
        };

        let revisions = extract_revisions(&doc);

        assert!(revisions.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 5
                && entry.end_cp == 5
                && entry.text.is_empty()
        }));
        assert!(revisions.iter().any(|entry| {
            entry.revision_type == RevisionType::Deletion
                && entry.start_cp == 6
                && entry.end_cp == 12
                && entry.text == "delete"
        }));
    }

    #[test]
    fn lo_current_text_overlap_alias_uses_stale_dual_positions() {
        let block = vec![
            LoOverlapRunInfo {
                start_cp: 0,
                end_cp: 1,
                text: "x".to_string(),
                has_insertion: false,
                has_deletion: true,
                insertion_author: None,
                insertion_timestamp: None,
                deletion_author: Some(0),
                deletion_timestamp: None,
            },
            LoOverlapRunInfo {
                start_cp: 1,
                end_cp: 2,
                text: "A".to_string(),
                has_insertion: true,
                has_deletion: true,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: Some(0),
                deletion_timestamp: None,
            },
            LoOverlapRunInfo {
                start_cp: 2,
                end_cp: 3,
                text: "B".to_string(),
                has_insertion: true,
                has_deletion: true,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: Some(0),
                deletion_timestamp: None,
            },
            LoOverlapRunInfo {
                start_cp: 3,
                end_cp: 9,
                text: "CDEFGH".to_string(),
                has_insertion: true,
                has_deletion: false,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: None,
                deletion_timestamp: None,
            },
        ];

        let (deletion_aliases, insertion_aliases, _barriers) =
            simulate_lo_current_text_overlap_block(&block, &[(0, 1)]);

        assert_eq!(
            deletion_aliases
                .iter()
                .map(|span| span.text.as_str())
                .collect::<Vec<_>>(),
            vec!["A", "C"]
        );
        assert_eq!(
            insertion_aliases
                .iter()
                .map(|span| span.text.as_str())
                .collect::<Vec<_>>(),
            vec!["B", "DEFGH"]
        );
    }

    #[test]
    fn lo_current_text_overlap_alias_can_leave_numeric_fragment() {
        let block = vec![
            LoOverlapRunInfo {
                start_cp: 0,
                end_cp: 1,
                text: "4".to_string(),
                has_insertion: false,
                has_deletion: true,
                insertion_author: None,
                insertion_timestamp: None,
                deletion_author: Some(0),
                deletion_timestamp: None,
            },
            LoOverlapRunInfo {
                start_cp: 1,
                end_cp: 2,
                text: "1".to_string(),
                has_insertion: true,
                has_deletion: true,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: Some(0),
                deletion_timestamp: None,
            },
            LoOverlapRunInfo {
                start_cp: 2,
                end_cp: 4,
                text: "07".to_string(),
                has_insertion: true,
                has_deletion: false,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: None,
                deletion_timestamp: None,
            },
        ];

        let (deletion_aliases, insertion_aliases, _barriers) =
            simulate_lo_current_text_overlap_block(&block, &[(0, 1)]);

        assert_eq!(
            deletion_aliases
                .iter()
                .map(|span| span.text.as_str())
                .collect::<Vec<_>>(),
            vec!["1"]
        );
        assert_eq!(
            insertion_aliases
                .iter()
                .map(|span| span.text.as_str())
                .collect::<Vec<_>>(),
            vec!["07"]
        );
    }

    #[test]
    fn lo_current_text_overlap_clipped_alias_compacts_insertions() {
        let block = vec![
            LoOverlapRunInfo {
                start_cp: 0,
                end_cp: 1,
                text: "4".to_string(),
                has_insertion: false,
                has_deletion: true,
                insertion_author: None,
                insertion_timestamp: None,
                deletion_author: Some(0),
                deletion_timestamp: None,
            },
            LoOverlapRunInfo {
                start_cp: 1,
                end_cp: 2,
                text: "1".to_string(),
                has_insertion: true,
                has_deletion: true,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: Some(0),
                deletion_timestamp: None,
            },
            LoOverlapRunInfo {
                start_cp: 2,
                end_cp: 4,
                text: "07".to_string(),
                has_insertion: true,
                has_deletion: false,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: None,
                deletion_timestamp: None,
            },
        ];

        let span = simulate_lo_current_text_overlap_clipped_alias(&block).expect("span");
        assert_eq!(span.text, "07");
        assert_eq!(span.start_cp, 2);
        assert_eq!(span.end_cp, 4);
    }

    #[test]
    fn lo_current_text_overlap_range_mutation_clips_inside_range() {
        let block = vec![
            LoOverlapRunInfo {
                start_cp: 0,
                end_cp: 1,
                text: "A".to_string(),
                has_insertion: true,
                has_deletion: false,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: None,
                deletion_timestamp: None,
            },
            LoOverlapRunInfo {
                start_cp: 1,
                end_cp: 3,
                text: "BC".to_string(),
                has_insertion: true,
                has_deletion: true,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: Some(0),
                deletion_timestamp: None,
            },
            LoOverlapRunInfo {
                start_cp: 3,
                end_cp: 4,
                text: "D".to_string(),
                has_insertion: true,
                has_deletion: false,
                insertion_author: Some(0),
                insertion_timestamp: None,
                deletion_author: None,
                deletion_timestamp: None,
            },
        ];

        let span = simulate_lo_current_text_overlap_range_mutation_alias(&block).expect("span");
        assert_eq!(span.text, "AD");
        assert_eq!(span.start_cp, 0);
        assert_eq!(span.end_cp, 4);
    }

    #[test]
    fn lo_current_text_overlap_alias_rejects_mid_run_islands() {
        assert!(!lo_current_text_overlap_span_respects_run_edges(&[
            LoOverlapVisibleChar {
                ch: 'a',
                cp: 10,
                inserted: true,
                run_idx: 0,
                offset_in_run: 1,
                run_len: 4,
            },
        ]));
        assert!(lo_current_text_overlap_span_respects_run_edges(&[
            LoOverlapVisibleChar {
                ch: 'B',
                cp: 10,
                inserted: true,
                run_idx: 0,
                offset_in_run: 0,
                run_len: 4,
            },
            LoOverlapVisibleChar {
                ch: 'C',
                cp: 11,
                inserted: true,
                run_idx: 0,
                offset_in_run: 1,
                run_len: 4,
            },
        ]));
        assert!(lo_current_text_overlap_alias_text_ok(
            "7",
            RevisionType::Insertion
        ));
    }

    #[test]
    fn dual_bridge_augmentation_emits_overlap_alias_fragments() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "terms of the E".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-26T16:00:00".to_string()),
                start_cp: 10,
                end_cp: 23,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "x".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-07-26T16:00:00".to_string()),
                start_cp: 23,
                end_cp: 24,
                paragraph_index: Some(1),
                char_offset: Some(13),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: " floor ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-23T12:00:00".to_string()),
                start_cp: 30,
                end_cp: 37,
                paragraph_index: Some(1),
                char_offset: Some(20),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "y".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-07-26T16:01:00".to_string()),
                start_cp: 37,
                end_cp: 38,
                paragraph_index: Some(1),
                char_offset: Some(27),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "Suite X ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-26T16:01:00".to_string()),
                start_cp: 37,
                end_cp: 48,
                paragraph_index: Some(1),
                char_offset: Some(27),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "as of September 30, 2027".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-23T12:02:00".to_string()),
                start_cp: 60,
                end_cp: 84,
                paragraph_index: Some(1),
                char_offset: Some(40),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "z".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-07-26T16:01:00".to_string()),
                start_cp: 84,
                end_cp: 85,
                paragraph_index: Some(1),
                char_offset: Some(64),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: ", or such later date as ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-24T17:04:00".to_string()),
                start_cp: 90,
                end_cp: 113,
                paragraph_index: Some(1),
                char_offset: Some(70),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "agreed to as".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-07-26T16:01:00".to_string()),
                start_cp: 113,
                end_cp: 124,
                paragraph_index: Some(1),
                char_offset: Some(93),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "agreed to as mutually agreed".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-24T17:04:00".to_string()),
                start_cp: 113,
                end_cp: 140,
                paragraph_index: Some(1),
                char_offset: Some(93),
                context: None,
            },
        ];

        augment_dual_bridge_entries(&mut entries);

        let insertion_texts: Vec<&str> = entries
            .iter()
            .filter(|entry| entry.revision_type == RevisionType::Insertion)
            .map(|entry| entry.text.as_str())
            .collect();

        assert!(insertion_texts.contains(&"the E"));
        assert!(insertion_texts.contains(&" flo "));
        assert!(insertion_texts.contains(&"Se 2 "));
        assert!(insertion_texts.contains(&"as of Seper 30, 2027"));
        assert!(insertion_texts.contains(&", or such later date as agreed to as mutuareed"));
    }

    #[test]
    fn dual_bridge_augmentation_aliases_at_metadata_transition_boundaries() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "as of September 30, 2027".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-23T12:02:00".to_string()),
                start_cp: 10,
                end_cp: 34,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: ", or such later date as ".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-24T17:04:00".to_string()),
                start_cp: 34,
                end_cp: 58,
                paragraph_index: Some(1),
                char_offset: Some(24),
                context: None,
            },
        ];

        augment_dual_bridge_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion && entry.text == "as of Seper 30, 2027"
        }));
    }

    #[test]
    fn dual_bridge_augmentation_merges_overlapping_same_meta_insertions() {
        let mut entries = vec![
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: ", or such later date as agreed to as".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-24T17:04:00".to_string()),
                start_cp: 100,
                end_cp: 136,
                paragraph_index: Some(1),
                char_offset: Some(0),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Deletion,
                text: "agreed to as".to_string(),
                author: Some("Other".to_string()),
                timestamp: Some("2025-07-26T16:01:00".to_string()),
                start_cp: 124,
                end_cp: 136,
                paragraph_index: Some(1),
                char_offset: Some(24),
                context: None,
            },
            RevisionEntry {
                revision_type: RevisionType::Insertion,
                text: "agreed to as mutuareed".to_string(),
                author: Some("Author".to_string()),
                timestamp: Some("2025-07-24T17:04:00".to_string()),
                start_cp: 124,
                end_cp: 152,
                paragraph_index: Some(1),
                char_offset: Some(24),
                context: None,
            },
        ];

        augment_dual_bridge_entries(&mut entries);

        assert!(entries.iter().any(|entry| {
            entry.revision_type == RevisionType::Insertion
                && entry.start_cp == 100
                && entry.end_cp == 152
                && entry.text == ", or such later date as agreed to as mutuareed"
        }));
    }
}
