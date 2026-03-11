use std::collections::BTreeSet;
use std::sync::OnceLock;

use crate::dttm::{Dttm, timestamps_compatible};
use crate::model::{Bookmark, ChpxRun, RevisionType, SourceSegment};

pub fn split_points_for_redline(
    start_cp: u32,
    end_cp: u32,
    revision_type: RevisionType,
    stacked: bool,
    segments: &[SourceSegment],
    runs: &[ChpxRun],
    bookmarks: &[Bookmark],
) -> Vec<u32> {
    let mut points = BTreeSet::new();
    points.insert(start_cp);
    points.insert(end_cp);

    let mut ordered_segments = segments.to_vec();
    ordered_segments.sort_by_key(|segment| (segment.start_cp, segment.end_cp));
    let timestamp_span_gt_one = segment_timestamp_span_gt_one_minute(&ordered_segments);
    let uniform_formatting = redline_has_uniform_formatting(&ordered_segments);
    let strict = lo_splitter_strict_enabled();

    for pair in ordered_segments.windows(2) {
        let left = &pair[0];
        let right = &pair[1];

        if right.start_cp <= start_cp || right.start_cp >= end_cp {
            continue;
        }

        if strict {
            points.insert(right.start_cp);
            continue;
        }
        if stacked {
            // LO emits separate XML runs for each CHPX boundary even when
            // stacked redlines are active.
            points.insert(right.start_cp);
            continue;
        }
        if should_split_on_format_change(
            left,
            right,
            revision_type,
            stacked,
            ordered_segments.len(),
            uniform_formatting,
        ) {
            points.insert(right.start_cp);
        }
    }

    if !strict && !stacked && revision_type == RevisionType::Deletion {
        for triple in ordered_segments.windows(3) {
            let left = &triple[0];
            let middle = &triple[1];
            let right = &triple[2];

            if right.start_cp <= start_cp || right.start_cp >= end_cp {
                continue;
            }
            if deletion_comma_clause_format_excursion_split(left, middle, right) {
                points.insert(right.start_cp);
            }
        }
    }

    if !strict && stacked {
        for triple in ordered_segments.windows(3) {
            let left = &triple[0];
            let middle = &triple[1];
            let right = &triple[2];

            if stacked_deletion_singleton_triplet_merge_guard(
                left,
                middle,
                right,
                revision_type,
                stacked,
            ) {
                points.remove(&middle.start_cp);
                points.remove(&right.start_cp);
                continue;
            }

            if middle.start_cp > start_cp
                && middle.start_cp < end_cp
                && stacked_middle_singleton_source_split(left, middle, right)
            {
                points.insert(middle.start_cp);
            }
            if right.start_cp > start_cp
                && right.start_cp < end_cp
                && stacked_middle_singleton_source_split(left, middle, right)
            {
                points.insert(right.start_cp);
            }
        }
    }

    if !strict
        && timestamp_oscillation_split_enabled()
        && revision_type == RevisionType::Insertion
        && timestamp_span_gt_one
    {
        let anchor_ts = segment_min_timestamp(&ordered_segments);
        for triple in ordered_segments.windows(3) {
            let left = &triple[0];
            let middle = &triple[1];
            let right = &triple[2];

            if right.start_cp <= start_cp || right.start_cp >= end_cp {
                continue;
            }
            if !is_timestamp_oscillation_boundary(left, middle, right, anchor_ts) {
                continue;
            }

            points.insert(right.start_cp);
        }

        if !stacked {
            insert_timestamp_excursion_block_splits(
                &mut points,
                start_cp,
                end_cp,
                &ordered_segments,
                anchor_ts,
            );
            insert_anchor_compat_excursion_block_splits(
                &mut points,
                start_cp,
                end_cp,
                &ordered_segments,
                anchor_ts,
            );
            insert_anchor_compat_terminal_tail_excursion_split(
                &mut points,
                start_cp,
                end_cp,
                &ordered_segments,
                anchor_ts,
            );
            insert_anchor_compat_midword_return_split(
                &mut points,
                start_cp,
                end_cp,
                &ordered_segments,
                anchor_ts,
            );
            insert_anchor_compat_midword_prefix_splits(
                &mut points,
                start_cp,
                end_cp,
                &ordered_segments,
                anchor_ts,
            );
            insert_anchor_compat_lowercase_clause_excursion_split(
                &mut points,
                start_cp,
                end_cp,
                &ordered_segments,
                anchor_ts,
            );
            insert_anchor_compat_word_boundary_excursion_splits(
                &mut points,
                start_cp,
                end_cp,
                &ordered_segments,
                anchor_ts,
            );
            insert_anchor_incompatible_segment_splits(
                &mut points,
                start_cp,
                end_cp,
                &ordered_segments,
                anchor_ts,
            );
        }
    }

    if !strict && revision_type == RevisionType::Insertion && !stacked {
        for triple in ordered_segments.windows(3) {
            let left = &triple[0];
            let middle = &triple[1];
            let right = &triple[2];

            if right.start_cp <= start_cp || right.start_cp >= end_cp {
                continue;
            }
            if middle.start_cp > start_cp
                && middle.start_cp < end_cp
                && insertion_timestamp_return_sentence_split(
                    left,
                    middle,
                    right,
                    revision_type,
                    stacked,
                )
            {
                points.insert(middle.start_cp);
                continue;
            }
            if insertion_timestamp_return_midword_split(left, middle, right, revision_type, stacked)
            {
                points.insert(right.start_cp);
                continue;
            }
            if !insertion_punctuation_space_lowercase_triplet_split(
                left,
                middle,
                right,
                revision_type,
                stacked,
            ) {
                continue;
            }

            points.insert(right.start_cp);
        }
    }

    for segment in &ordered_segments {
        for (idx, ch) in segment.text.chars().enumerate() {
            let char_cp = segment.start_cp.saturating_add(idx as u32);
            if splits_before_structural_char(ch) && char_cp > start_cp && char_cp < end_cp {
                points.insert(char_cp);
            }
            if splits_after_structural_char(ch) {
                let split_cp = char_cp.saturating_add(1);
                if split_cp > start_cp && split_cp < end_cp {
                    points.insert(split_cp);
                }
            }
        }
    }

    if !strict && field_parenthetical_split_enabled() {
        for triple in ordered_segments.windows(3) {
            let left = &triple[0];
            let middle = &triple[1];
            let right = &triple[2];

            if !is_field_parenthetical_left_boundary(&left.text) {
                continue;
            }
            if !is_field_parenthetical_right_boundary(&right.text) {
                continue;
            }

            let Some(paren_idx) = split_parenthetical_after_leading_digits(&middle.text) else {
                continue;
            };
            let split_cp = middle.start_cp.saturating_add(paren_idx as u32);
            if split_cp > start_cp && split_cp < end_cp {
                points.insert(split_cp);
            }
        }
    }

    if !strict {
        insert_raw_run_boundary_points(
            &mut points,
            start_cp,
            end_cp,
            revision_type,
            stacked,
            &ordered_segments,
            runs,
        );
    }

    for bookmark in bookmarks {
        if bookmark.start_cp >= end_cp {
            break;
        }
        if bookmark.end_cp <= start_cp {
            continue;
        }
        if skip_split_bookmark(bookmark) {
            continue;
        }

        if bookmark.start_cp > start_cp && bookmark.start_cp < end_cp {
            points.insert(bookmark.start_cp);
        }
        if bookmark.end_cp > start_cp && bookmark.end_cp < end_cp {
            points.insert(bookmark.end_cp);
        }
    }

    points.into_iter().collect()
}

fn insert_raw_run_boundary_points(
    points: &mut BTreeSet<u32>,
    start_cp: u32,
    end_cp: u32,
    revision_type: RevisionType,
    stacked: bool,
    segments: &[SourceSegment],
    runs: &[ChpxRun],
) {
    if !raw_run_split_all_enabled() && !stacked {
        return;
    }
    if !raw_run_split_all_enabled() && matches!(presence_split_mode(), PresenceSplitMode::Off) {
        return;
    }
    if !raw_run_split_all_enabled() && !presence_split_kind_matches(revision_type) {
        return;
    }

    // Only honor raw boundaries that coincide with a formatting fingerprint change.
    // LO's XML splits on formatting runs; boundaries caused solely by revision SPRMs
    // should not force a split.
    let mut raw_boundaries = BTreeSet::<u32>::new();
    let mut prev_end: Option<u32> = None;
    let mut prev_fp: Option<u64> = None;
    for run in runs {
        if run.start_cp >= end_cp {
            break;
        }
        if run.end_cp <= start_cp {
            continue;
        }

        let fp = crate::sprm::collect_revision_sprms(run).formatting_fingerprint;
        if let (Some(prev_end), Some(prev_fp)) = (prev_end, prev_fp) {
            if run.start_cp == prev_end && fp != prev_fp {
                if run.start_cp > start_cp && run.start_cp < end_cp {
                    raw_boundaries.insert(run.start_cp);
                }
            }
        }
        prev_end = Some(run.end_cp);
        prev_fp = Some(fp);
    }
    if raw_boundaries.is_empty() {
        return;
    }

    // Writer's SearchNext() works from the node's raw text-attribute/run stream,
    // not our absorbed SourceSegment list. Raw run boundaries are still only a
    // proxy for that stream, but they are materially closer to LO than
    // post-merge redline segments, which can contain overlap artifacts. LO does
    // not apply a "minimum fragment count" gate before honoring those
    // boundaries, so we should not suppress short stacked regions here either.
    if raw_run_split_all_enabled() || redline_has_uniform_formatting(segments) {
        points.extend(raw_boundaries);
        return;
    }

    if let Some(tail_start_cp) = stacked_uniform_tail_start_after_short_format_excursion(segments) {
        points.extend(raw_boundaries.into_iter().filter(|cp| *cp >= tail_start_cp));
    }
}

fn stacked_uniform_tail_start_after_short_format_excursion(
    segments: &[SourceSegment],
) -> Option<u32> {
    if segments.len() < 3 {
        return None;
    }

    for tail_idx in 1..segments.len() {
        let tail_fp = segments[tail_idx].formatting_fingerprint;
        if !segments[tail_idx..]
            .iter()
            .all(|segment| segment.formatting_fingerprint == tail_fp)
        {
            continue;
        }
        if !segments[..tail_idx]
            .iter()
            .any(|segment| segment.formatting_fingerprint != tail_fp)
        {
            continue;
        }

        let lead = &segments[tail_idx - 1];
        let lead_chars = lead.text.chars().count();
        if lead_chars > 3 || alnum_len(&lead.text) > 2 {
            continue;
        }

        return Some(segments[tail_idx].start_cp);
    }

    None
}

fn raw_run_split_all_enabled() -> bool {
    static VALUE: OnceLock<bool> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_SPLIT_RAW_RUNS_ALL")
            .ok()
            .as_deref()
            .is_some_and(|value| value == "1")
    })
}

fn is_field_parenthetical_left_boundary(text: &str) -> bool {
    is_exact_single_char(text, '\u{0003}') || is_exact_single_char(text, '\u{0014}')
}

fn is_field_parenthetical_right_boundary(text: &str) -> bool {
    is_exact_single_char(text, '\u{0008}') || is_exact_single_char(text, '\u{0015}')
}

fn skip_split_bookmark(bookmark: &Bookmark) -> bool {
    (bookmark.name.starts_with("_annotation_mark_")
        && bookmark.end_cp == bookmark.start_cp.saturating_add(1))
        // LO's DOCX export omits Word TOC helper bookmarks (_Toc...) even when
        // they exist in the imported document model, so they should not force
        // a revision split on our side either.
        || bookmark.name.starts_with("_Toc")
}

pub fn extract_text_for_range(segments: &[SourceSegment], start_cp: u32, end_cp: u32) -> String {
    if start_cp >= end_cp {
        return String::new();
    }

    let mut out = String::new();

    for segment in segments {
        let overlap_start = start_cp.max(segment.start_cp);
        let overlap_end = end_cp.min(segment.end_cp);
        if overlap_start >= overlap_end {
            continue;
        }

        let local_start = (overlap_start - segment.start_cp) as usize;
        let local_end = (overlap_end - segment.start_cp) as usize;
        out.push_str(&slice_chars(&segment.text, local_start, local_end));
    }

    out
}

pub fn slice_chars(text: &str, start: usize, end: usize) -> String {
    text.chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

fn is_structural_char(ch: char) -> bool {
    splits_before_structural_char(ch) || splits_after_structural_char(ch)
}

fn splits_before_structural_char(ch: char) -> bool {
    matches!(
        ch,
        '\u{0001}'
            | '\u{0003}'
            | '\u{0006}'
            | '\u{0007}'
            | '\u{0008}'
            | '\u{0013}'
            | '\u{0014}'
            | '\u{0015}'
            | '\u{FFF9}'
    )
}

fn splits_after_structural_char(ch: char) -> bool {
    matches!(
        ch,
        '\r' | '\u{000C}'
            | '\u{0001}'
            | '\u{0003}'
            | '\u{0006}'
            | '\u{0007}'
            | '\u{0008}'
            | '\u{0013}'
            | '\u{0014}'
            | '\u{0015}'
            | '\u{FFF9}'
    )
}

fn is_structural_only(text: &str) -> bool {
    let mut has_chars = false;
    for ch in text.chars() {
        has_chars = true;
        if !is_structural_char(ch) {
            return false;
        }
    }
    has_chars
}

fn is_exact_single_char(text: &str, expected: char) -> bool {
    let mut chars = text.chars();
    chars.next() == Some(expected) && chars.next().is_none()
}

fn split_parenthetical_after_leading_digits(text: &str) -> Option<usize> {
    let mut saw_digit = false;
    for (idx, ch) in text.chars().enumerate() {
        if ch.is_ascii_digit() {
            saw_digit = true;
            continue;
        }
        if saw_digit && ch == '(' {
            return Some(idx);
        }
        break;
    }
    None
}

fn should_split_on_format_change(
    left: &SourceSegment,
    right: &SourceSegment,
    revision_type: RevisionType,
    stacked: bool,
    _total_segments: usize,
    _uniform_formatting: bool,
) -> bool {
    if is_structural_only(&left.text) || is_structural_only(&right.text) {
        return true;
    }

    if left.formatting_fingerprint == right.formatting_fingerprint {
        if insertion_whitespace_bracket_split(left, right, revision_type, stacked) {
            return true;
        }
        if stacked_singleton_source_split(left, right, stacked) {
            return true;
        }
        if timestamp_whitespace_edge_split(left, right, revision_type) {
            return true;
        }
        if sequence_fingerprint_split_enabled()
            && left.formatting_sequence_fingerprint != right.formatting_sequence_fingerprint
            && sequence_fingerprint_split_side_ok(left, right)
        {
            return true;
        }
        return false;
    }

    let left_len = left.text.chars().count() as u32;
    let right_len = right.text.chars().count() as u32;

    if !stacked
        && revision_type == RevisionType::Deletion
        && punctuation_format_noise_boundary(left, right)
    {
        return false;
    }
    if !stacked
        && revision_type == RevisionType::Insertion
        && double_space_sentence_continuation_boundary(left, right)
    {
        return false;
    }
    if !stacked
        && revision_type == RevisionType::Insertion
        && placeholder_clause_continuation_boundary(left, right)
    {
        return false;
    }
    if !stacked
        && revision_type == RevisionType::Deletion
        && bracketed_note_prefix_boundary(left, right)
    {
        return false;
    }

    // When either segment at the boundary is very small, the fingerprint
    // difference is likely CHPX fragmentation noise rather than a meaningful
    // formatting boundary. Skip the split.
    let min_seg = format_split_min_segment();
    if min_seg > 0 && (left_len < min_seg || right_len < min_seg) {
        return false;
    }

    let max_short_run = format_split_max_short_run();
    if max_short_run == 0 {
        return true;
    }

    left_len <= max_short_run || right_len <= max_short_run
}

fn stacked_deletion_singleton_triplet_merge_guard(
    left: &SourceSegment,
    middle: &SourceSegment,
    right: &SourceSegment,
    revision_type: RevisionType,
    stacked: bool,
) -> bool {
    if !stacked || revision_type != RevisionType::Deletion {
        return false;
    }
    if !is_exact_single_alnum(&middle.text) {
        return false;
    }
    if left.segment_timestamp != middle.segment_timestamp
        || middle.segment_timestamp != right.segment_timestamp
    {
        return false;
    }
    let right_starts_ws = right
        .text
        .chars()
        .next()
        .is_some_and(|ch| ch.is_whitespace());
    let left_ends_alnum = left
        .text
        .chars()
        .next_back()
        .is_some_and(|ch| ch.is_ascii_alphanumeric());

    alnum_len(&left.text) >= 8 && alnum_len(&right.text) >= 8 && right_starts_ws && left_ends_alnum
}

fn stacked_singleton_source_split(
    left: &SourceSegment,
    right: &SourceSegment,
    stacked: bool,
) -> bool {
    if !stacked || !stacked_singleton_source_split_enabled() {
        return false;
    }
    if left.source_chpx_id == right.source_chpx_id {
        return false;
    }
    let min_side = stacked_singleton_source_split_min_left_alnum();

    if is_exact_single_alnum(&right.text) {
        return alnum_len(&left.text) >= min_side;
    }
    if boundary_transition(left, right) {
        let left_alnum = alnum_len(&left.text);
        let right_alnum = alnum_len(&right.text);
        let right_starts_whitespace = right
            .text
            .chars()
            .next()
            .is_some_and(|ch| ch.is_whitespace());
        return right_starts_whitespace && left_alnum == min_side && right_alnum > 0;
    }

    false
}

fn stacked_middle_singleton_source_split(
    left: &SourceSegment,
    middle: &SourceSegment,
    right: &SourceSegment,
) -> bool {
    if !stacked_singleton_source_split_enabled() {
        return false;
    }
    if !is_exact_single_alnum(&middle.text) {
        return false;
    }
    if left.source_chpx_id == middle.source_chpx_id || middle.source_chpx_id == right.source_chpx_id
    {
        return false;
    }

    let min_side = stacked_singleton_source_split_min_left_alnum();
    alnum_len(&left.text) >= min_side && alnum_len(&right.text) >= min_side
}

fn insertion_whitespace_bracket_split(
    left: &SourceSegment,
    right: &SourceSegment,
    revision_type: RevisionType,
    stacked: bool,
) -> bool {
    if revision_type != RevisionType::Insertion || stacked {
        return false;
    }
    if left.source_chpx_id == right.source_chpx_id {
        return false;
    }
    if !left.text.chars().all(|ch| ch.is_whitespace()) {
        return false;
    }
    right.text.chars().next() == Some('[')
}

fn insertion_punctuation_space_lowercase_triplet_split(
    left: &SourceSegment,
    middle: &SourceSegment,
    right: &SourceSegment,
    revision_type: RevisionType,
    stacked: bool,
) -> bool {
    if revision_type != RevisionType::Insertion || stacked {
        return false;
    }
    if left.source_chpx_id == middle.source_chpx_id || middle.source_chpx_id == right.source_chpx_id
    {
        return false;
    }
    if middle.segment_timestamp != right.segment_timestamp {
        return false;
    }

    if middle.text != " " {
        return false;
    }
    if alnum_len(&right.text) < 20 {
        return false;
    }

    let left_trimmed = left.text.trim_end();
    let left_sentence_end = left_trimmed
        .chars()
        .next_back()
        .is_some_and(|ch| matches!(ch, '.' | ';' | ':' | '!' | '?'));
    if !left_sentence_end {
        return false;
    }

    right
        .text
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_lowercase())
}

fn timestamp_whitespace_edge_split(
    left: &SourceSegment,
    right: &SourceSegment,
    revision_type: RevisionType,
) -> bool {
    if !timestamp_whitespace_edge_split_enabled() {
        return false;
    }
    if revision_type != RevisionType::Insertion {
        return false;
    }
    if left.source_chpx_id == right.source_chpx_id {
        return false;
    }
    if left.segment_timestamp == right.segment_timestamp {
        return false;
    }
    if !boundary_transition(left, right) {
        return false;
    }

    let max_short_len = timestamp_whitespace_edge_split_max_short_len();
    let left_short_ws = left.text.chars().count() <= max_short_len && alnum_len(&left.text) == 0;
    let right_short_ws = right.text.chars().count() <= max_short_len && alnum_len(&right.text) == 0;
    left_short_ws || right_short_ws
}

fn sequence_fingerprint_split_side_ok(left: &SourceSegment, right: &SourceSegment) -> bool {
    let min_side = sequence_fingerprint_split_min_side_alnum();
    if min_side == 0 {
        return true;
    }

    let left_alnum = alnum_len(&left.text);
    let right_alnum = alnum_len(&right.text);
    left_alnum >= min_side && right_alnum >= min_side
}

fn boundary_transition(left: &SourceSegment, right: &SourceSegment) -> bool {
    match (left.text.chars().next_back(), right.text.chars().next()) {
        (Some(left_edge), Some(right_edge)) => {
            left_edge.is_whitespace() != right_edge.is_whitespace()
                || left_edge.is_ascii_alphanumeric() != right_edge.is_ascii_alphanumeric()
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Copy)]
enum PresenceSplitMode {
    Off,
    Always,
    DttmDelta,
    DttmDeltaOrBoundary,
}

fn presence_split_mode() -> PresenceSplitMode {
    static VALUE: OnceLock<PresenceSplitMode> = OnceLock::new();
    *VALUE.get_or_init(|| {
        match std::env::var("DOC_RL_SOURCE_BOUNDARY_SPLIT_MODE")
            .ok()
            .unwrap_or_else(|| "always".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "off" | "0" | "false" | "no" => PresenceSplitMode::Off,
            "always" => PresenceSplitMode::Always,
            "dttm" | "dttm_delta" => PresenceSplitMode::DttmDelta,
            "dttm_or_boundary" | "boundary" => PresenceSplitMode::DttmDeltaOrBoundary,
            _ => PresenceSplitMode::Off,
        }
    })
}

fn alnum_len(text: &str) -> usize {
    text.chars().filter(|ch| ch.is_ascii_alphanumeric()).count()
}

fn punctuation_format_noise_boundary(left: &SourceSegment, right: &SourceSegment) -> bool {
    let left_len = left.text.chars().count();
    let right_len = right.text.chars().count();
    let (short, long) = if left_len <= right_len {
        (&left.text, &right.text)
    } else {
        (&right.text, &left.text)
    };

    let short_len = short.chars().count();
    if short_len == 0 || short_len > 2 {
        return false;
    }

    let trimmed = short.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed != "," {
        return false;
    }

    alnum_len(long) >= 8
}

fn deletion_comma_clause_format_excursion_split(
    left: &SourceSegment,
    middle: &SourceSegment,
    right: &SourceSegment,
) -> bool {
    if left.segment_timestamp.is_none()
        || middle.segment_timestamp.is_none()
        || right.segment_timestamp.is_none()
    {
        return false;
    }
    if left.segment_timestamp != middle.segment_timestamp
        || middle.segment_timestamp != right.segment_timestamp
    {
        return false;
    }
    if middle.text.trim() != "," {
        return false;
    }
    if !right
        .text
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_lowercase())
    {
        return false;
    }
    if alnum_len(&right.text) < 8 {
        return false;
    }
    if left.formatting_fingerprint != middle.formatting_fingerprint {
        return false;
    }
    if middle.formatting_fingerprint == right.formatting_fingerprint {
        return false;
    }

    true
}

fn double_space_sentence_continuation_boundary(
    left: &SourceSegment,
    right: &SourceSegment,
) -> bool {
    let (Some(left_ts), Some(right_ts)) = (left.segment_timestamp, right.segment_timestamp) else {
        return false;
    };
    if left_ts != right_ts {
        return false;
    }
    if !left.text.ends_with(".  ") {
        return false;
    }
    if alnum_len(&right.text) < 20 {
        return false;
    }
    right
        .text
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_uppercase())
}

fn is_exact_single_alnum(text: &str) -> bool {
    let mut chars = text.chars();
    let Some(ch) = chars.next() else {
        return false;
    };
    ch.is_ascii_alphanumeric() && chars.next().is_none()
}

fn presence_split_kind_matches(kind: RevisionType) -> bool {
    static VALUE: OnceLock<String> = OnceLock::new();
    let mode = VALUE.get_or_init(|| {
        std::env::var("DOC_RL_SOURCE_BOUNDARY_SPLIT_KIND")
            .ok()
            .unwrap_or_else(|| "both".to_string())
            .trim()
            .to_ascii_lowercase()
    });

    match mode.as_str() {
        "both" | "all" => true,
        "deletion" => kind == RevisionType::Deletion,
        _ => kind == RevisionType::Insertion,
    }
}

fn format_split_min_segment() -> u32 {
    static VALUE: OnceLock<u32> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_FORMAT_SPLIT_MIN_SEGMENT")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0)
    })
}

fn format_split_max_short_run() -> u32 {
    static VALUE: OnceLock<u32> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_FORMAT_SPLIT_MAXLEN")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(1000)
    })
}

fn lo_splitter_strict_enabled() -> bool {
    if cfg!(test) {
        return std::env::var("DOC_RL_STRICT_LO_SPLITTER")
            .ok()
            .as_deref()
            .is_some_and(|value| value == "1");
    }
    if std::env::var("DOC_RL_STRICT_LO")
        .ok()
        .as_deref()
        .is_some_and(|value| value == "1")
    {
        return true;
    }
    std::env::var("DOC_RL_STRICT_LO_SPLITTER")
        .ok()
        .as_deref()
        .is_some_and(|value| value == "1")
}

fn field_parenthetical_split_enabled() -> bool {
    static VALUE: OnceLock<bool> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_FIELD_PAREN_SPLIT")
            .ok()
            .as_deref()
            .is_none_or(|value| value != "0")
    })
}

fn sequence_fingerprint_split_enabled() -> bool {
    static VALUE: OnceLock<bool> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_SPLIT_ON_SEQUENCE_FINGERPRINT")
            .ok()
            .as_deref()
            .is_none_or(|value| value != "0")
    })
}

fn sequence_fingerprint_split_min_side_alnum() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_SPLIT_ON_SEQUENCE_FINGERPRINT_MIN_SIDE_ALNUM")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(8)
    })
}

fn stacked_singleton_source_split_enabled() -> bool {
    static VALUE: OnceLock<bool> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_STACKED_SINGLETON_SOURCE_SPLIT")
            .ok()
            .as_deref()
            .is_none_or(|value| value != "0")
    })
}

fn stacked_singleton_source_split_min_left_alnum() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_STACKED_SINGLETON_SOURCE_SPLIT_MIN_LEFT_ALNUM")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4)
    })
}

fn redline_has_uniform_formatting(segments: &[SourceSegment]) -> bool {
    let Some(first) = segments.first() else {
        return false;
    };

    segments.iter().all(|segment| {
        segment.formatting_fingerprint == first.formatting_fingerprint
            && segment.formatting_sequence_fingerprint == first.formatting_sequence_fingerprint
    })
}

fn timestamp_whitespace_edge_split_enabled() -> bool {
    static VALUE: OnceLock<bool> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_TIMESTAMP_WHITESPACE_EDGE_SPLIT")
            .ok()
            .as_deref()
            .is_some_and(|value| value == "1")
    })
}

fn timestamp_whitespace_edge_split_max_short_len() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_TIMESTAMP_WHITESPACE_EDGE_SPLIT_MAX_SHORT_LEN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(2)
    })
}

fn timestamp_oscillation_split_enabled() -> bool {
    static VALUE: OnceLock<bool> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_TIMESTAMP_OSCILLATION_SPLIT")
            .ok()
            .as_deref()
            .is_none_or(|value| value != "0")
    })
}

fn segment_timestamp_span_gt_one_minute(segments: &[SourceSegment]) -> bool {
    let mut min_ts: Option<chrono::NaiveDateTime> = None;
    let mut max_ts: Option<chrono::NaiveDateTime> = None;

    for segment in segments {
        let Some(ts) = segment
            .segment_timestamp
            .and_then(|value| value.to_naive_datetime())
        else {
            continue;
        };

        if min_ts.is_none_or(|existing| ts < existing) {
            min_ts = Some(ts);
        }
        if max_ts.is_none_or(|existing| ts > existing) {
            max_ts = Some(ts);
        }
    }

    match (min_ts, max_ts) {
        (Some(min), Some(max)) => (max - min).num_minutes().abs() > 1,
        _ => false,
    }
}

fn is_timestamp_oscillation_boundary(
    left: &SourceSegment,
    middle: &SourceSegment,
    right: &SourceSegment,
    anchor_ts: Option<Dttm>,
) -> bool {
    let (Some(left_ts), Some(middle_ts), Some(right_ts)) = (
        left.segment_timestamp,
        middle.segment_timestamp,
        right.segment_timestamp,
    ) else {
        return false;
    };
    let Some(anchor_ts) = anchor_ts else {
        return false;
    };
    if left_ts != right_ts || left_ts == middle_ts {
        return false;
    }
    if left_ts.compatible_with(anchor_ts)
        || right_ts.compatible_with(anchor_ts)
        || !middle_ts.compatible_with(anchor_ts)
    {
        return false;
    }

    if middle.text.chars().count() > 2 {
        return false;
    }
    if !middle.text.chars().all(|ch| ch.is_whitespace()) {
        return false;
    }

    if alnum_len(&right.text) == 0 {
        return false;
    }
    if boundary_transition(left, right) {
        return false;
    }

    left.source_chpx_id != middle.source_chpx_id || middle.source_chpx_id != right.source_chpx_id
}

fn insert_timestamp_excursion_block_splits(
    points: &mut BTreeSet<u32>,
    start_cp: u32,
    end_cp: u32,
    segments: &[SourceSegment],
    anchor_ts: Option<Dttm>,
) {
    let Some(anchor_ts) = anchor_ts else {
        return;
    };
    let blocks = contiguous_timestamp_blocks(segments);
    if blocks.len() < 3 {
        return;
    }

    for window in blocks.windows(3) {
        let [left, middle, right] = window else {
            continue;
        };
        let (Some(left_ts), Some(middle_ts), Some(right_ts)) =
            (left.timestamp, middle.timestamp, right.timestamp)
        else {
            continue;
        };
        if left_ts != right_ts || left_ts == middle_ts {
            continue;
        }
        if !left_ts.compatible_with(anchor_ts)
            || !right_ts.compatible_with(anchor_ts)
            || middle_ts.compatible_with(anchor_ts)
        {
            continue;
        }
        if !block_has_alnum(*left, segments)
            || !block_has_alnum(*middle, segments)
            || !block_has_alnum(*right, segments)
        {
            continue;
        }
        if block_alnum_len(*middle, segments) < 8
            || !block_starts_sentence_punctuation(*middle, segments)
        {
            continue;
        }

        let middle_start = segments[middle.start_idx].start_cp;
        let middle_end = segments[middle.end_idx - 1].end_cp;
        if middle_start > start_cp && middle_start < end_cp {
            points.insert(middle_start);
        }
        if middle_end > start_cp && middle_end < end_cp {
            points.insert(middle_end);
        }
    }
}

fn insert_anchor_compat_excursion_block_splits(
    points: &mut BTreeSet<u32>,
    start_cp: u32,
    end_cp: u32,
    segments: &[SourceSegment],
    anchor_ts: Option<Dttm>,
) {
    let Some(anchor_ts) = anchor_ts else {
        return;
    };
    let blocks = contiguous_anchor_compat_blocks(segments, anchor_ts);
    if blocks.len() != 3 {
        return;
    }

    for window in blocks.windows(3) {
        let [left, middle, right] = window else {
            continue;
        };
        if !left.compatible || middle.compatible || !right.compatible {
            continue;
        }
        if !compat_block_all_have_timestamps(*left, segments)
            || !compat_block_all_have_timestamps(*middle, segments)
            || !compat_block_all_have_timestamps(*right, segments)
        {
            continue;
        }
        if !compat_blocks_share_timestamp(*left, *right, segments) {
            continue;
        }

        let left_block = left.as_timestamp_block();
        let middle_block = middle.as_timestamp_block();
        let right_block = right.as_timestamp_block();
        if !block_has_alnum(left_block, segments) || !block_has_alnum(middle_block, segments) {
            continue;
        }
        if block_alnum_len(middle_block, segments) < 8
            || !block_starts_sentence_punctuation(middle_block, segments)
        {
            continue;
        }

        let middle_start = segments[middle.start_idx].start_cp;
        if middle_start > start_cp && middle_start < end_cp {
            points.insert(middle_start);
        }

        if block_has_alnum(right_block, segments) {
            let middle_end = segments[middle.end_idx - 1].end_cp;
            if middle_end > start_cp && middle_end < end_cp {
                points.insert(middle_end);
            }
        }
    }
}

fn insert_anchor_compat_terminal_tail_excursion_split(
    points: &mut BTreeSet<u32>,
    start_cp: u32,
    end_cp: u32,
    segments: &[SourceSegment],
    anchor_ts: Option<Dttm>,
) {
    let Some(anchor_ts) = anchor_ts else {
        return;
    };
    let blocks = contiguous_anchor_compat_blocks(segments, anchor_ts);
    if blocks.len() < 3 {
        return;
    }

    for window in blocks.windows(3) {
        let [left, middle, right] = window else {
            continue;
        };
        if !left.compatible || middle.compatible || !right.compatible {
            continue;
        }
        if !compat_block_all_have_timestamps(*left, segments)
            || !compat_block_all_have_timestamps(*middle, segments)
            || !compat_block_all_have_timestamps(*right, segments)
        {
            continue;
        }

        let left_block = left.as_timestamp_block();
        let middle_block = middle.as_timestamp_block();
        let right_block = right.as_timestamp_block();
        if !block_has_alnum(left_block, segments) || !block_has_alnum(middle_block, segments) {
            continue;
        }
        if block_has_alnum(right_block, segments) {
            continue;
        }
        if block_alnum_len(left_block, segments) < 24 || block_alnum_len(middle_block, segments) < 8
        {
            continue;
        }

        let middle_text = block_text(middle_block, segments);
        if !middle_text
            .chars()
            .next()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        if !first_nonspace_char(&middle_text).is_some_and(|ch| ch.is_ascii_lowercase()) {
            continue;
        }

        let right_text = block_text(right_block, segments);
        let right_trimmed = right_text.trim();
        if right_trimmed.is_empty()
            || !right_trimmed
                .chars()
                .all(|ch| matches!(ch, '.' | '!' | '?' | ';' | ':' | ','))
        {
            continue;
        }

        let middle_start = segments[middle.start_idx].start_cp;
        if middle_start > start_cp && middle_start < end_cp {
            points.insert(middle_start);
        }
    }
}

fn insert_anchor_compat_midword_return_split(
    points: &mut BTreeSet<u32>,
    start_cp: u32,
    end_cp: u32,
    segments: &[SourceSegment],
    anchor_ts: Option<Dttm>,
) {
    let Some(anchor_ts) = anchor_ts else {
        return;
    };
    let blocks = contiguous_anchor_compat_blocks(segments, anchor_ts);
    if blocks.len() < 3 {
        return;
    }

    for window in blocks.windows(3) {
        let [left, middle, right] = window else {
            continue;
        };
        if !left.compatible || middle.compatible || !right.compatible {
            continue;
        }
        if !compat_block_all_have_timestamps(*left, segments)
            || !compat_block_all_have_timestamps(*middle, segments)
            || !compat_block_all_have_timestamps(*right, segments)
            || !compat_blocks_share_timestamp(*left, *right, segments)
        {
            continue;
        }

        let left_block = left.as_timestamp_block();
        let middle_block = middle.as_timestamp_block();
        let right_block = right.as_timestamp_block();
        let left_text = block_text(left_block, segments);
        let middle_text = block_text(middle_block, segments);
        let right_text = block_text(right_block, segments);

        if block_alnum_len(left_block, segments) < 24
            || block_alnum_len(middle_block, segments) == 0
            || block_alnum_len(middle_block, segments) > 2
            || block_alnum_len(right_block, segments) < 12
        {
            continue;
        }

        let Some(left_last) = last_nonspace_char(&left_text) else {
            continue;
        };
        let Some(middle_first) = first_nonspace_char(&middle_text) else {
            continue;
        };
        let Some(right_first) = first_nonspace_char(&right_text) else {
            continue;
        };
        if !left_last.is_ascii_alphabetic()
            || !middle_first.is_ascii_lowercase()
            || !right_first.is_ascii_lowercase()
        {
            continue;
        }

        let middle_trimmed = middle_text.trim();
        if middle_trimmed.is_empty()
            || middle_trimmed.chars().count() > 2
            || !middle_trimmed
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || matches!(ch, '\'' | '’'))
        {
            continue;
        }

        let middle_start = segments[middle.start_idx].start_cp;
        if middle_start > start_cp && middle_start < end_cp {
            points.insert(middle_start);
        }
    }
}

fn insert_anchor_compat_midword_prefix_splits(
    points: &mut BTreeSet<u32>,
    start_cp: u32,
    end_cp: u32,
    segments: &[SourceSegment],
    anchor_ts: Option<Dttm>,
) {
    let Some(anchor_ts) = anchor_ts else {
        return;
    };
    let blocks = contiguous_anchor_compat_blocks(segments, anchor_ts);
    if blocks.len() < 3 {
        return;
    }

    for window in blocks.windows(3) {
        let [left, middle, right] = window else {
            continue;
        };
        if !left.compatible || middle.compatible || !right.compatible {
            continue;
        }
        if !compat_block_all_have_timestamps(*left, segments)
            || !compat_block_all_have_timestamps(*middle, segments)
            || !compat_block_all_have_timestamps(*right, segments)
            || !compat_blocks_share_timestamp(*left, *right, segments)
        {
            continue;
        }

        let left_block = left.as_timestamp_block();
        let middle_block = middle.as_timestamp_block();
        let right_block = right.as_timestamp_block();
        let left_text = block_text(left_block, segments);
        let middle_text = block_text(middle_block, segments);
        let right_text = block_text(right_block, segments);
        let Some(left_first) = first_nonspace_char(&left_text) else {
            continue;
        };
        let Some(left_last) = last_nonspace_char(&left_text) else {
            continue;
        };
        let Some(middle_first) = first_nonspace_char(&middle_text) else {
            continue;
        };
        let Some(right_first) = first_nonspace_char(&right_text) else {
            continue;
        };

        if !left_text
            .chars()
            .next()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        if !left_first.is_ascii_uppercase()
            || !left_last.is_ascii_alphabetic()
            || !middle_first.is_ascii_lowercase()
            || !right_first.is_ascii_lowercase()
        {
            continue;
        }
        if block_alnum_len(left_block, segments) == 0 || block_alnum_len(left_block, segments) > 4 {
            continue;
        }
        // LO splits when a short uppercase-led prefix is followed by a
        // multi-segment lowercase excursion, but keeps single-run continuations
        // attached. Require the lowercase midword excursion to span multiple
        // source segments.
        if middle.end_idx.saturating_sub(middle.start_idx) < 2 {
            continue;
        }
        if block_alnum_len(middle_block, segments) < 8
            || block_alnum_len(right_block, segments) < 12
        {
            continue;
        }

        let middle_start = segments[middle.start_idx].start_cp;
        if middle_start > start_cp && middle_start < end_cp {
            points.insert(middle_start);
        }
    }
}

fn insert_anchor_incompatible_segment_splits(
    points: &mut BTreeSet<u32>,
    start_cp: u32,
    end_cp: u32,
    segments: &[SourceSegment],
    anchor_ts: Option<Dttm>,
) {
    let Some(anchor_ts) = anchor_ts else {
        return;
    };

    let min_alnum = anchor_incompat_min_alnum();
    let mut suffix_start: Option<usize> = None;
    for (idx, segment) in segments.iter().enumerate().rev() {
        let Some(seg_ts) = segment.segment_timestamp else {
            continue;
        };
        if timestamps_compatible(Some(anchor_ts), Some(seg_ts)) {
            break;
        }
        suffix_start = Some(idx);
    }

    let Some(start_idx) = suffix_start else {
        return;
    };

    for segment in &segments[start_idx..] {
        if segment.start_cp <= start_cp || segment.start_cp >= end_cp {
            continue;
        }
        if alnum_len(&segment.text) < min_alnum {
            continue;
        }
        points.insert(segment.start_cp);
        break;
    }
}

fn anchor_incompat_min_alnum() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("DOC_RL_ANCHOR_INCOMPAT_MIN_ALNUM")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4)
    })
}

fn insert_anchor_compat_word_boundary_excursion_splits(
    points: &mut BTreeSet<u32>,
    start_cp: u32,
    end_cp: u32,
    segments: &[SourceSegment],
    anchor_ts: Option<Dttm>,
) {
    let Some(anchor_ts) = anchor_ts else {
        return;
    };
    let blocks = contiguous_anchor_compat_blocks(segments, anchor_ts);
    if blocks.len() < 3 {
        return;
    }

    for window in blocks.windows(3) {
        let [left, middle, right] = window else {
            continue;
        };
        if !left.compatible || middle.compatible || !right.compatible {
            continue;
        }
        if !compat_block_all_have_timestamps(*left, segments)
            || !compat_block_all_have_timestamps(*middle, segments)
            || !compat_block_all_have_timestamps(*right, segments)
        {
            continue;
        }

        let left_block = left.as_timestamp_block();
        let middle_block = middle.as_timestamp_block();
        let right_block = right.as_timestamp_block();
        let left_text = block_text(left_block, segments);
        let middle_text = block_text(middle_block, segments);
        let right_text = block_text(right_block, segments);
        let Some(middle_first) = first_nonspace_char(&middle_text) else {
            continue;
        };

        if !left_text
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        if !middle_first.is_ascii_lowercase() {
            continue;
        }
        if block_alnum_len(left_block, segments) < 8 || block_alnum_len(middle_block, segments) < 8
        {
            continue;
        }
        if !right_text.chars().all(|ch| ch.is_whitespace()) {
            continue;
        }
        if right.end_idx != segments.len() {
            continue;
        }

        let middle_start = segments[middle.start_idx].start_cp;
        if middle_start > start_cp && middle_start < end_cp {
            points.insert(middle_start);
        }
    }
}

fn insert_anchor_compat_lowercase_clause_excursion_split(
    points: &mut BTreeSet<u32>,
    start_cp: u32,
    end_cp: u32,
    segments: &[SourceSegment],
    anchor_ts: Option<Dttm>,
) {
    let Some(anchor_ts) = anchor_ts else {
        return;
    };
    let blocks = contiguous_anchor_compat_blocks(segments, anchor_ts);
    if blocks.len() < 3 {
        return;
    }

    for window in blocks.windows(3) {
        let [left, middle, right] = window else {
            continue;
        };
        if !left.compatible || middle.compatible || !right.compatible {
            continue;
        }
        if !compat_block_all_have_timestamps(*left, segments)
            || !compat_block_all_have_timestamps(*middle, segments)
            || !compat_block_all_have_timestamps(*right, segments)
            || !compat_blocks_share_timestamp(*left, *right, segments)
        {
            continue;
        }
        // LO splits when a multi-run excursion block with alternating
        // timestamps returns to the prior timestamp class. Single-timestamp
        // blips stay inside one insertion, so do not split those here.
        if compat_block_distinct_timestamp_count(*middle, segments) < 2 {
            continue;
        }

        let left_block = left.as_timestamp_block();
        let middle_block = middle.as_timestamp_block();
        let right_block = right.as_timestamp_block();
        let left_text = block_text(left_block, segments);
        let middle_text = block_text(middle_block, segments);
        let right_text = block_text(right_block, segments);
        let Some(left_last) = last_nonspace_char(&left_text) else {
            continue;
        };
        let Some(middle_first) = first_nonspace_char(&middle_text) else {
            continue;
        };
        let Some(right_first) = first_nonspace_char(&right_text) else {
            continue;
        };

        if !left_text
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_whitespace())
        {
            continue;
        }
        if !left_last.is_ascii_alphabetic()
            || !middle_first.is_ascii_lowercase()
            || !right_first.is_ascii_lowercase()
        {
            continue;
        }
        if block_alnum_len(left_block, segments) < 12
            || block_alnum_len(middle_block, segments) < 12
            || block_alnum_len(right_block, segments) < 12
        {
            continue;
        }

        let middle_start = segments[middle.start_idx].start_cp;
        if middle_start > start_cp && middle_start < end_cp {
            points.insert(middle_start);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TimestampBlock {
    start_idx: usize,
    end_idx: usize,
    timestamp: Option<Dttm>,
}

#[derive(Debug, Clone, Copy)]
struct AnchorCompatBlock {
    start_idx: usize,
    end_idx: usize,
    compatible: bool,
}

impl AnchorCompatBlock {
    fn as_timestamp_block(self) -> TimestampBlock {
        TimestampBlock {
            start_idx: self.start_idx,
            end_idx: self.end_idx,
            timestamp: None,
        }
    }
}

fn contiguous_timestamp_blocks(segments: &[SourceSegment]) -> Vec<TimestampBlock> {
    let mut out = Vec::new();
    let Some(first) = segments.first() else {
        return out;
    };

    let mut start_idx = 0usize;
    let mut current_ts = first.segment_timestamp;
    for idx in 1..segments.len() {
        if segments[idx].segment_timestamp == current_ts {
            continue;
        }

        out.push(TimestampBlock {
            start_idx,
            end_idx: idx,
            timestamp: current_ts,
        });
        start_idx = idx;
        current_ts = segments[idx].segment_timestamp;
    }

    out.push(TimestampBlock {
        start_idx,
        end_idx: segments.len(),
        timestamp: current_ts,
    });

    out
}

fn contiguous_anchor_compat_blocks(
    segments: &[SourceSegment],
    anchor_ts: Dttm,
) -> Vec<AnchorCompatBlock> {
    let mut out = Vec::new();
    let Some(first) = segments.first() else {
        return out;
    };

    let mut start_idx = 0usize;
    let mut current_compatible = first
        .segment_timestamp
        .is_some_and(|timestamp| timestamp.compatible_with(anchor_ts));
    for idx in 1..segments.len() {
        let compatible = segments[idx]
            .segment_timestamp
            .is_some_and(|timestamp| timestamp.compatible_with(anchor_ts));
        if compatible == current_compatible {
            continue;
        }

        out.push(AnchorCompatBlock {
            start_idx,
            end_idx: idx,
            compatible: current_compatible,
        });
        start_idx = idx;
        current_compatible = compatible;
    }

    out.push(AnchorCompatBlock {
        start_idx,
        end_idx: segments.len(),
        compatible: current_compatible,
    });

    out
}

fn compat_block_all_have_timestamps(block: AnchorCompatBlock, segments: &[SourceSegment]) -> bool {
    segments[block.start_idx..block.end_idx]
        .iter()
        .all(|segment| segment.segment_timestamp.is_some())
}

fn compat_blocks_share_timestamp(
    left: AnchorCompatBlock,
    right: AnchorCompatBlock,
    segments: &[SourceSegment],
) -> bool {
    segments[left.start_idx..left.end_idx]
        .iter()
        .any(|left_segment| {
            let Some(left_ts) = left_segment.segment_timestamp else {
                return false;
            };
            segments[right.start_idx..right.end_idx]
                .iter()
                .any(|right_segment| right_segment.segment_timestamp == Some(left_ts))
        })
}

fn compat_block_distinct_timestamp_count(
    block: AnchorCompatBlock,
    segments: &[SourceSegment],
) -> usize {
    let mut seen = Vec::<Dttm>::new();
    for segment in &segments[block.start_idx..block.end_idx] {
        if let Some(ts) = segment.segment_timestamp
            && !seen.contains(&ts)
        {
            seen.push(ts);
        }
    }
    seen.len()
}

fn block_has_alnum(block: TimestampBlock, segments: &[SourceSegment]) -> bool {
    segments[block.start_idx..block.end_idx]
        .iter()
        .any(|segment| alnum_len(&segment.text) > 0)
}

fn block_alnum_len(block: TimestampBlock, segments: &[SourceSegment]) -> usize {
    segments[block.start_idx..block.end_idx]
        .iter()
        .map(|segment| alnum_len(&segment.text))
        .sum()
}

fn block_starts_sentence_punctuation(block: TimestampBlock, segments: &[SourceSegment]) -> bool {
    for segment in &segments[block.start_idx..block.end_idx] {
        for ch in segment.text.chars() {
            if ch.is_whitespace() {
                continue;
            }
            return matches!(ch, '.' | '!' | '?' | ';' | ':');
        }
    }

    false
}

fn block_text(block: TimestampBlock, segments: &[SourceSegment]) -> String {
    segments[block.start_idx..block.end_idx]
        .iter()
        .map(|segment| segment.text.as_str())
        .collect()
}

fn first_nonspace_char(text: &str) -> Option<char> {
    text.chars().find(|ch| !ch.is_whitespace())
}

fn last_nonspace_char(text: &str) -> Option<char> {
    text.chars().rev().find(|ch| !ch.is_whitespace())
}

fn segment_min_timestamp(segments: &[SourceSegment]) -> Option<Dttm> {
    let mut min_ts: Option<Dttm> = None;

    for segment in segments {
        let Some(segment_ts) = segment.segment_timestamp else {
            continue;
        };
        min_ts = Some(match min_ts {
            Some(existing) => existing.min(segment_ts),
            None => segment_ts,
        });
    }

    min_ts
}

fn insertion_timestamp_return_midword_split(
    left: &SourceSegment,
    middle: &SourceSegment,
    right: &SourceSegment,
    revision_type: RevisionType,
    stacked: bool,
) -> bool {
    if revision_type != RevisionType::Insertion || stacked {
        return false;
    }
    if left.source_chpx_id == middle.source_chpx_id || middle.source_chpx_id == right.source_chpx_id
    {
        return false;
    }
    if left.segment_timestamp.is_none()
        || middle.segment_timestamp.is_none()
        || right.segment_timestamp.is_none()
    {
        return false;
    }
    if left.segment_timestamp != right.segment_timestamp
        || left.segment_timestamp == middle.segment_timestamp
    {
        return false;
    }
    if !left.text.chars().all(|ch| ch.is_whitespace()) || left.text.chars().count() > 32 {
        return false;
    }
    if alnum_len(&middle.text) < 7 || alnum_len(&right.text) < 12 {
        return false;
    }
    if boundary_transition(middle, right) {
        return false;
    }

    let middle_edge = middle.text.chars().next_back();
    let right_edge = right.text.chars().next();
    matches!(middle_edge, Some(ch) if ch.is_ascii_lowercase())
        && matches!(right_edge, Some(ch) if ch.is_ascii_lowercase())
}

fn insertion_timestamp_return_sentence_split(
    left: &SourceSegment,
    middle: &SourceSegment,
    right: &SourceSegment,
    revision_type: RevisionType,
    stacked: bool,
) -> bool {
    if revision_type != RevisionType::Insertion || stacked {
        return false;
    }
    if left.source_chpx_id == middle.source_chpx_id || middle.source_chpx_id == right.source_chpx_id
    {
        return false;
    }
    let (Some(left_ts), Some(middle_ts), Some(right_ts)) = (
        left.segment_timestamp,
        middle.segment_timestamp,
        right.segment_timestamp,
    ) else {
        return false;
    };
    if left_ts != right_ts || left_ts == middle_ts {
        return false;
    }

    let left_trimmed = left.text.trim_end();
    if !left_trimmed
        .chars()
        .next_back()
        .is_some_and(|ch| matches!(ch, '.' | '!' | '?' | ';' | ':'))
    {
        return false;
    }

    let Some(middle_first) = first_nonspace_char(&middle.text) else {
        return false;
    };
    let Some(middle_last) = last_nonspace_char(&middle.text) else {
        return false;
    };
    let Some(right_first) = first_nonspace_char(&right.text) else {
        return false;
    };
    if !middle_first.is_ascii_uppercase()
        || !middle_last.is_ascii_lowercase()
        || !right_first.is_ascii_lowercase()
    {
        return false;
    }
    if alnum_len(&middle.text) < 7 || alnum_len(&right.text) < 12 {
        return false;
    }
    if boundary_transition(middle, right) {
        return false;
    }

    true
}

fn placeholder_clause_continuation_boundary(left: &SourceSegment, right: &SourceSegment) -> bool {
    if left.segment_timestamp != right.segment_timestamp || left.segment_timestamp.is_none() {
        return false;
    }

    let left_trimmed = left.text.trim_end();
    let right_trimmed = right.text.trim_start();
    if !left_trimmed.ends_with(']') || !right_trimmed.starts_with(',') {
        return false;
    }

    right_trimmed.contains('[')
}

fn bracketed_note_prefix_boundary(left: &SourceSegment, right: &SourceSegment) -> bool {
    if left.segment_timestamp != right.segment_timestamp || left.segment_timestamp.is_none() {
        return false;
    }
    if left.text.chars().count() > 12 || alnum_len(&left.text) > 8 {
        return false;
    }

    let left_trimmed = left.text.trim_end();
    if !left_trimmed.starts_with('[') || !left_trimmed.contains(':') {
        return false;
    }
    if !left
        .text
        .chars()
        .next_back()
        .is_some_and(|ch| ch.is_whitespace())
    {
        return false;
    }
    if alnum_len(&right.text) < 20 {
        return false;
    }

    right
        .text
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_lowercase())
}

#[derive(Debug, Clone)]
pub struct DocumentTextIndex {
    base_cp: u32,
    chars: Vec<char>,
}

impl DocumentTextIndex {
    pub fn from_runs(runs: &[ChpxRun]) -> Self {
        if runs.is_empty() {
            return Self {
                base_cp: 0,
                chars: Vec::new(),
            };
        }

        let mut sorted = runs.to_vec();
        sorted.sort_by_key(|run| (run.start_cp, run.end_cp));
        let base_cp = sorted.first().map(|run| run.start_cp).unwrap_or(0);

        let mut chars = Vec::<char>::new();

        for run in sorted {
            let start_offset = run.start_cp.saturating_sub(base_cp) as usize;
            if chars.len() < start_offset {
                chars.resize(start_offset, ' ');
            }

            for (idx, ch) in run.text.chars().enumerate() {
                let absolute = start_offset + idx;
                if absolute < chars.len() {
                    chars[absolute] = ch;
                } else {
                    chars.push(ch);
                }
            }

            let expected_end = run.end_cp.saturating_sub(base_cp) as usize;
            if chars.len() < expected_end {
                chars.resize(expected_end, ' ');
            }
        }

        Self { base_cp, chars }
    }

    pub fn paragraph_index_at(&self, cp: u32) -> u32 {
        let offset = self.cp_offset(cp);
        self.chars
            .iter()
            .take(offset)
            .filter(|&&ch| ch == '\r')
            .count() as u32
    }

    pub fn char_offset_at(&self, cp: u32) -> u32 {
        let offset = self.cp_offset(cp);
        let prefix = &self.chars[..offset.min(self.chars.len())];
        let since_paragraph = prefix
            .iter()
            .rposition(|&ch| ch == '\r')
            .map(|idx| prefix.len().saturating_sub(idx + 1))
            .unwrap_or(prefix.len());
        since_paragraph as u32
    }

    pub fn context(&self, start_cp: u32, end_cp: u32, radius: u32) -> String {
        let start = self.cp_offset(start_cp).saturating_sub(radius as usize);
        let end = (self.cp_offset(end_cp) + radius as usize).min(self.chars.len());
        self.chars[start..end].iter().collect()
    }

    pub fn text_for_range(&self, start_cp: u32, end_cp: u32) -> String {
        if start_cp >= end_cp {
            return String::new();
        }

        let start = self.cp_offset(start_cp).min(self.chars.len());
        let end = self.cp_offset(end_cp).min(self.chars.len());
        if start >= end {
            return String::new();
        }

        self.chars[start..end].iter().collect()
    }

    pub fn max_cp(&self) -> u32 {
        self.base_cp.saturating_add(self.chars.len() as u32)
    }

    fn cp_offset(&self, cp: u32) -> usize {
        if cp <= self.base_cp {
            return 0;
        }
        (cp - self.base_cp).min(self.chars.len() as u32) as usize
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::dttm::Dttm;
    use crate::model::{Bookmark, ChpxRun, RevisionType, SourceSegment};

    use super::{DocumentTextIndex, extract_text_for_range, split_points_for_redline};

    fn pack_dttm(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> u32 {
        let year_bits = ((year - 1900) as u32) << 20;
        let month_bits = month << 16;
        let day_bits = day << 11;
        let hour_bits = hour << 6;
        let minute_bits = minute;
        year_bits | month_bits | day_bits | hour_bits | minute_bits
    }

    #[test]
    fn split_points_include_structural_bookmark_and_segment_boundaries() {
        let segments = vec![SourceSegment {
            start_cp: 0,
            end_cp: 8,
            text: format!("ab{}\rdefg", '\u{0003}'),
            formatting_fingerprint: 1,
            formatting_sequence_fingerprint: 1,
            source_chpx_id: Some(1),
            segment_author_index: None,
            segment_timestamp: None,
        }];
        let bookmarks = vec![Bookmark {
            name: "_cp_change_42".to_string(),
            start_cp: 4,
            end_cp: 6,
        }];

        let points = split_points_for_redline(
            0,
            8,
            RevisionType::Insertion,
            false,
            &segments,
            &[],
            &bookmarks,
        );
        assert_eq!(points, vec![0, 2, 3, 4, 6, 8]);
    }

    #[test]
    fn split_points_include_hlk_bookmark_boundaries() {
        let segments = vec![SourceSegment {
            start_cp: 0,
            end_cp: 10,
            text: "abcdefghij".to_string(),
            formatting_fingerprint: 1,
            formatting_sequence_fingerprint: 1,
            source_chpx_id: Some(1),
            segment_author_index: None,
            segment_timestamp: None,
        }];
        let bookmarks = vec![Bookmark {
            name: "_Hlk123".to_string(),
            start_cp: 4,
            end_cp: 7,
        }];

        let points = split_points_for_redline(
            0,
            10,
            RevisionType::Insertion,
            false,
            &segments,
            &[],
            &bookmarks,
        );
        assert_eq!(points, vec![0, 4, 7, 10]);
    }

    #[test]
    fn split_points_include_generic_bookmark_boundaries() {
        let segments = vec![SourceSegment {
            start_cp: 0,
            end_cp: 10,
            text: "abcdefghij".to_string(),
            formatting_fingerprint: 1,
            formatting_sequence_fingerprint: 1,
            source_chpx_id: Some(1),
            segment_author_index: None,
            segment_timestamp: None,
        }];
        let bookmarks = vec![Bookmark {
            name: "CustomerBookmark".to_string(),
            start_cp: 3,
            end_cp: 8,
        }];

        let points = split_points_for_redline(
            0,
            10,
            RevisionType::Insertion,
            false,
            &segments,
            &[],
            &bookmarks,
        );
        assert_eq!(points, vec![0, 3, 8, 10]);
    }

    #[test]
    fn split_points_ignore_single_char_annotation_marks() {
        let segments = vec![SourceSegment {
            start_cp: 0,
            end_cp: 10,
            text: "abcdefghij".to_string(),
            formatting_fingerprint: 1,
            formatting_sequence_fingerprint: 1,
            source_chpx_id: Some(1),
            segment_author_index: None,
            segment_timestamp: None,
        }];
        let bookmarks = vec![Bookmark {
            name: "_annotation_mark_4".to_string(),
            start_cp: 4,
            end_cp: 5,
        }];

        let points = split_points_for_redline(
            0,
            10,
            RevisionType::Insertion,
            false,
            &segments,
            &[],
            &bookmarks,
        );
        assert_eq!(points, vec![0, 10]);
    }

    #[test]
    fn split_points_ignore_toc_bookmarks() {
        let segments = vec![SourceSegment {
            start_cp: 0,
            end_cp: 10,
            text: "abcdefghij".to_string(),
            formatting_fingerprint: 1,
            formatting_sequence_fingerprint: 1,
            source_chpx_id: Some(1),
            segment_author_index: None,
            segment_timestamp: None,
        }];
        let bookmarks = vec![Bookmark {
            name: "_Toc71077754".to_string(),
            start_cp: 3,
            end_cp: 8,
        }];

        let points = split_points_for_redline(
            0,
            10,
            RevisionType::Deletion,
            false,
            &segments,
            &[],
            &bookmarks,
        );
        assert_eq!(points, vec![0, 10]);
    }

    #[test]
    fn stacked_uniform_fragmented_source_boundaries_split_by_default() {
        let segments: Vec<SourceSegment> = (0..8)
            .map(|idx| SourceSegment {
                start_cp: idx * 2,
                end_cp: idx * 2 + 2,
                text: "ab".to_string(),
                formatting_fingerprint: 55,
                formatting_sequence_fingerprint: 55,
                source_chpx_id: Some(idx + 1),
                segment_author_index: None,
                segment_timestamp: None,
            })
            .collect();
        let runs: Vec<ChpxRun> = (0..8)
            .map(|idx| ChpxRun {
                start_cp: idx * 2,
                end_cp: idx * 2 + 2,
                text: "ab".to_string(),
                sprms: vec![],
                source_chpx_id: Some(idx + 1),
            })
            .collect();

        let points =
            split_points_for_redline(0, 16, RevisionType::Insertion, true, &segments, &runs, &[]);
        assert_eq!(points, vec![0, 2, 4, 6, 8, 10, 12, 14, 16]);
    }

    #[test]
    fn stacked_short_uniform_raw_runs_split_without_min_fragment_gate() {
        let segments: Vec<SourceSegment> = (0..5)
            .map(|idx| SourceSegment {
                start_cp: idx * 2,
                end_cp: idx * 2 + 2,
                text: "ab".to_string(),
                formatting_fingerprint: 55,
                formatting_sequence_fingerprint: 55,
                source_chpx_id: Some(idx + 1),
                segment_author_index: None,
                segment_timestamp: None,
            })
            .collect();
        let runs: Vec<ChpxRun> = (0..5)
            .map(|idx| ChpxRun {
                start_cp: idx * 2,
                end_cp: idx * 2 + 2,
                text: "ab".to_string(),
                sprms: vec![],
                source_chpx_id: Some(idx + 1),
            })
            .collect();

        let points =
            split_points_for_redline(0, 10, RevisionType::Insertion, true, &segments, &runs, &[]);
        assert_eq!(points, vec![0, 2, 4, 6, 8, 10]);
    }

    #[test]
    fn stacked_uniform_raw_runs_split_even_when_absorbed_segments_are_coarse() {
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 8,
                text: "abcdefgh".to_string(),
                formatting_fingerprint: 55,
                formatting_sequence_fingerprint: 55,
                source_chpx_id: Some(100),
                segment_author_index: None,
                segment_timestamp: None,
            },
            SourceSegment {
                start_cp: 8,
                end_cp: 16,
                text: "ijklmnop".to_string(),
                formatting_fingerprint: 55,
                formatting_sequence_fingerprint: 55,
                source_chpx_id: Some(200),
                segment_author_index: None,
                segment_timestamp: None,
            },
        ];
        let runs: Vec<ChpxRun> = (0..8)
            .map(|idx| ChpxRun {
                start_cp: idx * 2,
                end_cp: idx * 2 + 2,
                text: "ab".to_string(),
                sprms: vec![],
                source_chpx_id: Some(idx + 1),
            })
            .collect();

        let points =
            split_points_for_redline(0, 16, RevisionType::Insertion, true, &segments, &runs, &[]);
        assert_eq!(points, vec![0, 2, 4, 6, 8, 10, 12, 14, 16]);
    }

    #[test]
    fn stacked_nonuniform_tail_raw_runs_split_after_short_format_excursion() {
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 2,
                text: "AA".to_string(),
                formatting_fingerprint: 11,
                formatting_sequence_fingerprint: 11,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: None,
            },
            SourceSegment {
                start_cp: 2,
                end_cp: 4,
                text: "th".to_string(),
                formatting_fingerprint: 22,
                formatting_sequence_fingerprint: 22,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: None,
            },
            SourceSegment {
                start_cp: 4,
                end_cp: 6,
                text: ") ".to_string(),
                formatting_fingerprint: 11,
                formatting_sequence_fingerprint: 11,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: None,
            },
            SourceSegment {
                start_cp: 6,
                end_cp: 8,
                text: "bb".to_string(),
                formatting_fingerprint: 11,
                formatting_sequence_fingerprint: 11,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: None,
            },
            SourceSegment {
                start_cp: 8,
                end_cp: 10,
                text: "cc".to_string(),
                formatting_fingerprint: 11,
                formatting_sequence_fingerprint: 11,
                source_chpx_id: Some(5),
                segment_author_index: None,
                segment_timestamp: None,
            },
        ];
        let runs: Vec<ChpxRun> = (0..5)
            .map(|idx| ChpxRun {
                start_cp: idx * 2,
                end_cp: idx * 2 + 2,
                text: "ab".to_string(),
                sprms: vec![],
                source_chpx_id: Some(idx + 1),
            })
            .collect();

        let points =
            split_points_for_redline(0, 10, RevisionType::Insertion, true, &segments, &runs, &[]);
        assert_eq!(points, vec![0, 2, 4, 6, 8, 10]);
    }

    #[test]
    fn extracts_text_for_cp_range() {
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 4,
                text: "abcd".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: None,
            },
            SourceSegment {
                start_cp: 4,
                end_cp: 7,
                text: "XYZ".to_string(),
                formatting_fingerprint: 2,
                formatting_sequence_fingerprint: 2,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: None,
            },
        ];

        assert_eq!(extract_text_for_range(&segments, 2, 6), "cdXY");
    }

    #[test]
    fn document_index_tracks_paragraph_and_offsets() {
        let runs = vec![ChpxRun {
            start_cp: 0,
            end_cp: 8,
            text: "ab\rcdefg".to_string(),
            sprms: vec![],
            source_chpx_id: None,
        }];
        let index = DocumentTextIndex::from_runs(&runs);

        assert_eq!(index.paragraph_index_at(0), 0);
        assert_eq!(index.paragraph_index_at(3), 1);
        assert_eq!(index.char_offset_at(3), 0);
        assert_eq!(index.char_offset_at(6), 3);
    }

    #[test]
    fn split_parenthetical_detector_handles_numbered_payloads() {
        assert_eq!(
            super::split_parenthetical_after_leading_digits("25(a)"),
            Some(2)
        );
        assert_eq!(
            super::split_parenthetical_after_leading_digits("123(abc)"),
            Some(3)
        );
        assert_eq!(super::split_parenthetical_after_leading_digits("(a)"), None);
        assert_eq!(
            super::split_parenthetical_after_leading_digits("A(1)"),
            None
        );
    }

    #[test]
    fn field_parenthetical_split_supports_lo_internal_field_chars() {
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 1,
                text: "\u{0003}".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: None,
            },
            SourceSegment {
                start_cp: 1,
                end_cp: 6,
                text: "25(a)".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: None,
            },
            SourceSegment {
                start_cp: 6,
                end_cp: 7,
                text: "\u{0008}".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: None,
            },
        ];

        let points =
            split_points_for_redline(0, 7, RevisionType::Deletion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 1, 3, 6, 7]);
    }

    #[test]
    fn insertion_timestamp_return_midword_splits() {
        let ts_2049 = Dttm::from_raw(pack_dttm(2018, 6, 17, 20, 49));
        let ts_2050 = Dttm::from_raw(pack_dttm(2018, 6, 17, 20, 50));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 8,
                text: "        ".to_string(),
                formatting_fingerprint: 42,
                formatting_sequence_fingerprint: 42,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_2050,
            },
            SourceSegment {
                start_cp: 8,
                end_cp: 24,
                text: "of charge through".to_string(),
                formatting_fingerprint: 42,
                formatting_sequence_fingerprint: 42,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_2049,
            },
            SourceSegment {
                start_cp: 24,
                end_cp: 64,
                text: "out the term of the lease and renewal".to_string(),
                formatting_fingerprint: 42,
                formatting_sequence_fingerprint: 42,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_2050,
            },
        ];

        let points =
            split_points_for_redline(0, 64, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 24, 64]);
    }

    #[test]
    fn insertion_timestamp_return_sentence_split_keeps_later_sentence_head() {
        let ts_1402 = Dttm::from_raw(pack_dttm(2025, 6, 25, 14, 2));
        let ts_1403 = Dttm::from_raw(pack_dttm(2025, 6, 25, 14, 3));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 2,
                text: ". ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1402,
            },
            SourceSegment {
                start_cp: 2,
                end_cp: 11,
                text: "On-site p".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1403,
            },
            SourceSegment {
                start_cp: 11,
                end_cp: 33,
                text: "ersonnel includes one".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1402,
            },
        ];

        assert_ne!(segments[0].source_chpx_id, segments[1].source_chpx_id);
        assert_ne!(segments[1].source_chpx_id, segments[2].source_chpx_id);
        assert_eq!(segments[0].segment_timestamp, segments[2].segment_timestamp);
        assert_ne!(segments[0].segment_timestamp, segments[1].segment_timestamp);
        assert_eq!(segments[0].text.trim_end(), ".");
        assert_eq!(super::first_nonspace_char(&segments[1].text), Some('O'));
        assert_eq!(super::last_nonspace_char(&segments[1].text), Some('p'));
        assert_eq!(super::first_nonspace_char(&segments[2].text), Some('e'));
        assert_eq!(super::alnum_len(&segments[1].text), 7);
        assert_eq!(super::alnum_len(&segments[2].text), 19);
        assert!(!super::boundary_transition(&segments[1], &segments[2]));
        assert!(super::insertion_timestamp_return_sentence_split(
            &segments[0],
            &segments[1],
            &segments[2],
            RevisionType::Insertion,
            false,
        ));

        let points =
            split_points_for_redline(0, 33, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 2, 33]);
    }

    #[test]
    fn anchor_compat_midword_prefix_split_skips_single_run_excursion() {
        let ts_0752 = Dttm::from_raw(pack_dttm(2025, 10, 22, 7, 52));
        let ts_0753 = Dttm::from_raw(pack_dttm(2025, 10, 22, 7, 53));
        let ts_0754 = Dttm::from_raw(pack_dttm(2025, 10, 22, 7, 54));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 1,
                text: " ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_0752,
            },
            SourceSegment {
                start_cp: 1,
                end_cp: 4,
                text: "Not".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_0753,
            },
            SourceSegment {
                start_cp: 4,
                end_cp: 17,
                text: "withstanding".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_0754,
            },
            SourceSegment {
                start_cp: 17,
                end_cp: 43,
                text: " the foregoing and beyond".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: ts_0753,
            },
        ];

        let points =
            split_points_for_redline(0, 43, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 43]);
    }

    #[test]
    fn placeholder_clause_continuation_does_not_split_inside_clause() {
        let ts = Dttm::from_raw(pack_dttm(2025, 10, 22, 7, 54));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 18,
                text: "[TO BE DISCUSSED]".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts,
            },
            SourceSegment {
                start_cp: 18,
                end_cp: 56,
                text: ", then [PENALTIES FOR LATE DELIVERY]".to_string(),
                formatting_fingerprint: 2,
                formatting_sequence_fingerprint: 2,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts,
            },
        ];

        let points =
            split_points_for_redline(0, 56, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 56]);
    }

    #[test]
    fn anchor_compat_word_boundary_excursion_splits_substantive_lowercase_block() {
        let ts_1511 = Dttm::from_raw(pack_dttm(2020, 8, 13, 15, 11));
        let ts_1512 = Dttm::from_raw(pack_dttm(2020, 8, 13, 15, 12));
        let ts_1513 = Dttm::from_raw(pack_dttm(2020, 8, 13, 15, 13));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 20,
                text: "Lease terms will ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1511,
            },
            SourceSegment {
                start_cp: 20,
                end_cp: 48,
                text: "be updated to reflect suite".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1513,
            },
            SourceSegment {
                start_cp: 48,
                end_cp: 50,
                text: "  ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1512,
            },
        ];

        let points =
            split_points_for_redline(0, 50, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 20, 50]);
    }

    #[test]
    fn anchor_compat_lowercase_clause_excursion_split_inserts_middle_start() {
        let ts_1940 = Dttm::from_raw(pack_dttm(2025, 7, 8, 19, 40));
        let ts_1941 = Dttm::from_raw(pack_dttm(2025, 7, 8, 19, 41));
        let ts_1942 = Dttm::from_raw(pack_dttm(2025, 7, 8, 19, 42));
        let ts_1943 = Dttm::from_raw(pack_dttm(2025, 7, 8, 19, 43));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 4,
                text: "Hdr ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1940,
            },
            SourceSegment {
                start_cp: 4,
                end_cp: 28,
                text: "prefix clause ends ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1941,
            },
            SourceSegment {
                start_cp: 28,
                end_cp: 33,
                text: "with ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1942,
            },
            SourceSegment {
                start_cp: 33,
                end_cp: 44,
                text: "prior text ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: ts_1943,
            },
            SourceSegment {
                start_cp: 44,
                end_cp: 66,
                text: "to continue the clause".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(5),
                segment_author_index: None,
                segment_timestamp: ts_1941,
            },
        ];

        let points =
            split_points_for_redline(0, 66, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 28, 66]);
    }

    #[test]
    fn anchor_compat_lowercase_clause_excursion_skips_single_timestamp_middle() {
        let ts_1109 = Dttm::from_raw(pack_dttm(2025, 10, 1, 11, 9));
        let ts_1110 = Dttm::from_raw(pack_dttm(2025, 10, 1, 11, 10));
        let ts_1111 = Dttm::from_raw(pack_dttm(2025, 10, 1, 11, 11));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 22,
                text: "Landlord acknowledges ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1109,
            },
            SourceSegment {
                start_cp: 22,
                end_cp: 34,
                text: "Austin Fahy ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1110,
            },
            SourceSegment {
                start_cp: 34,
                end_cp: 52,
                text: "of Cresa Partners ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1111,
            },
            SourceSegment {
                start_cp: 52,
                end_cp: 80,
                text: "as Tenant's representative".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: ts_1110,
            },
        ];

        let points =
            split_points_for_redline(0, 80, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 80]);
    }

    #[test]
    fn deletion_comma_clause_format_excursion_inserts_right_start() {
        let ts = Dttm::from_raw(pack_dttm(2025, 4, 9, 14, 50));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 9,
                text: " does not".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts,
            },
            SourceSegment {
                start_cp: 9,
                end_cp: 11,
                text: ", ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts,
            },
            SourceSegment {
                start_cp: 11,
                end_cp: 26,
                text: "other than to a".to_string(),
                formatting_fingerprint: 2,
                formatting_sequence_fingerprint: 2,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts,
            },
        ];

        let points =
            split_points_for_redline(0, 26, RevisionType::Deletion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 11, 26]);
    }

    #[test]
    fn anchor_compat_midword_prefix_split_skips_single_run_middle() {
        let ts_0752 = Dttm::from_raw(pack_dttm(2025, 10, 22, 7, 52));
        let ts_0753 = Dttm::from_raw(pack_dttm(2025, 10, 22, 7, 53));
        let ts_0754 = Dttm::from_raw(pack_dttm(2025, 10, 22, 7, 54));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 4,
                text: " Not".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_0752,
            },
            SourceSegment {
                start_cp: 4,
                end_cp: 16,
                text: "withstanding".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_0754,
            },
            SourceSegment {
                start_cp: 16,
                end_cp: 63,
                text: " the foregoing, if Landlord does not cause the ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_0753,
            },
        ];

        let points =
            split_points_for_redline(0, 63, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 63]);
    }

    #[test]
    fn bracketed_note_prefix_boundary_keeps_single_deletion_entry() {
        let ts = Dttm::from_raw(pack_dttm(2025, 12, 7, 13, 16));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 7,
                text: "[note: ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts,
            },
            SourceSegment {
                start_cp: 7,
                end_cp: 70,
                text: "we are moving up this lease amendment earlier than expected".to_string(),
                formatting_fingerprint: 2,
                formatting_sequence_fingerprint: 2,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts,
            },
        ];

        let points =
            split_points_for_redline(0, 70, RevisionType::Deletion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 70]);
    }

    #[test]
    fn timestamp_oscillation_split_skips_punctuation_boundaries() {
        let ts_1150 = Dttm::from_raw(pack_dttm(2025, 6, 17, 11, 50));
        let ts_1151 = Dttm::from_raw(pack_dttm(2025, 6, 17, 11, 51));
        let ts_1152 = Dttm::from_raw(pack_dttm(2025, 6, 17, 11, 52));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 10,
                text: " Tenant’s ".to_string(),
                formatting_fingerprint: 42,
                formatting_sequence_fingerprint: 42,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1150,
            },
            SourceSegment {
                start_cp: 10,
                end_cp: 40,
                text: "rate shall reset to the Date,".to_string(),
                formatting_fingerprint: 42,
                formatting_sequence_fingerprint: 42,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1151,
            },
            SourceSegment {
                start_cp: 40,
                end_cp: 41,
                text: " ".to_string(),
                formatting_fingerprint: 42,
                formatting_sequence_fingerprint: 42,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1152,
            },
            SourceSegment {
                start_cp: 41,
                end_cp: 80,
                text: "and then escalate at 2.5% annually".to_string(),
                formatting_fingerprint: 42,
                formatting_sequence_fingerprint: 42,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: ts_1151,
            },
        ];

        let points =
            split_points_for_redline(0, 80, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 80]);
    }

    #[test]
    fn timestamp_oscillation_split_keeps_midword_boundaries() {
        let ts_1313 = Dttm::from_raw(pack_dttm(2019, 1, 18, 13, 13));
        let ts_1314 = Dttm::from_raw(pack_dttm(2019, 1, 18, 13, 14));
        let ts_1315 = Dttm::from_raw(pack_dttm(2019, 1, 18, 13, 15));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 20,
                text: " preceding period of ".to_string(),
                formatting_fingerprint: 7,
                formatting_sequence_fingerprint: 7,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1313,
            },
            SourceSegment {
                start_cp: 20,
                end_cp: 30,
                text: "rebuilding".to_string(),
                formatting_fingerprint: 7,
                formatting_sequence_fingerprint: 7,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1315,
            },
            SourceSegment {
                start_cp: 30,
                end_cp: 31,
                text: " ".to_string(),
                formatting_fingerprint: 7,
                formatting_sequence_fingerprint: 7,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1314,
            },
            SourceSegment {
                start_cp: 31,
                end_cp: 47,
                text: "and restoration".to_string(),
                formatting_fingerprint: 7,
                formatting_sequence_fingerprint: 7,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: ts_1315,
            },
        ];

        let points =
            split_points_for_redline(0, 47, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 31, 47]);
    }

    #[test]
    fn timestamp_oscillation_splits_anchor_incompatible_segment() {
        let ts_1203 = Dttm::from_raw(pack_dttm(2025, 7, 23, 12, 3));
        let ts_1205 = Dttm::from_raw(pack_dttm(2025, 7, 23, 12, 5));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 10,
                text: "Alpha text".to_string(),
                formatting_fingerprint: 9,
                formatting_sequence_fingerprint: 9,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1203,
            },
            SourceSegment {
                start_cp: 10,
                end_cp: 20,
                text: "Beta text".to_string(),
                formatting_fingerprint: 9,
                formatting_sequence_fingerprint: 9,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1203,
            },
            SourceSegment {
                start_cp: 20,
                end_cp: 30,
                text: "Gamma text".to_string(),
                formatting_fingerprint: 9,
                formatting_sequence_fingerprint: 9,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1205,
            },
        ];

        let points =
            split_points_for_redline(0, 30, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 20, 30]);
    }

    #[test]
    fn double_space_sentence_continuation_requires_real_timestamps() {
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 16,
                text: "SUCH TRANSFER.  ".to_string(),
                formatting_fingerprint: 10,
                formatting_sequence_fingerprint: 10,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: None,
            },
            SourceSegment {
                start_cp: 16,
                end_cp: 32,
                text: "ALL CHARGES NOW".to_string(),
                formatting_fingerprint: 20,
                formatting_sequence_fingerprint: 20,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: None,
            },
        ];

        let points =
            split_points_for_redline(0, 32, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 16, 32]);
    }

    #[test]
    fn double_space_sentence_continuation_can_merge_when_timestamps_match() {
        let ts = Dttm::from_raw(pack_dttm(2025, 2, 18, 9, 30));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 16,
                text: "SUCH TRANSFER.  ".to_string(),
                formatting_fingerprint: 10,
                formatting_sequence_fingerprint: 10,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts,
            },
            SourceSegment {
                start_cp: 16,
                end_cp: 48,
                text: "ALL CHARGES ARE FOR APPLICANT".to_string(),
                formatting_fingerprint: 20,
                formatting_sequence_fingerprint: 20,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts,
            },
        ];

        let points =
            split_points_for_redline(0, 48, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 48]);
    }

    #[test]
    fn timestamp_excursion_block_splits_between_matching_outer_timestamps() {
        let ts_0909 = Dttm::from_raw(pack_dttm(2025, 11, 26, 9, 9));
        let ts_0911 = Dttm::from_raw(pack_dttm(2025, 11, 26, 9, 11));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 8,
                text: "Furniture".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_0909,
            },
            SourceSegment {
                start_cp: 8,
                end_cp: 16,
                text: " to be ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_0909,
            },
            SourceSegment {
                start_cp: 16,
                end_cp: 28,
                text: ". If Tenant".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_0911,
            },
            SourceSegment {
                start_cp: 28,
                end_cp: 40,
                text: " owns FF&E".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: ts_0909,
            },
        ];

        let points =
            split_points_for_redline(0, 40, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 16, 28, 40]);
    }

    #[test]
    fn timestamp_excursion_block_skips_when_outer_timestamps_do_not_match() {
        let ts_0909 = Dttm::from_raw(pack_dttm(2025, 11, 26, 9, 9));
        let ts_0910 = Dttm::from_raw(pack_dttm(2025, 11, 26, 9, 10));
        let ts_0911 = Dttm::from_raw(pack_dttm(2025, 11, 26, 9, 11));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 8,
                text: "Furniture".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_0909,
            },
            SourceSegment {
                start_cp: 8,
                end_cp: 20,
                text: ". If Tenant".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_0911,
            },
            SourceSegment {
                start_cp: 20,
                end_cp: 32,
                text: " owns FF&E".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_0910,
            },
        ];

        let points =
            split_points_for_redline(0, 32, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 32]);
    }

    #[test]
    fn anchor_compat_excursion_block_splits_substantive_sentence_excursion() {
        let ts_1218 = Dttm::from_raw(pack_dttm(2019, 5, 15, 12, 18));
        let ts_1219 = Dttm::from_raw(pack_dttm(2019, 5, 15, 12, 19));
        let ts_1220 = Dttm::from_raw(pack_dttm(2019, 5, 15, 12, 20));
        let ts_1221 = Dttm::from_raw(pack_dttm(2019, 5, 15, 12, 21));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 8,
                text: "Landlord".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1218,
            },
            SourceSegment {
                start_cp: 8,
                end_cp: 16,
                text: " branding".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1219,
            },
            SourceSegment {
                start_cp: 16,
                end_cp: 28,
                text: ". The size ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1220,
            },
            SourceSegment {
                start_cp: 28,
                end_cp: 40,
                text: "and location".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: ts_1221,
            },
            SourceSegment {
                start_cp: 40,
                end_cp: 41,
                text: " ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(5),
                segment_author_index: None,
                segment_timestamp: ts_1219,
            },
        ];

        let points =
            split_points_for_redline(0, 41, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 16, 41]);
    }

    #[test]
    fn anchor_compat_excursion_block_skips_micro_word_excursion() {
        let ts_0936 = Dttm::from_raw(pack_dttm(2025, 8, 12, 9, 36));
        let ts_0937 = Dttm::from_raw(pack_dttm(2025, 8, 12, 9, 37));
        let ts_0938 = Dttm::from_raw(pack_dttm(2025, 8, 12, 9, 38));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 16,
                text: "Seller shall ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_0936,
            },
            SourceSegment {
                start_cp: 16,
                end_cp: 18,
                text: "to".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_0938,
            },
            SourceSegment {
                start_cp: 18,
                end_cp: 32,
                text: " notify parties".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_0937,
            },
        ];

        let points =
            split_points_for_redline(0, 32, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 32]);
    }

    #[test]
    fn anchor_compat_terminal_tail_excursion_split_starts_later_tail() {
        let ts_1709 = Dttm::from_raw(pack_dttm(2023, 6, 1, 17, 9));
        let ts_1710 = Dttm::from_raw(pack_dttm(2023, 6, 1, 17, 10));
        let ts_1711 = Dttm::from_raw(pack_dttm(2023, 6, 1, 17, 11));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 9,
                text: "Written n".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1710,
            },
            SourceSegment {
                start_cp: 9,
                end_cp: 28,
                text: "otice must be given".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1709,
            },
            SourceSegment {
                start_cp: 28,
                end_cp: 41,
                text: " no less than".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1709,
            },
            SourceSegment {
                start_cp: 41,
                end_cp: 63,
                text: " 60 days prior to this".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: ts_1710,
            },
            SourceSegment {
                start_cp: 63,
                end_cp: 104,
                text: " date and the minimum number that the".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(5),
                segment_author_index: None,
                segment_timestamp: ts_1710,
            },
            SourceSegment {
                start_cp: 104,
                end_cp: 133,
                text: " Tenant can reduce to is ten".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(6),
                segment_author_index: None,
                segment_timestamp: ts_1710,
            },
            SourceSegment {
                start_cp: 133,
                end_cp: 138,
                text: " (10)".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(7),
                segment_author_index: None,
                segment_timestamp: ts_1710,
            },
            SourceSegment {
                start_cp: 138,
                end_cp: 154,
                text: " reserved spaces".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(8),
                segment_author_index: None,
                segment_timestamp: ts_1711,
            },
            SourceSegment {
                start_cp: 154,
                end_cp: 156,
                text: ". ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(9),
                segment_author_index: None,
                segment_timestamp: ts_1710,
            },
        ];

        let points =
            split_points_for_redline(0, 156, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 138, 156]);
    }

    #[test]
    fn anchor_compat_midword_return_split_starts_midword_timestamp_excursion() {
        let ts_1058 = Dttm::from_raw(pack_dttm(2024, 4, 3, 10, 58));
        let ts_1059 = Dttm::from_raw(pack_dttm(2024, 4, 3, 10, 59));
        let ts_1100 = Dttm::from_raw(pack_dttm(2024, 4, 3, 11, 0));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 16,
                text: "Tenant shall de".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(1),
                segment_author_index: None,
                segment_timestamp: ts_1059,
            },
            SourceSegment {
                start_cp: 16,
                end_cp: 30,
                text: "fend the same ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(2),
                segment_author_index: None,
                segment_timestamp: ts_1059,
            },
            SourceSegment {
                start_cp: 30,
                end_cp: 40,
                text: "at Landlor".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(3),
                segment_author_index: None,
                segment_timestamp: ts_1058,
            },
            SourceSegment {
                start_cp: 40,
                end_cp: 42,
                text: "d’".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(4),
                segment_author_index: None,
                segment_timestamp: ts_1100,
            },
            SourceSegment {
                start_cp: 42,
                end_cp: 63,
                text: "s expense by counsel".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(5),
                segment_author_index: None,
                segment_timestamp: ts_1059,
            },
            SourceSegment {
                start_cp: 63,
                end_cp: 67,
                text: ".  ".to_string(),
                formatting_fingerprint: 1,
                formatting_sequence_fingerprint: 1,
                source_chpx_id: Some(6),
                segment_author_index: None,
                segment_timestamp: ts_1100,
            },
        ];

        let points =
            split_points_for_redline(0, 67, RevisionType::Insertion, false, &segments, &[], &[]);
        assert_eq!(points, vec![0, 40, 67]);
    }

    #[test]
    fn stacked_deletion_singleton_boundary_can_stay_merged() {
        let ts = Dttm::from_raw(pack_dttm(2025, 9, 24, 9, 43));
        let segments = vec![
            SourceSegment {
                start_cp: 0,
                end_cp: 40,
                text: "Any restoration work must be completed t".to_string(),
                formatting_fingerprint: 100,
                formatting_sequence_fingerprint: 100,
                source_chpx_id: Some(10),
                segment_author_index: None,
                segment_timestamp: ts,
            },
            SourceSegment {
                start_cp: 40,
                end_cp: 41,
                text: "o".to_string(),
                formatting_fingerprint: 101,
                formatting_sequence_fingerprint: 101,
                source_chpx_id: Some(11),
                segment_author_index: None,
                segment_timestamp: ts,
            },
            SourceSegment {
                start_cp: 41,
                end_cp: 74,
                text: " the satisfaction of counterparty. ".to_string(),
                formatting_fingerprint: 102,
                formatting_sequence_fingerprint: 102,
                source_chpx_id: Some(12),
                segment_author_index: None,
                segment_timestamp: ts,
            },
        ];

        let points =
            split_points_for_redline(0, 74, RevisionType::Deletion, true, &segments, &[], &[]);
        assert_eq!(points, vec![0, 74]);
    }
}
