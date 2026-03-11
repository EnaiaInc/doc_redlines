use crate::dttm::timestamps_compatible;
use crate::model::{RedlineSignature, StackMetadata};

pub fn can_combine(
    left: &RedlineSignature,
    left_start: u32,
    left_end: u32,
    right: &RedlineSignature,
    right_start: u32,
    right_end: u32,
) -> bool {
    left.revision_type == right.revision_type
        && left.author_index == right.author_index
        && timestamps_compatible(left.timestamp, right.timestamp)
        && ranges_adjacent_or_overlapping(left_start, left_end, right_start, right_end)
        && stack_compatible(left.stack.as_deref(), right.stack.as_deref())
}

pub fn ranges_adjacent_or_overlapping(
    left_start: u32,
    left_end: u32,
    right_start: u32,
    right_end: u32,
) -> bool {
    if left_start > left_end || right_start > right_end {
        return false;
    }

    let overlaps = left_start < right_end && right_start < left_end;
    let adjacent = left_end == right_start || right_end == left_start;
    overlaps || adjacent
}

fn stack_compatible(left: Option<&StackMetadata>, right: Option<&StackMetadata>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
        (Some(a), Some(b)) => {
            a.revision_type == b.revision_type
                && a.author_index == b.author_index
                && timestamps_compatible(a.timestamp, b.timestamp)
                && stack_compatible(a.next.as_deref(), b.next.as_deref())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dttm::Dttm;
    use crate::model::{RedlineSignature, RevisionType, StackMetadata};

    use super::can_combine;

    fn sig(revision_type: RevisionType, author: Option<u16>) -> RedlineSignature {
        RedlineSignature {
            revision_type,
            author_index: author,
            timestamp: Dttm::from_raw(0x4E2A0C01),
            stack: None,
        }
    }

    #[test]
    fn mixed_stacking_is_blocked() {
        let mut left = sig(RevisionType::Insertion, Some(1));
        left.stack = Some(Box::new(StackMetadata {
            revision_type: RevisionType::Deletion,
            author_index: Some(4),
            timestamp: Dttm::from_raw(0x4E2A0C01),
            next: None,
        }));

        let right = sig(RevisionType::Insertion, Some(1));
        assert!(!can_combine(&left, 0, 5, &right, 5, 9));
    }

    #[test]
    fn same_signature_adjacent_ranges_can_combine() {
        let left = sig(RevisionType::Deletion, Some(2));
        let right = sig(RevisionType::Deletion, Some(2));
        assert!(can_combine(&left, 0, 4, &right, 4, 7));
    }
}
