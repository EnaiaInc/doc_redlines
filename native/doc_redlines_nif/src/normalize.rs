pub fn normalize_revision_text(text: &str) -> String {
    text.chars()
        .filter_map(|ch| match ch {
            '\0' | '\t' | '\u{0003}' | '\u{0004}' | '\u{0005}' | '\u{0006}' | '\u{0008}'
            | '\u{000B}' | '\u{001E}' | '\u{001F}' | '\u{0013}' | '\u{0014}' | '\u{0015}'
            | '\u{0001}' | '\u{FFF9}' => None,
            '\u{0007}' | '\r' | '\n' => Some(' '),
            _ => Some(ch),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::normalize_revision_text;

    #[test]
    fn strips_and_replaces_control_characters() {
        let input = "A\0B\t\u{0003}\u{0004}\u{0005}\u{0006}\u{0008}\u{000B}\u{001E}C\u{001F}D\u{0001}\u{FFF9}\u{0007}E\rF\nG";
        let normalized = normalize_revision_text(input);
        assert_eq!(normalized, "ABCD E F G");
    }
}
