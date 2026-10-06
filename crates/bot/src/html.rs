pub fn escape(value: &str) -> String {
    engine::orchestrator::caption::html_escape(value)
}

pub fn parse_dynamic_html(content: &str) -> String {
    content
        .chars()
        .filter(|ch| {
            !matches!(
                *ch,
                '✅' | '❌'
                    | '⚠'
                    | '🎵'
                    | '🎧'
                    | '📀'
                    | '📁'
                    | '📊'
                    | '🔄'
                    | '⏳'
                    | '🗑'
                    | '🔍'
                    | '🎲'
                    | '💾'
                    | '📥'
                    | '📤'
                    | '🔗'
                    | '🛠'
                    | '⚙'
                    | 'ℹ'
                    | '🚫'
                    | '👋'
                    | '🔥'
                    | '💿'
                    | '📋'
                    | '🧹'
                    | '🗒'
                    | '🟢'
                    | '🔴'
                    | '🟡'
                    | '📩'
                    | '🏓'
                    | '⚡'
                    | '🌐'
                    | '💡'
                    | '🚨'
                    | '✂'
                    | '🏷'
                    | '✍'
                    | '🛑'
                    | '📡'
                    | '🔙'
                    | '⬅'
                    | '👉'
                    | '🚀'
                    | '💽'
                    | '🆔'
                    | '🖥'
                    | '🔒'
                    | '⛔'
                    | '\u{fe0f}'
            )
        })
        .collect()
}

pub fn unescape(value: &str) -> String {
    if !value.contains('&') {
        return value.to_string();
    }
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '&' {
            let mut entity = String::new();
            let mut matched = false;
            while let Some(&next_c) = chars.peek() {
                if next_c == ';' {
                    chars.next();
                    matched = true;
                    break;
                }
                if next_c == '&' || next_c.is_whitespace() || entity.len() > 10 {
                    break;
                }
                entity.push(chars.next().unwrap());
            }
            if matched {
                match entity.as_str() {
                    "amp" => result.push('&'),
                    "lt" => result.push('<'),
                    "gt" => result.push('>'),
                    "quot" => result.push('"'),
                    "apos" | "#39" => result.push('\''),
                    _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                        if let Ok(code) = u32::from_str_radix(&entity[2..], 16) {
                            if let Some(decoded) = char::from_u32(code) {
                                result.push(decoded);
                            } else {
                                result.push('&');
                                result.push_str(&entity);
                                result.push(';');
                            }
                        } else {
                            result.push('&');
                            result.push_str(&entity);
                            result.push(';');
                        }
                    }
                    _ if entity.starts_with('#') => {
                        if let Ok(code) = entity[1..].parse::<u32>() {
                            if let Some(decoded) = char::from_u32(code) {
                                result.push(decoded);
                            } else {
                                result.push('&');
                                result.push_str(&entity);
                                result.push(';');
                            }
                        } else {
                            result.push('&');
                            result.push_str(&entity);
                            result.push(';');
                        }
                    }
                    _ => {
                        result.push('&');
                        result.push_str(&entity);
                        result.push(';');
                    }
                }
            } else {
                result.push('&');
                result.push_str(&entity);
            }
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unescape_decodes_standard_and_numeric_entities() {
        assert_eq!(unescape("It Ain&#39;t Me"), "It Ain't Me");
        assert_eq!(unescape("A &amp; B"), "A & B");
        assert_eq!(unescape("&quot;Hello&quot;"), "\"Hello\"");
        assert_eq!(unescape("&lt;tag&gt;"), "<tag>");
        assert_eq!(unescape("&#x27;"), "'");
    }

    #[test]
    fn final_html_seam_removes_legacy_decoration_but_keeps_semantic_markers() {
        assert_eq!(parse_dynamic_html("⚠️ <b>Paused</b> 🎵"), " <b>Paused</b> ");
        assert_eq!(parse_dynamic_html("✓ <b>Complete</b>"), "✓ <b>Complete</b>");
    }

    #[test]
    fn escape_replaces_exactly_the_five_html_significant_characters() {
        assert_eq!(escape("&"), "&amp;");
        assert_eq!(escape("<"), "&lt;");
        assert_eq!(escape(">"), "&gt;");
        assert_eq!(escape("\""), "&quot;");
        assert_eq!(escape("'"), "&#39;");
        assert_eq!(escape("a&b<c>d\"e'f"), "a&amp;b&lt;c&gt;d&quot;e&#39;f");
    }

    #[test]
    fn escape_ampersand_first_so_entities_are_not_double_escaped() {
        assert_eq!(escape("&amp;"), "&amp;amp;");
    }

    #[test]
    fn escape_leaves_ordinary_text_untouched() {
        assert_eq!(escape(""), "");
        assert_eq!(
            escape("Night Song — A&R <duo>"),
            "Night Song — A&amp;R &lt;duo&gt;"
        );
    }
}
