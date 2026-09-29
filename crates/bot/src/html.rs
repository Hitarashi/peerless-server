/// Escape dynamic text for Telegram HTML.
///
/// Delegates to the engine's escaper so the bot and the engine cannot drift
/// apart: the exact set of replaced characters is a contract with Telegram's
/// HTML parse mode, and a divergence here would either inject raw markup or
/// show `&amp;` literally to the user.
pub fn escape(value: &str) -> String {
    engine::orchestrator::caption::html_escape(value)
}

/// Prepare the dynamic HTML used by the TypeScript bot for ferogram's parser.
/// Ferogram's parser understands all the tags the emits, including
/// `<br>`, `<blockquote>`, and `<blockquote expandable>`, natively, so the
/// content passes through unchanged. This helper is kept as the single place
/// where Telegram-HTML dialect differences are reconciled.
pub fn parse_dynamic_html(content: &str) -> String {
    // Presentation policy: decorative emoji are not used to carry meaning.
    // Keep the semantic ASCII markers used by the new renderer (✓, !, ×, …)
    // and remove legacy pictographs at the final Telegram seam so any
    // unmigrated administrative handler still follows the UX rule.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_html_seam_removes_legacy_decoration_but_keeps_semantic_markers() {
        assert_eq!(parse_dynamic_html("⚠️ <b>Paused</b> 🎵"), " <b>Paused</b> ");
        assert_eq!(parse_dynamic_html("✓ <b>Complete</b>"), "✓ <b>Complete</b>");
    }

    /// The exact replacement set is a contract with Telegram's HTML parse
    /// mode. Escaping too little injects raw markup; escaping too much shows
    /// `&amp;` literally to the user.
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
        // The ampersand of a pre-existing entity must itself be escaped.
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
