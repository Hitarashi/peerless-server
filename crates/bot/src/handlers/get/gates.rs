//! `/get` policy gates. Strings intentionally remain literals: they are part
//! of the Telegram API of the bot .

use engine::{settings::BotSettings, types::ParsedTargetItem};

pub const CACHE_RESTRICTED: &str = "! <b>Access restricted</b><br/>Caching directly to the dump channel is restricted to the bot owner.";
pub const FORCE_RESTRICTED: &str =
    "! <b>Access restricted</b><br/>Force re-rip is restricted to the bot owner.";
pub const PAUSED: &str =
    "! <b>Ripping is temporarily paused for maintenance.</b><br/>Please try again later.";
pub const ALBUM_DISABLED: &str =
    "! <b>Album ripping is currently disabled.</b><br/>Please request individual tracks instead.";
pub const PLAYLIST_DISABLED: &str = "! <b>Playlist ripping is currently disabled.</b><br/>Please request individual tracks instead.";
pub const ARTIST_DISABLED: &str = "! <b>Artist ripping is currently disabled.</b><br/>Please request individual tracks or albums instead.";
pub const TXT_DISABLED: &str = "! <b>.TXT file ripping is currently disabled.</b><br/>Please request individual links instead.";
pub const MULTI_DISABLED: &str = "! <b>Multi-link ripping is currently disabled.</b><br/>Please request tracks or collections one at a time.";

pub fn cache_gate(is_cache_only: bool, is_admin: bool) -> Option<&'static str> {
    (is_cache_only && !is_admin).then_some(CACHE_RESTRICTED)
}

pub fn force_gate(force: bool, is_admin: bool) -> Option<&'static str> {
    (force && !is_admin).then_some(FORCE_RESTRICTED)
}

/// Returns the first feature gate which fails, preserving the ordering.
pub fn feature_gate(
    settings: &BotSettings,
    items: &[ParsedTargetItem],
    is_admin: bool,
    is_document: bool,
) -> Option<&'static str> {
    if is_admin {
        return None;
    }
    if !settings.can_serve_cache(false) {
        return Some(PAUSED);
    }
    if items
        .iter()
        .any(|x| x.kind == engine::types::TargetKind::Album)
        && !settings.can_rip_album(false)
    {
        return Some(ALBUM_DISABLED);
    }
    if items
        .iter()
        .any(|x| x.kind == engine::types::TargetKind::Playlist)
        && !settings.can_rip_playlist(false)
    {
        return Some(PLAYLIST_DISABLED);
    }
    if items
        .iter()
        .any(|x| x.kind == engine::types::TargetKind::Artist)
        && !settings.can_rip_artist(false)
    {
        return Some(ARTIST_DISABLED);
    }
    if is_document && !settings.can_rip_txt(false) {
        return Some(TXT_DISABLED);
    }
    if !is_document && items.len() > 1 && !settings.can_rip_multi_link(false) {
        return Some(MULTI_DISABLED);
    }
    None
}

pub fn usage(is_cache_only: bool) -> &'static str {
    if is_cache_only {
        "<b>Apple Music dumper (admin)</b><br/><br/><blockquote><b>Usage:</b><br/>• <code>/get &lt;Apple Music link&gt;</code><br/>• Send multiple links or attach a <code>.txt</code> file<br/>• Multi-track albums are archived as ZIPs<br/>• Option: <code>-f</code> (force re-rip)<br/><i>Seeds primary audio and optional Dolby Atmos into the dump channel and database.</i></blockquote>"
    } else {
        "<b>Apple Music downloader</b><br/><br/><blockquote><b>Supported inputs:</b><br/>• <code>/get &lt;Apple Music link&gt;</code><br/>• Send multiple links or attach a <code>.txt</code> file<br/>• Multi-track albums are delivered as ZIPs automatically<br/>• Primary audio plus optional Dolby Atmos<br/>• Missing Atmos is silent<br/>• Cancel with the Cancel download button<br/>• Option: <code>-f</code> (force re-rip)</blockquote>"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn usage_and_restrictions_are_actionable() {
        assert!(usage(false).contains("Cancel download button"));
        assert!(usage(false).contains("/get"));
        assert!(usage(false).contains("optional Dolby Atmos"));
        assert!(!usage(false).contains("/alac"));
        assert!(!usage(false).contains('🎵'));
        assert_eq!(cache_gate(true, false), Some(CACHE_RESTRICTED));
        assert_eq!(force_gate(true, false), Some(FORCE_RESTRICTED));
        assert_eq!(cache_gate(true, true), None);
    }
}
