//! Apple Music link/id parser.
//!
//! Order of regex checks matters: playlist URL → artist URL → album URL with
//! `?i=` → direct song URL → album URL. Storefronts are lowercased when
//! captured.

use std::sync::OnceLock;

use music::{ParsedAlacInput, ParsedTargetItem, TargetKind};
use regex::Regex;

fn regexes() -> &'static [Regex; 5] {
    static RE: OnceLock<[Regex; 5]> = OnceLock::new();
    RE.get_or_init(|| {
        // 1. music.apple.com/…/playlist/<slug>/pl.xxx or pl.u-xxx
        let playlist = Regex::new(
            r"(?i)(?:music|itunes)\.apple\.com/(?:([a-z]{2})/)?playlist/(?:[^/]+/)?(pl\.(?:u-[a-zA-Z0-9]+|[a-zA-Z0-9]+))",
        )
        .expect("playlist regex");
        // 2. music.apple.com/…/artist/<slug>/<id> (also itunes.apple.com)
        let artist = Regex::new(r"(?i)(?:music|itunes)\.apple\.com/(?:([a-z]{2})/)?artist/(?:[^/]+/)?(\d+)")
            .expect("artist regex");
        // 3. album URL with ?i=<track id>
        let song_with_album = Regex::new(r"(?i)(?:music|itunes)\.apple\.com/(?:([a-z]{2})/)?album/(?:[^/]+/)?\d+\?i=(\d+)")
            .expect("song with album regex");
        // 4. direct song URL
        let song_direct = Regex::new(r"(?i)(?:music|itunes)\.apple\.com/(?:([a-z]{2})/)?song/(?:[^/]+/)?(\d+)").expect("song direct regex");
        // 5. album URL
        let album = Regex::new(r"(?i)(?:music|itunes)\.apple\.com/(?:([a-z]{2})/)?album/(?:[^/]+/)?(\d+)").expect("album regex");
        [playlist, artist, song_with_album, song_direct, album]
    })
}

fn storefront_of(caps: &regex::Captures<'_>) -> Option<String> {
    caps.get(1).map(|m| m.as_str().to_lowercase())
}
fn item(id: impl Into<String>, kind: TargetKind, caps: &regex::Captures<'_>) -> ParsedTargetItem {
    ParsedTargetItem {
        id: id.into(),
        kind,
        storefront: storefront_of(caps),
    }
}

/// Parse a single token into a target item, or `None` when unrecognized.
pub fn parse_single_item(raw_token: &str) -> Option<ParsedTargetItem> {
    let token = raw_token.trim();
    if token.is_empty() {
        return None;
    }
    let [playlist, artist, song_with_album, song_direct, album] = regexes();

    if let Some(caps) = playlist.captures(token)
        && let Some(id) = caps.get(2)
    {
        return Some(item(id.as_str(), TargetKind::Playlist, &caps));
    }
    if let Some(caps) = artist.captures(token)
        && let Some(id) = caps.get(2)
    {
        return Some(item(id.as_str(), TargetKind::Artist, &caps));
    }
    if let Some(caps) = song_with_album.captures(token)
        && let Some(id) = caps.get(2)
    {
        return Some(item(id.as_str(), TargetKind::Track, &caps));
    }
    if let Some(caps) = song_direct.captures(token)
        && let Some(id) = caps.get(2)
    {
        return Some(item(id.as_str(), TargetKind::Track, &caps));
    }
    if let Some(caps) = album.captures(token)
        && let Some(id) = caps.get(2)
    {
        return Some(item(id.as_str(), TargetKind::Album, &caps));
    }
    None
}

/// Extract items from a `.txt` batch file's content: lines split on
/// newlines, tokens split on whitespace; `#` or `//` stops the line's
/// remaining tokens. Dedup by `{kind}:{id}` preserving first-seen order.
pub fn extract_batch_items(content: &str) -> Vec<ParsedTargetItem> {
    let mut results = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for line in content.split(['\r', '\n']) {
        'tokens: for token in line.split_whitespace() {
            if token.starts_with('#') || token.starts_with("//") {
                break 'tokens;
            }
            if let Some(parsed) = parse_single_item(token) {
                let key = format!("{:?}", (parsed.kind, parsed.id.clone()));
                if seen.insert(key) {
                    results.push(parsed);
                }
            }
        }
    }
    results
}

/// Parse a full command text (with optional reply text fallback) into a
/// structured input. Returns `None` when nothing parseable was found.
pub fn parse_alac_input(raw_text: &str, reply_text: Option<&str>) -> Option<ParsedAlacInput> {
    let text = raw_text.trim();
    let mut tokens = text.split_whitespace().peekable();
    if let Some(first) = tokens.peek()
        && first.starts_with('/')
    {
        tokens.next();
    }

    let mut force = false;
    let mut filtered: Vec<&str> = Vec::new();
    for token in tokens {
        if token == "-f" || token == "--force" {
            force = true;
        } else {
            filtered.push(token);
        }
    }

    let mut items: Vec<ParsedTargetItem> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for token in filtered {
        if let Some(parsed) = parse_single_item(token) {
            let key = format!("{:?}", (parsed.kind, parsed.id.clone()));
            if seen.insert(key) {
                items.push(parsed);
            }
        }
    }

    if items.is_empty()
        && let Some(reply) = reply_text
    {
        for token in reply.split_whitespace() {
            if let Some(parsed) = parse_single_item(token) {
                let key = format!("{:?}", (parsed.kind, parsed.id.clone()));
                if seen.insert(key) {
                    items.push(parsed);
                }
            }
        }
    }

    let first = items.first()?;
    Some(ParsedAlacInput {
        track_id: first.id.clone(),
        force,
        is_album: first.kind == TargetKind::Album,
        is_playlist: first.kind == TargetKind::Playlist,
        is_artist: first.kind == TargetKind::Artist,
        storefront: first.storefront.clone(),
        items,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(id: &str, sf: Option<&str>) -> ParsedTargetItem {
        ParsedTargetItem {
            id: id.into(),
            kind: TargetKind::Track,
            storefront: sf.map(str::to_owned),
        }
    }
    fn album(id: &str, sf: Option<&str>) -> ParsedTargetItem {
        ParsedTargetItem {
            id: id.into(),
            kind: TargetKind::Album,
            storefront: sf.map(str::to_owned),
        }
    }
    fn playlist(id: &str, sf: Option<&str>) -> ParsedTargetItem {
        ParsedTargetItem {
            id: id.into(),
            kind: TargetKind::Playlist,
            storefront: sf.map(str::to_owned),
        }
    }
    fn artist(id: &str, sf: Option<&str>) -> ParsedTargetItem {
        ParsedTargetItem {
            id: id.into(),
            kind: TargetKind::Artist,
            storefront: sf.map(str::to_owned),
        }
    }

    #[test]
    fn extracts_track_from_album_link_with_i_param() {
        let res = parse_alac_input(
            "/alac https://music.apple.com/us/album/shape-of-you/1193701079?i=1193701400",
            None,
        )
        .unwrap();
        assert_eq!(res.items, vec![track("1193701400", Some("us"))]);
        assert_eq!(res.track_id, "1193701400");
        assert!(!res.force && !res.is_album && !res.is_playlist && !res.is_artist);
        assert_eq!(res.storefront.as_deref(), Some("us"));
    }

    #[test]
    fn extracts_track_with_force_flag() {
        let res = parse_alac_input(
            "/alac https://music.apple.com/us/album/shape-of-you/1193701079?i=1193701400 -f",
            None,
        )
        .unwrap();
        assert!(res.force);
        assert_eq!(res.track_id, "1193701400");
    }

    #[test]
    fn extracts_track_with_force_flag_in_front() {
        let res = parse_alac_input(
            "/alac --force https://music.apple.com/us/album/shape-of-you/1193701079?i=1193701400",
            None,
        )
        .unwrap();
        assert!(res.force);
        assert_eq!(res.track_id, "1193701400");
    }

    #[test]
    fn extracts_track_from_direct_song_link_regional() {
        let res = parse_alac_input(
            "/alac https://music.apple.com/in/song/tum-hi-ho/1122334455",
            None,
        )
        .unwrap();
        assert_eq!(res.items, vec![track("1122334455", Some("in"))]);
        assert_eq!(res.storefront.as_deref(), Some("in"));
    }

    #[test]
    fn extracts_album_from_direct_album_link() {
        let res = parse_alac_input(
            "/alac https://music.apple.com/us/album/blinding-lights/1499378108",
            None,
        )
        .unwrap();
        assert_eq!(res.items, vec![album("1499378108", Some("us"))]);
        assert!(res.is_album);
        assert_eq!(res.track_id, "1499378108");
    }

    #[test]
    fn extracts_playlist_from_url() {
        let res = parse_alac_input(
            "/alac https://music.apple.com/us/playlist/todays-hits/pl.f4d106fed2bd41149aaacabb233eb5eb",
            None,
        )
        .unwrap();
        assert_eq!(
            res.items,
            vec![playlist("pl.f4d106fed2bd41149aaacabb233eb5eb", Some("us"))]
        );
        assert!(res.is_playlist);
    }

    #[test]
    fn extracts_artist_from_url() {
        let res = parse_alac_input(
            "/alac https://music.apple.com/us/artist/taylor-swift/159260351",
            None,
        )
        .unwrap();
        assert_eq!(res.items, vec![artist("159260351", Some("us"))]);
        assert!(res.is_artist);
    }

    #[test]
    fn rejects_bare_artist_with_prefix() {
        assert!(parse_alac_input("/alac artist:159260351", None).is_none());
    }

    #[test]
    fn rejects_bare_artist_path() {
        assert!(parse_alac_input("/alac Artist/159260351", None).is_none());
    }

    #[test]
    fn extracts_user_curated_playlist_pl_u_prefix() {
        let res = parse_alac_input(
            "/batch https://music.apple.com/playlist/chill-vibes/pl.u-76oNke3FvPyK8r",
            None,
        )
        .unwrap();
        assert_eq!(res.items, vec![playlist("pl.u-76oNke3FvPyK8r", None)]);
        assert!(res.is_playlist);
    }

    #[test]
    fn rejects_bare_track_id() {
        assert!(parse_alac_input("/alac 1440841730", None).is_none());
    }

    #[test]
    fn rejects_bare_playlist_id() {
        assert!(parse_alac_input("/dl pl.f4d106fed2bd41149aaacabb233eb5eb", None).is_none());
    }

    #[test]
    fn extracts_multiple_items_from_multi_link() {
        let res = parse_alac_input(
            "/batch https://music.apple.com/us/album/song1/1000?i=1111 https://music.apple.com/us/album/album2/2222",
            None,
        )
        .unwrap();
        assert_eq!(res.items.len(), 2);
        assert_eq!(res.items[0], track("1111", Some("us")));
        assert_eq!(res.items[1], album("2222", Some("us")));
    }

    #[test]
    fn extracts_from_reply_when_no_args() {
        let reply =
            "Check this song https://music.apple.com/jp/album/song/1000?i=2000 it is awesome";
        let res = parse_alac_input("/alac", Some(reply)).unwrap();
        assert_eq!(res.items, vec![track("2000", Some("jp"))]);
        assert_eq!(res.storefront.as_deref(), Some("jp"));
    }

    #[test]
    fn extracts_from_reply_with_force_on_command() {
        let reply = "https://music.apple.com/us/album/song/1000?i=2000";
        let res = parse_alac_input("/alac -f", Some(reply)).unwrap();
        assert!(res.force);
        assert_eq!(res.items, vec![track("2000", Some("us"))]);
    }

    #[test]
    fn returns_none_for_invalid_input() {
        assert!(parse_alac_input("/alac", None).is_none());
        assert!(parse_alac_input("/alac not_a_link", None).is_none());
    }

    #[test]
    fn batch_extracts_and_ignores_comments_and_blank_lines() {
        let file_content = "
# My Queue of songs
https://music.apple.com/us/album/shape-of-you/1193701079?i=1193701400

https://music.apple.com/us/album/blinding-lights/1499378108

# Artist link
https://music.apple.com/us/artist/taylor-swift/159260351

# Playlist
https://music.apple.com/us/playlist/todays-hits/pl.f4d106fed2bd41149aaacabb233eb5eb
not-a-link
";
        let items = extract_batch_items(file_content);
        assert_eq!(
            items,
            vec![
                track("1193701400", Some("us")),
                album("1499378108", Some("us")),
                artist("159260351", Some("us")),
                playlist("pl.f4d106fed2bd41149aaacabb233eb5eb", Some("us")),
            ]
        );
    }

    #[test]
    fn batch_deduplicates_identical_entries() {
        let file_content = "
 not-a-link
 not-a-link
https://music.apple.com/us/album/song/1?i=1440841730
";
        let items = extract_batch_items(file_content);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], track("1440841730", Some("us")));
    }
}
