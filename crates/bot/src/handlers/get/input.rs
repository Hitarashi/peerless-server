//! Input handling for the `/get` command.
//!
//! Parses Apple Music and Qobuz links, Telegram replies, and text documents.

use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use apple::{extract_batch_items, parse_alac_input};
use engine::{limits::MAX_DOCUMENT_BYTES, types::ParsedTargetItem};
use ferogram::update::IncomingMessage;
use music::CodecPreference;

#[derive(Debug, Clone)]
pub struct ParsedCommand {
    pub provider: engine::types::Provider,
    pub items: Vec<ParsedTargetItem>,
    pub force: bool,
    pub storefront: Option<String>,
    pub document: bool,
    pub from_reply: bool,
    pub reply_sender_id: Option<i64>,
    pub reply_sender_name: Option<String>,
    pub codec_preference: Option<CodecPreference>,
}

pub fn has_force_token(text: &str) -> bool {
    text.split_whitespace().any(|t| t == "-f" || t == "--force")
}

/// Extracts Qobuz entities from whitespace-separated text tokens.
pub fn parse_qobuz_entities(text: &str) -> Vec<ParsedTargetItem> {
    let mut items = Vec::new();
    for token in text.split_whitespace() {
        let clean = token.trim_matches(|c| c == '<' || c == '>' || c == '"' || c == '\'');
        if let Some(entity) = qobuz::parse_qobuz_url(clean) {
            let kind = match entity.kind {
                qobuz::QobuzKind::Track => engine::types::TargetKind::Track,
                qobuz::QobuzKind::Album => engine::types::TargetKind::Album,
                qobuz::QobuzKind::Artist => engine::types::TargetKind::Artist,
                qobuz::QobuzKind::Playlist => engine::types::TargetKind::Playlist,
            };
            items.push(ParsedTargetItem {
                id: entity.id,
                kind,
                storefront: Some("qobuz".to_string()),
            });
        }
    }
    items
}

pub fn parse_text(text: &str, reply: Option<&str>, force_override: bool) -> Option<ParsedCommand> {
    // 1. Check for direct Qobuz links
    let direct_qobuz = parse_qobuz_entities(text);
    if !direct_qobuz.is_empty() {
        return Some(ParsedCommand {
            provider: engine::types::Provider::Qobuz,
            items: direct_qobuz,
            force: force_override || has_force_token(text),
            storefront: Some("qobuz".to_string()),
            document: false,
            from_reply: false,
            reply_sender_id: None,
            reply_sender_name: None,
            codec_preference: Some(CodecPreference::HighestQuality),
        });
    }

    // 2. Check replied message for Qobuz links
    if let Some(reply_text) = reply {
        let reply_qobuz = parse_qobuz_entities(reply_text);
        if !reply_qobuz.is_empty() {
            return Some(ParsedCommand {
                provider: engine::types::Provider::Qobuz,
                items: reply_qobuz,
                force: force_override || has_force_token(text),
                storefront: Some("qobuz".to_string()),
                document: false,
                from_reply: true,
                reply_sender_id: None,
                reply_sender_name: None,
                codec_preference: Some(CodecPreference::HighestQuality),
            });
        }
    }

    // 3. Fall back to Apple Music direct
    if let Some(direct) = parse_alac_input(text, None)
        && !direct.items.is_empty()
    {
        return Some(ParsedCommand {
            provider: engine::types::Provider::Apple,
            items: direct.items,
            force: force_override || direct.force,
            storefront: direct.storefront,
            document: false,
            from_reply: false,
            reply_sender_id: None,
            reply_sender_name: None,
            codec_preference: Some(CodecPreference::HighestQuality),
        });
    }

    // 4. Fall back to Apple Music reply
    if let Some(reply_text) = reply
        && let Some(parsed) = parse_alac_input(text, Some(reply_text))
    {
        return Some(ParsedCommand {
            provider: engine::types::Provider::Apple,
            items: parsed.items,
            force: force_override || parsed.force,
            storefront: parsed.storefront,
            document: false,
            from_reply: true,
            reply_sender_id: None,
            reply_sender_name: None,
            codec_preference: Some(CodecPreference::HighestQuality),
        });
    }
    None
}

fn temp_path() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("batch_{stamp}.txt"))
}

/// Parse a command, resolving a replied-to message and an attached `.txt`
/// document. Failed document downloads deliberately fall back to normal text,
/// just as the live bot does.
///
/// `chat_id` is the Bot-API marked chat id (negative for channels/supergroups).
/// It is used to prime the peer cache before fetching the reply message, so
/// `channels.getMessages` works even immediately after a fresh bot start.
pub async fn parse_message(
    client: &ferogram::Client,
    message: &IncomingMessage,
    chat_id: i64,
    force_override: bool,
) -> ParsedCommand {
    // Prime the peer cache for this chat so channels.getMessages has a valid
    // access_hash. On a cache hit this is a no-op (just a local map read);
    // on a cache miss (fresh start) it does one cheap RPC to fetch the chat.
    if message.reply_to_message_id().is_some()
        && let Err(e) = client.resolve(ferogram::PeerRef::Id(chat_id)).await
    {
        tracing::warn!(chat_id, error = %e, "get: could not prime peer cache for chat");
    }

    let reply = message.get_reply_with(client).await.ok().flatten();
    let (reply_sender_id, reply_sender_name) = match reply.as_ref() {
        Some(r) => {
            let uid = r.sender_user_id();
            let name = match r.sender_user().await {
                Ok(Some(u)) => u
                    .username()
                    .filter(|n| !n.trim().is_empty())
                    .map(|n| format!("@{n}"))
                    .or_else(|| {
                        let first = u.first_name().unwrap_or_default().trim();
                        match u.last_name().map(str::trim).filter(|l| !l.is_empty()) {
                            Some(last) if !first.is_empty() => Some(format!("{first} {last}")),
                            Some(last) => Some(last.to_owned()),
                            None if !first.is_empty() => Some(first.to_owned()),
                            None => None,
                        }
                    }),
                _ => None,
            };
            (uid, name)
        }
        None => (None, None),
    };

    let msg_doc = message.document();
    let reply_doc = reply.as_ref().and_then(IncomingMessage::document);
    let (document, doc_from_reply) = match (msg_doc, reply_doc) {
        (Some(doc), _) => (Some(doc), false),
        (None, Some(doc)) => (Some(doc), true),
        (None, None) => (None, false),
    };

    let text_str = message.text().unwrap_or_default();

    if let Some(document) = document {
        let name = document.file_name().unwrap_or("").to_ascii_lowercase();
        let mime = document.mime_type().to_ascii_lowercase();
        if name.ends_with(".txt") || mime == "text/plain" || mime.contains("text/plain") {
            let path = temp_path();
            let result: Result<String, String> = async {
                client
                    .download_file(&document, &path)
                    .await
                    .map_err(|e| e.to_string())?;
                let size = tokio::fs::metadata(&path)
                    .await
                    .map_err(|e| e.to_string())?
                    .len();
                if size > MAX_DOCUMENT_BYTES {
                    return Err(format!(
                        "text document exceeds the {} MiB limit",
                        MAX_DOCUMENT_BYTES / (1024 * 1024)
                    ));
                }
                let bytes = tokio::fs::read(&path).await.map_err(|e| e.to_string())?;
                let content = if let Ok(s) = std::str::from_utf8(&bytes) {
                    s.trim_start_matches('\u{FEFF}').to_owned()
                } else {
                    String::from_utf8_lossy(&bytes)
                        .trim_start_matches('\u{FEFF}')
                        .to_owned()
                };
                Ok(content)
            }
            .await;
            let _ = tokio::fs::remove_file(&path).await;
            if let Ok(content) = result {
                let apple_items = extract_batch_items(&content);
                let qobuz_items = parse_qobuz_entities(&content);

                let (provider, items, sf) = if !qobuz_items.is_empty() && apple_items.is_empty() {
                    (
                        engine::types::Provider::Qobuz,
                        qobuz_items,
                        Some("qobuz".to_string()),
                    )
                } else {
                    (engine::types::Provider::Apple, apple_items, None)
                };

                if !items.is_empty() {
                    return ParsedCommand {
                        provider,
                        items,
                        force: force_override || message.text().is_some_and(has_force_token),
                        storefront: sf,
                        document: true,
                        from_reply: doc_from_reply,
                        reply_sender_id: if doc_from_reply {
                            reply_sender_id
                        } else {
                            None
                        },
                        reply_sender_name: if doc_from_reply {
                            reply_sender_name
                        } else {
                            None
                        },
                        codec_preference: Some(CodecPreference::HighestQuality),
                    };
                }
            }
        }
    }

    let mut parsed = parse_text(
        text_str,
        reply.as_ref().and_then(IncomingMessage::text),
        force_override,
    )
    .unwrap_or(ParsedCommand {
        provider: engine::types::Provider::Apple,
        items: Vec::new(),
        force: force_override,
        storefront: None,
        document: false,
        from_reply: false,
        reply_sender_id: None,
        reply_sender_name: None,
        codec_preference: Some(CodecPreference::HighestQuality),
    });

    if parsed.from_reply {
        parsed.reply_sender_id = reply_sender_id;
        parsed.reply_sender_name = reply_sender_name;
    }

    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn force_token_is_recognized() {
        assert!(has_force_token("/get --force 1"));
    }

    #[test]
    fn parse_qobuz_url_routing() {
        let direct = parse_text(
            "/get https://play.qobuz.com/album/0060253786977",
            None,
            false,
        );
        assert!(direct.is_some());
        let cmd = direct.unwrap();
        assert_eq!(cmd.provider, engine::types::Provider::Qobuz);
        assert_eq!(cmd.items.len(), 1);
        assert_eq!(cmd.items[0].id, "0060253786977");
        assert_eq!(cmd.codec_preference, Some(CodecPreference::HighestQuality));

        let interpreter = parse_text(
            "/get https://www.qobuz.com/us-en/interpreter/billie-eilish/2867335",
            None,
            false,
        );
        assert!(interpreter.is_some());
        let cmd = interpreter.unwrap();
        assert_eq!(cmd.provider, engine::types::Provider::Qobuz);
        assert_eq!(cmd.items.len(), 1);
        assert_eq!(cmd.items[0].id, "2867335");
        assert_eq!(cmd.items[0].kind, engine::types::TargetKind::Artist);
    }

    #[test]
    fn parse_text_distinguishes_direct_from_reply() {
        let direct = parse_text(
            "/get https://music.apple.com/us/album/test/1440828878",
            None,
            false,
        );
        assert!(direct.is_some());
        let direct = direct.unwrap();
        assert_eq!(direct.provider, engine::types::Provider::Apple);
        assert_eq!(direct.items.len(), 1);
        assert_eq!(
            direct.codec_preference,
            Some(CodecPreference::HighestQuality)
        );
        assert!(!direct.from_reply);

        let reply = parse_text(
            "/get",
            Some("https://music.apple.com/us/album/test/1440828878"),
            false,
        );
        assert!(reply.is_some());
        let reply = reply.unwrap();
        assert_eq!(reply.provider, engine::types::Provider::Apple);
        assert_eq!(reply.items.len(), 1);
        assert_eq!(
            reply.codec_preference,
            Some(CodecPreference::HighestQuality)
        );
        assert!(reply.from_reply);

        let reply_force = parse_text(
            "/get -f",
            Some("https://music.apple.com/us/album/test/1440828878"),
            false,
        );
        assert!(reply_force.is_some());
        let reply_force = reply_force.unwrap();
        assert_eq!(reply_force.items.len(), 1);
        assert_eq!(
            reply_force.codec_preference,
            Some(CodecPreference::HighestQuality)
        );
        assert!(reply_force.from_reply);
        assert!(reply_force.force);
    }
}
