use std::{io, path::Path, sync::Arc};

use ferogram::{InputMessage, filters, filters::Dispatcher};

use crate::{
    BotState,
    html::{escape, parse_dynamic_html},
};

const RESTRICTED: &str =
    "🔒 <b>Access Restricted:</b> This command is restricted to the bot owner.";

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".to_owned();
    }
    let units = ["B", "KB", "MB", "GB", "TB"];
    let index = ((bytes as f64).ln() / 1024_f64.ln()).floor() as usize;
    let index = index.min(units.len() - 1);
    let value = bytes as f64 / 1024_f64.powi(index as i32);
    if index == 0 {
        format!("{value:.0} {}", units[index])
    } else {
        format!("{value:.2} {}", units[index])
    }
}

/// Remove the contents of the bot's downloads directory without following
/// symlinks or deleting its repository marker files.  The caller supplies the
/// already-authorized downloads root; this helper never derives a parent or a
/// system temporary directory from it.
fn clean_downloads_dir(downloads_dir: &Path) -> io::Result<(usize, u64)> {
    let root_type = std::fs::symlink_metadata(downloads_dir)?.file_type();
    if !root_type.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "downloads path must be a real directory",
        ));
    }
    // In production this prevents `bot-data -> /tmp/...` from turning the
    // fixed relative target into an arbitrary cleanup root.
    if let Some(parent) = downloads_dir.parent()
        && std::fs::symlink_metadata(parent)?.file_type().is_symlink()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "downloads parent must not be a symlink",
        ));
    }

    fn clean_entry(path: &Path) -> io::Result<(usize, u64)> {
        let file_type = std::fs::symlink_metadata(path)?.file_type();
        if file_type.is_dir() {
            let mut files_removed = 0;
            let mut bytes_freed = 0;
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let name = entry.file_name();
                if name == ".gitkeep" || name == ".gitignore" {
                    continue;
                }
                let (files, bytes) = clean_entry(&entry.path())?;
                files_removed += files;
                bytes_freed += bytes;
            }

            // A directory containing a preserved marker is intentionally left
            // in place.  Other removal errors still surface to the command.
            match std::fs::remove_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => {}
                Err(error) => return Err(error),
            }
            Ok((files_removed, bytes_freed))
        } else {
            let bytes = std::fs::symlink_metadata(path)?.len();
            std::fs::remove_file(path)?;
            Ok((1, bytes))
        }
    }

    let mut files_removed = 0;
    let mut bytes_freed = 0;
    for entry in std::fs::read_dir(downloads_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".gitkeep" || name == ".gitignore" {
            continue;
        }
        let (files, bytes) = clean_entry(&entry.path())?;
        files_removed += files;
        bytes_freed += bytes;
    }
    Ok((files_removed, bytes_freed))
}

async fn clean(msg: ferogram::update::IncomingMessage, state: Arc<BotState>) {
    let sender = msg.sender_user_id().unwrap_or_default();
    if !state.auth.is_admin(sender) {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(RESTRICTED)))
            .await;
        return;
    }

    let downloads_dir = Path::new("bot-data/downloads");
    if !downloads_dir.exists() {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(
                "✨ <b>Clean:</b> Downloads directory is empty.",
            )))
            .await;
        return;
    }

    let result = clean_downloads_dir(downloads_dir);

    match result {
        Ok((files_removed, bytes_freed)) => {
            let text = format!(
                "✓ <b>Temporary storage cleaned</b><br/><br/><blockquote>• Files removed: <code>{files_removed}</code><br/>• Space reclaimed: <code>{}</code><br/>• Target: <code>bot-data/downloads/</code></blockquote>",
                format_bytes(bytes_freed)
            );
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(&text)))
                .await;
        }
        Err(error) => {
            let text = format!(
                "! <b>Could not clean temporary storage.</b><br/><code>{}</code>",
                escape(&error.to_string())
            );
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(&text)))
                .await;
        }
    }
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("clean"), move |msg| {
        clean(msg, Arc::clone(&state))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_downloads_recursively_removes_stale_entries_and_preserves_markers() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(".gitkeep"), b"").unwrap();
        std::fs::write(root.path().join(".gitignore"), b"*").unwrap();
        std::fs::create_dir_all(root.path().join("nested/deeper")).unwrap();
        std::fs::write(root.path().join("nested/file.m4a"), b"audio").unwrap();
        std::fs::write(root.path().join("nested/deeper/.track_raw"), b"raw").unwrap();

        let (files_removed, bytes_freed) = clean_downloads_dir(root.path()).unwrap();

        assert_eq!(files_removed, 2);
        assert_eq!(bytes_freed, 8);
        assert!(root.path().join(".gitkeep").exists());
        assert!(root.path().join(".gitignore").exists());
        assert!(!root.path().join("nested").exists());
    }

    #[cfg(unix)]
    #[test]
    fn clean_does_not_follow_child_symlinks() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("must-survive");
        std::fs::write(&outside_file, b"outside").unwrap();
        symlink(outside.path(), root.path().join("linked-dir")).unwrap();

        clean_downloads_dir(root.path()).unwrap();

        assert!(outside_file.exists());
        assert!(!root.path().join("linked-dir").exists());
    }
}
