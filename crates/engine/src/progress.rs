//! Progress formatting helpers.

/// Render a Unicode block progress bar using `■`, `▤`, `□`,
/// e.g. `[■■■■■■□□□□□□] 50%` or `[■■■▤□□□□□□□□] 35%`.
pub fn render_progress_bar(current: u64, total: u64, length: usize) -> String {
    if total == 0 {
        return format!("[{}] 0%", "□".repeat(length));
    }
    let fraction = (current as f64 / total as f64).clamp(0.0, 1.0);
    let units = (fraction * (2.0 * length as f64)).round() as usize;
    let full_count = (units / 2).min(length);
    let half_count = if units % 2 == 1 && full_count < length {
        1
    } else {
        0
    };
    let empty_count = length.saturating_sub(full_count + half_count);
    let half_str = if half_count > 0 { "▤" } else { "" };
    let bar = format!(
        "{}{}{}",
        "■".repeat(full_count),
        half_str,
        "□".repeat(empty_count)
    );
    let percent = (fraction * 100.0).round() as u64;
    format!("[{bar}] {percent}%")
}

/// Separator between a byte count and its unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteStyle {
    /// `565.53MB` — no space. For dense progress text where the bar and the
    /// number compete for horizontal room.
    Compact,
    /// `565.53 MB` — space. For prose messages where the value stands alone as
    /// a labelled quantity ("Space reclaimed: 565.53 MB").
    Spaced,
}

/// Format a byte count with 1024-based units (B, KB, MB, GB, TB, PB).
///
/// The single workspace byte formatter; the bot crate's `/clean` summary and
/// `readable_file_size` both route here. Whole bytes render as an integer
/// (`1023B`); larger units always carry two decimals.
pub fn format_bytes_with(bytes: u64, style: ByteStyle) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];

    let mut value = bytes as f64;
    let mut index = 0;
    while value >= 1024.0 && index < UNITS.len() - 1 {
        value /= 1024.0;
        index += 1;
    }

    let separator = match style {
        ByteStyle::Compact => "",
        ByteStyle::Spaced => " ",
    };
    if index == 0 {
        // Print the integer, not the f64, so a byte count is never rendered
        // with a spurious decimal.
        format!("{bytes}{separator}B")
    } else {
        format!("{value:.2}{separator}{}", UNITS[index])
    }
}

/// Format bytes into human-readable B, KB, MB, GB, TB, PB string.
pub fn format_bytes(bytes: u64) -> String {
    format_bytes_with(bytes, ByteStyle::Compact)
}

/// Format MB progress as `x.x/y.y MB` (or `x.x MB` when total is unknown).
pub fn format_mb_progress(current_bytes: u64, total_bytes: u64) -> String {
    let current_mb = current_bytes as f64 / (1024.0 * 1024.0);
    if total_bytes > 0 {
        let total_mb = total_bytes as f64 / (1024.0 * 1024.0);
        format!("{current_mb:.1}/{total_mb:.1} MB")
    } else {
        format!("{current_mb:.1} MB")
    }
}

/// Format byte progress with bar: `[■■■■■■□□□□□□] 50% (14.5/29.0 MB)`.
pub fn format_byte_progress(current_bytes: u64, total_bytes: u64, bar_length: usize) -> String {
    let bar = render_progress_bar(current_bytes, total_bytes, bar_length);
    let current_mb = current_bytes as f64 / (1024.0 * 1024.0);
    if total_bytes > 0 {
        let total_mb = total_bytes as f64 / (1024.0 * 1024.0);
        format!("{bar} ({current_mb:.1}/{total_mb:.1} MB)")
    } else {
        format!("{current_mb:.1} MB")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_bytes_examples() {
        assert_eq!(format_bytes(0), "0B");
        assert_eq!(format_bytes(1024), "1.00KB");
        assert_eq!(format_bytes(593_000_000), "565.53MB");
        assert_eq!(format_bytes(8_799_493_473), "8.20GB");
    }

    #[test]
    fn format_bytes_covers_every_unit_boundary() {
        assert_eq!(format_bytes(1), "1B");
        assert_eq!(format_bytes(1023), "1023B");
        assert_eq!(format_bytes(1_048_576), "1.00MB");
        assert_eq!(format_bytes(1_073_741_824), "1.00GB");
        assert_eq!(format_bytes(1_099_511_627_776), "1.00TB");
        assert_eq!(format_bytes(1_125_899_906_842_624), "1.00PB");
    }

    #[test]
    fn spaced_style_only_adds_the_separator() {
        assert_eq!(format_bytes_with(0, ByteStyle::Spaced), "0 B");
        assert_eq!(
            format_bytes_with(593_000_000, ByteStyle::Spaced),
            "565.53 MB"
        );
        assert_eq!(
            format_bytes_with(8_799_493_473, ByteStyle::Spaced),
            "8.20 GB"
        );
        // Same numbers, same units — the styles differ only by the space.
        for bytes in [0, 1, 1023, 1024, 593_000_000, 8_799_493_473] {
            assert_eq!(
                format_bytes_with(bytes, ByteStyle::Spaced).replace(' ', ""),
                format_bytes(bytes)
            );
        }
    }
}
