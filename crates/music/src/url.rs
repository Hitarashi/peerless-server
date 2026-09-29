//! URL percent-encoding shared by every crate that builds a request URL.

/// Percent-encode a value for use as a single URL component (a path segment
/// or a query-string value).
///
/// Unreserved characters (`A-Z a-z 0-9 - _ . ~`) pass through; every other
/// **byte** becomes `%XX` with uppercase hex. Encoding per byte is what makes
/// non-ASCII text correct: a multi-byte UTF-8 character is emitted as its
/// sequence of `%XX` escapes, which is exactly what a browser or server
/// decodes back to the original text.
///
/// A space becomes `%20`, never `+`. `+` only means "space" inside an
/// `application/x-www-form-urlencoded` body; in a path or a query value it is
/// a literal plus sign, so encoding a space as `+` there silently corrupts
/// values that contain one. `%20` is correct in both positions, so this is the
/// single encoder for the whole workspace.
///
/// The escaped set matches JS `encodeURIComponent` (RFC 3986 unreserved set),
/// which is what the Apple catalog endpoints expect.
pub fn urlencode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn space_is_percent_twenty_not_plus() {
        assert_eq!(urlencode("a b"), "a%20b");
        assert_eq!(urlencode("Jay-Z & 4eva"), "Jay-Z%20%26%204eva");
    }

    #[test]
    fn plus_is_escaped_so_it_survives_a_round_trip() {
        // A literal `+` must not be able to masquerade as a space.
        assert_eq!(urlencode("a+b"), "a%2Bb");
        assert_eq!(urlencode("+"), "%2B");
    }

    #[test]
    fn reserved_path_and_query_characters_are_escaped() {
        assert_eq!(urlencode("a/b"), "a%2Fb");
        assert_eq!(urlencode("a?b&c=d#e"), "a%3Fb%26c%3Dd%23e");
    }

    #[test]
    fn non_ascii_is_encoded_per_utf8_byte() {
        // U+00E9 is 0xC3 0xA9 in UTF-8.
        assert_eq!(urlencode("é"), "%C3%A9");
        // U+1F3B5 MUSICAL NOTE is 0xF0 0x9F 0x8E 0xB5.
        assert_eq!(urlencode("🎵"), "%F0%9F%8E%B5");
    }

    #[test]
    fn unreserved_characters_pass_through_unchanged() {
        let unreserved = "AZaz09-_.~";
        assert_eq!(urlencode(unreserved), unreserved);
        assert_eq!(urlencode(""), "");
    }

    #[test]
    fn hex_digits_are_uppercase() {
        // 'ÿ' is 0xC3 0xBF in UTF-8, so it exercises the hex letters B and F.
        assert_eq!(urlencode("ÿ"), "%C3%BF");
    }
}
