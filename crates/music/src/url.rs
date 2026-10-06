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
        assert_eq!(urlencode("é"), "%C3%A9");

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
        assert_eq!(urlencode("ÿ"), "%C3%BF");
    }
}
