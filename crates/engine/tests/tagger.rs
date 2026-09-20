//! Filename policy tests.

use engine::{
    filename::{BoundedName, FilenameError, MAX_FILENAME_BYTES, TrackFilename},
    tagger::{build_track_filename, build_track_filename_with_codec},
    types::TrackMeta,
};

fn meta() -> TrackMeta {
    TrackMeta {
        id: "1".into(),
        title: "Song".into(),
        artist: "Artist".into(),
        album: "Album".into(),
        album_artist: "Artist".into(),
        genre: None,
        release_date: String::new(),
        composer: None,
        track_number: Some(3),
        track_count: Some(12),
        disc_number: Some(1),
        disc_count: Some(2),
        duration_secs: 100,
        explicit: false,
        content_advisory: None,
        artwork_url: String::new(),
        album_id: None,
        artist_id: None,
        isrc: None,
        record_label: None,
        copyright: None,
        upc: None,
        is_streamable: None,
    }
}

#[test]
fn sanitize_replaces_and_defaults() {
    let bounded = TrackFilename::sanitize_and_bound("a<b>c:d\"e/f\\g|h?i*j", None);
    assert_eq!(bounded.as_str(), "a_b_c_d_e_f_g_h_i_j");

    let empty = TrackFilename::sanitize_and_bound("   ", None);
    assert_eq!(empty.as_str(), "track");

    let trimmed = TrackFilename::sanitize_and_bound("  ok  ", None);
    assert_eq!(trimmed.as_str(), "ok");
}

#[test]
fn filename_explicit_and_number_padding() {
    let mut value = meta();
    value.explicit = true;
    assert_eq!(
        build_track_filename(&value).as_str(),
        "03. Song - Artist [E] [ALAC].m4a"
    );
    value.explicit = false;
    value.track_number = Some(0);
    assert_eq!(
        build_track_filename(&value).as_str(),
        "01. Song - Artist [ALAC].m4a"
    );
    value.track_number = Some(123);
    assert_eq!(
        build_track_filename(&value).as_str(),
        "123. Song - Artist [ALAC].m4a"
    );
}

#[test]
fn filename_labels_canonical_aac_primary_codec() {
    assert_eq!(
        build_track_filename_with_codec(&meta(), "aac").as_str(),
        "03. Song - Artist [AAC].m4a"
    );
}

#[test]
fn bounds_filename_at_utf8_boundary_without_losing_suffix() {
    let name = format!("{} [AAC].m4a", "é".repeat(200));
    let suffix = " [AAC].m4a";
    let bounded = TrackFilename::sanitize_and_bound(&name, Some(suffix));

    assert!(bounded.len() <= MAX_FILENAME_BYTES);
    assert!(bounded.ends_with(suffix));
}

#[test]
fn strict_validation_catches_invalid_or_oversized() {
    assert_eq!(
        BoundedName::<10>::try_new("bad/name").unwrap_err(),
        FilenameError::InvalidCharacter('/')
    );
    assert_eq!(
        BoundedName::<5>::try_new("toolong").unwrap_err(),
        FilenameError::TooLong { len: 7, max: 5 }
    );
}
