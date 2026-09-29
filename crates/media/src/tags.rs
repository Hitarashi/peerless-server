//! Container tagging for FLAC, MP3, and M4A.
//!
//! Tag writers are reached from `finalize_m4a_sync`, which runs under
//! `spawn_blocking`, so every function here is allowed to touch the
//! filesystem. `mp4ameta` and `id3` details never leave this module.

use std::path::Path;

use id3::TagLike;
use tokio_util::sync::CancellationToken;

use crate::{
    AdvisoryKind, MediaError, MediaKind, TrackTags, ValidatedM4a, decode::inspect_sync,
    mark_source_validation,
};

fn tag_flac(path: &Path, tags: &TrackTags) -> Result<(), MediaError> {
    let mut tag = metaflac::Tag::read_from_path(path)
        .map_err(|error| MediaError::Invalid(error.to_string()))?;
    let comments = tag.vorbis_comments_mut();
    if let Some(value) = non_empty(tags.title.as_deref()) {
        comments.set_title(vec![value]);
    }
    if let Some(value) = non_empty(tags.artist.as_deref()) {
        comments.set_artist(vec![value]);
    }
    if let Some(value) = non_empty(tags.album.as_deref()) {
        comments.set_album(vec![value]);
    }
    if let Some(value) = non_empty(tags.album_artist.as_deref()) {
        comments.set_album_artist(vec![value]);
    }
    if let Some(value) = non_empty(tags.release_date.as_deref()) {
        comments.set("DATE", vec![value]);
    }
    if let Some(value) = non_empty(tags.genre.as_deref()) {
        comments.set_genre(vec![value]);
    }
    if let Some(value) = non_empty(tags.composer.as_deref()) {
        comments.set("COMPOSER", vec![value]);
    }
    if let Some(value) = tags.track_number {
        comments.set_track(value as u32);
    }
    if let Some(value) = tags.track_count {
        comments.set_total_tracks(value as u32);
    }
    if let Some(value) = tags.disc_number {
        comments.set("DISCNUMBER", vec![value.to_string()]);
    }
    if let Some(value) = tags.disc_count {
        comments.set("DISCTOTAL", vec![value.to_string()]);
    }
    if let Some(value) = non_empty(tags.lyrics.as_deref()) {
        comments.set_lyrics(vec![value]);
    }
    if let Some(value) = non_empty(tags.isrc.as_deref()) {
        comments.set("ISRC", vec![value]);
    }
    if let Some(value) = non_empty(tags.copyright.as_deref()) {
        comments.set("COPYRIGHT", vec![value]);
    }
    if let Some(image) = tags
        .artwork_jpeg
        .as_deref()
        .filter(|image| !image.is_empty())
    {
        let mime_type = if image.starts_with(b"\x89PNG\r\n\x1a\n") {
            "image/png"
        } else {
            "image/jpeg"
        };
        tag.add_picture(
            mime_type,
            metaflac::block::PictureType::CoverFront,
            image.to_vec(),
        );
    }
    tag.save()
        .map_err(|error| MediaError::Metadata(error.to_string()))?;
    Ok(())
}

fn tag_mp3(path: &Path, tags: &TrackTags) -> Result<(), MediaError> {
    let mut tag = id3::Tag::read_from_path(path).unwrap_or_default();
    if let Some(value) = non_empty(tags.title.as_deref()) {
        tag.set_title(value);
    }
    if let Some(value) = non_empty(tags.artist.as_deref()) {
        tag.set_artist(value);
    }
    if let Some(value) = non_empty(tags.album.as_deref()) {
        tag.set_album(value);
    }
    if let Some(value) = non_empty(tags.album_artist.as_deref()) {
        tag.set_album_artist(value);
    }
    if let Some(value) = non_empty(tags.genre.as_deref()) {
        tag.set_genre(value);
    }
    if let Some(value) = tags.track_number {
        tag.set_track(value as u32);
    }
    if let Some(value) = tags.track_count {
        tag.set_total_tracks(value as u32);
    }
    if let Some(value) = tags.disc_number {
        tag.set_disc(value as u32);
    }
    if let Some(value) = tags.disc_count {
        tag.set_total_discs(value as u32);
    }
    if let Some(image) = tags
        .artwork_jpeg
        .as_deref()
        .filter(|image| !image.is_empty())
    {
        let mime_type = if image.starts_with(b"\x89PNG\r\n\x1a\n") {
            "image/png".to_string()
        } else {
            "image/jpeg".to_string()
        };
        tag.add_frame(id3::frame::Picture {
            mime_type,
            picture_type: id3::frame::PictureType::CoverFront,
            description: String::new(),
            data: image.to_vec(),
        });
    }
    tag.write_to_path(path, id3::Version::Id3v24)
        .map_err(|error| MediaError::Metadata(error.to_string()))?;
    Ok(())
}

fn tag_m4a(path: &Path, tags: &TrackTags) -> Result<(), MediaError> {
    let mut tag = mp4ameta::Tag::read_from_path(path)
        .map_err(|error| MediaError::Invalid(error.to_string()))?;
    if let Some(value) = non_empty(tags.title.as_deref()) {
        tag.set_title(value);
    }
    if let Some(value) = non_empty(tags.title_sort.as_deref()) {
        tag.set_title_sort_order(value);
    }
    if let Some(value) = non_empty(tags.artist.as_deref()) {
        tag.set_artist(value);
    }
    if let Some(value) = non_empty(tags.artist_sort.as_deref()) {
        tag.set_artist_sort_order(value);
    }
    if let Some(value) = non_empty(tags.album.as_deref()) {
        tag.set_album(value);
    }
    if let Some(value) = non_empty(tags.album_sort.as_deref()) {
        tag.set_album_sort_order(value);
    }
    if let Some(value) = non_empty(tags.album_artist.as_deref()) {
        tag.set_album_artist(value);
    }
    if let Some(value) = non_empty(tags.album_artist_sort.as_deref()) {
        tag.set_album_artist_sort_order(value);
    }
    if let Some(value) = non_empty(tags.release_date.as_deref()) {
        tag.set_year(value);
    }
    if let Some(value) = non_empty(tags.genre.as_deref()) {
        tag.set_genre(value);
    }
    if let Some(value) = non_empty(tags.composer.as_deref()) {
        tag.set_composer(value);
    }
    if let Some(value) = non_empty(tags.composer_sort.as_deref()) {
        tag.set_composer_sort_order(value);
    }
    if let Some(value) = tags.track_number {
        tag.set_track(value, tags.track_count.unwrap_or(0));
    }
    if let Some(value) = tags.disc_number {
        tag.set_disc(value, tags.disc_count.unwrap_or(0));
    }
    if let Some(value) = non_empty(tags.lyrics.as_deref()) {
        tag.set_lyrics(value);
    }
    if let Some(image) = tags
        .artwork_jpeg
        .as_deref()
        .filter(|image| !image.is_empty())
    {
        if image.starts_with(b"\x89PNG\r\n\x1a\n") {
            tag.set_artwork(mp4ameta::Img::png(image));
        } else {
            tag.set_artwork(mp4ameta::Img::jpeg(image));
        }
    }
    write_extended_metadata(&mut tag, tags);
    tag.write_to_path(path)
        .map_err(|error| MediaError::Metadata(error.to_string()))?;
    Ok(())
}

pub(super) fn finalize_m4a_sync(
    source: &Path,
    destination: &Path,
    tags: &TrackTags,
    cancellation: &CancellationToken,
) -> Result<ValidatedM4a, MediaError> {
    if cancellation.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    let info = inspect_sync(source, cancellation).map_err(mark_source_validation)?;

    let ext = destination
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("m4a");
    let part = destination.with_extension(format!("{ext}.part"));
    let part = match part.file_name().and_then(|value| value.to_str()) {
        Some(name) if name.len() <= 255 => part,
        _ => destination.with_extension("p"),
    };
    if part.exists() {
        std::fs::remove_file(&part)?;
    }
    std::fs::copy(source, &part)?;
    let result = (|| {
        if cancellation.is_cancelled() {
            return Err(MediaError::Cancelled);
        }

        let is_flac = ext == "flac" || info.codec == "flac";
        let is_mp3 = ext == "mp3" || info.codec == "mp3";

        if is_flac {
            tag_flac(&part, tags)?;
        } else if is_mp3 {
            tag_mp3(&part, tags)?;
        } else {
            tag_m4a(&part, tags)?;
        }

        if cancellation.is_cancelled() {
            return Err(MediaError::Cancelled);
        }
        let validated = inspect_sync(&part, cancellation)?;
        if validated.codec != info.codec {
            return Err(MediaError::Invalid(format!(
                "finalized file codec changed: expected {}, found {}",
                info.codec, validated.codec
            )));
        }
        if validated.duration_secs == 0.0 {
            let size_on_disk = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
            return Err(MediaError::Invalid(format!(
                "finalized file has no duration: {size_on_disk} bytes on disk, re-inspect read \
                 codec={} rate={} ch={} bit_depth={:?} (expected codec={} rate={} ch={})",
                validated.codec,
                validated.sample_rate,
                validated.channels,
                validated.bit_depth,
                info.codec,
                info.sample_rate,
                info.channels,
            )));
        }
        std::fs::rename(&part, destination)?;
        Ok(ValidatedM4a {
            path: destination.to_owned(),
            info: validated,
        })
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&part);
    }
    result
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// Write provider and Apple-specific metadata while keeping `mp4ameta` out of
/// the public media interface. Optional values are omitted rather than writing
/// empty atoms, which keeps files compact and avoids misleading player output.
fn write_extended_metadata(tag: &mut mp4ameta::Tag, tags: &TrackTags) {
    if let Some(value) = non_empty(tags.isrc.as_deref()) {
        tag.set_isrc(value);
        set_freeform_text(tag, "ISRC", value);
    }
    if let Some(value) = non_empty(tags.label.as_deref()) {
        tag.set_label(value);
        set_freeform_text(tag, "LABEL", value);
    }
    if let Some(value) = non_empty(tags.copyright.as_deref()) {
        tag.set_copyright(value);
    }
    if let Some(value) = non_empty(tags.publisher.as_deref()) {
        set_freeform_text(tag, "PUBLISHER", value);
    }
    if let Some(value) = non_empty(tags.performer.as_deref()) {
        set_freeform_text(tag, "PERFORMER", value);
    }
    if let Some(value) = non_empty(tags.release_time.as_deref()) {
        set_freeform_text(tag, "RELEASETIME", value);
    }
    if let Some(value) = non_empty(tags.upc.as_deref()) {
        set_freeform_text(tag, "UPC", value);
    }

    if let Some(value) = tags.song_id.and_then(to_u32) {
        set_numeric_atom(tag, *b"cnID", value);
    }
    if let Some(value) = tags.album_id.and_then(to_u32) {
        set_numeric_atom(tag, *b"plID", value);
    }
    if let Some(value) = tags.artist_id.and_then(to_u32) {
        set_numeric_atom(tag, *b"atID", value);
    }
    if let Some(value) = tags.genre_id {
        set_numeric_atom(tag, *b"geID", value);
    }
    if let Some(value) = tags.storefront_id {
        set_numeric_atom(tag, *b"sfID", value);
    }

    if tags.compilation == Some(true) {
        tag.set_data(
            mp4ameta::ident::COMPILATION,
            mp4ameta::Data::BeSigned(vec![1]),
        );
    }
    if tags.gapless == Some(true) {
        tag.set_data(
            mp4ameta::Fourcc(*b"pgap"),
            mp4ameta::Data::BeSigned(vec![1]),
        );
    }
    if let Some(value) = non_empty(tags.encoder.as_deref()) {
        tag.set_data(
            mp4ameta::Fourcc(*b"\xa9too"),
            mp4ameta::Data::Utf8(value.to_owned()),
        );
        tag.set_data(
            mp4ameta::ident::ENCODER,
            mp4ameta::Data::Utf8(value.to_owned()),
        );
    }
    if let Some(value) = non_empty(tags.comment.as_deref()) {
        tag.set_comment(value);
    }
    if let Some(value) = non_empty(tags.description.as_deref()) {
        tag.set_description(value);
    }

    if let Some(advisory) = tags.advisory.or_else(|| {
        tags.explicit.map(|explicit| {
            if explicit {
                AdvisoryKind::Explicit
            } else {
                AdvisoryKind::Inoffensive
            }
        })
    }) {
        let rating = match advisory {
            AdvisoryKind::Explicit => mp4ameta::AdvisoryRating::Explicit,
            AdvisoryKind::Clean => mp4ameta::AdvisoryRating::Clean,
            AdvisoryKind::Inoffensive => mp4ameta::AdvisoryRating::Inoffensive,
        };
        tag.set_data(
            mp4ameta::ident::ADVISORY_RATING,
            mp4ameta::Data::BeSigned(vec![rating.code()]),
        );
    }
    if let Some(kind) = tags.media_kind {
        let media_type = match kind {
            MediaKind::Music => mp4ameta::MediaType::Normal,
            MediaKind::Audiobook => mp4ameta::MediaType::AudioBook,
            MediaKind::MusicVideo => mp4ameta::MediaType::MusicVideo,
            MediaKind::Movie => mp4ameta::MediaType::Movie,
        };
        tag.set_data(
            mp4ameta::ident::MEDIA_TYPE,
            mp4ameta::Data::BeSigned(vec![media_type.code()]),
        );
    }
}

fn to_u32(value: u64) -> Option<u32> {
    u32::try_from(value).ok()
}

fn set_numeric_atom(tag: &mut mp4ameta::Tag, atom: [u8; 4], value: u32) {
    tag.set_data(
        mp4ameta::Fourcc(atom),
        mp4ameta::Data::BeSigned(value.to_be_bytes().to_vec()),
    );
}

fn set_freeform_text(tag: &mut mp4ameta::Tag, name: &'static str, value: &str) {
    let ident = mp4ameta::FreeformIdent::new_static("com.apple.iTunes", name);
    tag.set_data(ident, mp4ameta::Data::Utf8(value.to_owned()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extended_metadata_maps_to_itunes_atoms() {
        let mut tag = mp4ameta::Tag::default();
        let tags = TrackTags {
            title: Some("Title".into()),
            title_sort: Some("Title, The".into()),
            artist: Some("Artist".into()),
            artist_sort: Some("Artist, The".into()),
            album: Some("Album".into()),
            album_sort: Some("Album, The".into()),
            album_artist: Some("Album Artist".into()),
            album_artist_sort: Some("Artist, Album".into()),
            composer: Some("Composer".into()),
            composer_sort: Some("Composer, The".into()),
            isrc: Some("US-AAA-24-00001".into()),
            label: Some("Example Records".into()),
            copyright: Some("2024 Example Records".into()),
            publisher: Some("Example Publishing".into()),
            performer: Some("Featured Performer".into()),
            release_time: Some("2024-01-02T03:04:05Z".into()),
            upc: Some("123456789012".into()),
            song_id: Some(42),
            album_id: Some(43),
            artist_id: Some(44),
            explicit: Some(true),
            media_kind: Some(MediaKind::Music),
            ..TrackTags::default()
        };

        if let Some(value) = tags.title.as_deref() {
            tag.set_title(value);
        }
        if let Some(value) = non_empty(tags.title_sort.as_deref()) {
            tag.set_title_sort_order(value);
        }
        write_extended_metadata(&mut tag, &tags);

        assert_eq!(tag.title(), Some("Title"));
        assert_eq!(tag.title_sort_order(), Some("Title, The"));
        assert_eq!(tag.isrc(), Some("US-AAA-24-00001"));
        assert_eq!(tag.label(), Some("Example Records"));
        assert_eq!(tag.copyright(), Some("2024 Example Records"));
        assert_eq!(tag.media_type(), Some(mp4ameta::MediaType::Normal));
        assert_eq!(
            tag.advisory_rating(),
            Some(mp4ameta::AdvisoryRating::Explicit)
        );
        assert!(matches!(
            tag.data_of(&mp4ameta::ident::ADVISORY_RATING).next(),
            Some(mp4ameta::Data::BeSigned(bytes)) if bytes == &[4]
        ));
        assert!(matches!(
            tag.data_of(&mp4ameta::ident::MEDIA_TYPE).next(),
            Some(mp4ameta::Data::BeSigned(bytes)) if bytes == &[1]
        ));
        assert_eq!(
            tag.bytes_of(&mp4ameta::Fourcc(*b"cnID")).next(),
            Some(&42u32.to_be_bytes()[..])
        );
        let publisher = mp4ameta::FreeformIdent::new_borrowed("com.apple.iTunes", "PUBLISHER");
        assert_eq!(
            tag.strings_of(&publisher).next(),
            Some("Example Publishing")
        );
        let performer = mp4ameta::FreeformIdent::new_borrowed("com.apple.iTunes", "PERFORMER");
        assert_eq!(
            tag.strings_of(&performer).next(),
            Some("Featured Performer")
        );
        let isrc_freeform = mp4ameta::FreeformIdent::new_borrowed("com.apple.iTunes", "ISRC");
        assert_eq!(
            tag.strings_of(&isrc_freeform).next(),
            Some("US-AAA-24-00001")
        );
        let label_freeform = mp4ameta::FreeformIdent::new_borrowed("com.apple.iTunes", "LABEL");
        assert_eq!(
            tag.strings_of(&label_freeform).next(),
            Some("Example Records")
        );
    }
}
