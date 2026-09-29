//! YouTube Music description adapter.

#[cfg(feature = "youtube")]
use super::shared::{RecordingMetadata, lrc_candidate, metadata_score};
#[cfg(feature = "youtube")]
use crate::{LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsSource};

#[cfg(feature = "youtube")]
#[derive(Debug, Default)]
pub struct YouTubeMusic;

#[cfg(feature = "youtube")]
impl LyricsSource for YouTubeMusic {
    fn id(&self) -> &str {
        "youtube-music"
    }

    fn lookup<'a>(
        &'a self,
        _http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            use ytmusic::{Client, YtMusic, nav::Nav as _, parse::find_renderers};

            let client = YtMusic::anonymous();
            let query = format!("{} {}", input.title, input.artist_string());
            let mut tracks = match client.search_songs(&query).await {
                Ok(tracks) => tracks,
                Err(_) => return Vec::new(),
            };
            if let Some(id) = input.provider_ids.get("youtube") {
                tracks.sort_by_key(|track| track.video_id.as_deref() != Some(id));
            }
            tracks.retain(|track| {
                let artist = track
                    .artists
                    .iter()
                    .map(|artist| artist.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let album = track.album.as_ref().map(|album| album.name.as_str());
                let duration = track.duration.map(|duration| duration.as_secs());
                metadata_score(input, &track.title, &artist, album, duration).is_some()
                    || input
                        .provider_ids
                        .get("youtube")
                        .is_some_and(|id| track.video_id.as_deref() == Some(id.as_str()))
            });
            tracks.truncate(3);
            for track in tracks {
                let Some(id) = track.video_id.as_deref() else {
                    continue;
                };
                let next = match client
                    .execute(
                        "next",
                        Client::Music,
                        serde_json::json!({
                            "videoId": id,
                            "playlistId": format!("RDAMVM{id}"),
                            "enablePersistentPlaylistPanel": true,
                        }),
                    )
                    .await
                {
                    Ok(next) => next,
                    Err(_) => continue,
                };
                let browse = find_renderers(&next, "browseEndpoint")
                    .into_iter()
                    .find_map(|endpoint| {
                        let kind = endpoint.str_at(&[
                            "browseEndpointContextSupportedConfigs",
                            "browseEndpointContextMusicConfig",
                            "pageType",
                        ]);
                        let browse_id = endpoint.str_at(&["browseId"])?;
                        (kind == Some("MUSIC_PAGE_TYPE_TRACK_LYRICS")
                            || browse_id.starts_with("MPLYt"))
                        .then(|| browse_id.to_owned())
                    });
                let Some(browse) = browse else {
                    continue;
                };
                let Ok(response) = client
                    .execute(
                        "browse",
                        Client::Music,
                        serde_json::json!({"browseId": browse}),
                    )
                    .await
                else {
                    continue;
                };
                let Some(text) = find_renderers(&response, "musicDescriptionShelfRenderer")
                    .into_iter()
                    .filter_map(|shelf| shelf.run_text(&["description"]))
                    .find(|text| !text.trim().is_empty())
                else {
                    continue;
                };
                let artist = track
                    .artists
                    .iter()
                    .map(|artist| artist.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let album = track.album.as_ref().map(|album| album.name.as_str());
                let duration = track.duration.map(|duration| duration.as_secs());
                let Some(mut candidate) = lrc_candidate(
                    &text,
                    "YouTube Music Description",
                    &format!("youtube:{id}"),
                    &format!("https://music.youtube.com/watch?v={id}"),
                    input,
                    RecordingMetadata {
                        title: &track.title,
                        artist: &artist,
                        album,
                        duration,
                    },
                ) else {
                    continue;
                };
                candidate.ranking.score += 5;
                return vec![candidate];
            }
            Vec::new()
        })
    }
}
