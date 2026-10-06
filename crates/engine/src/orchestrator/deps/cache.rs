use music::{Codec, Provider};
pub use peerless_core::cache::{
    AlbumCacheError, AlbumCacheOperation, AlbumReplacementExpectation, AlbumReplacementResult,
    AlbumUpload, CachedAlbum, CachedTrack, CachedTracksMap, SaveTrackInput, TrackCacheError,
    TrackCacheOperation,
};

use super::BoxFuture;

pub trait TrackCache: Send + Sync {
    fn find_cached_tracks<'a>(
        &'a self,
        track_ids: &'a [String],
    ) -> BoxFuture<'a, Result<CachedTracksMap, TrackCacheError>>;

    fn save_track<'a>(
        &'a self,
        input: SaveTrackInput,
    ) -> BoxFuture<'a, Result<(), TrackCacheError>>;

    fn delete_track<'a>(
        &'a self,
        track_id: &'a str,
        codec: Option<Codec>,
    ) -> BoxFuture<'a, Result<bool, TrackCacheError>>;
}

pub trait AlbumCache: Send + Sync {
    fn save_album<'a>(&'a self, upload: AlbumUpload) -> BoxFuture<'a, Result<(), AlbumCacheError>>;

    fn replace_albums<'a>(
        &'a self,
        provider: Provider,
        album_id: &'a str,
        codec: Codec,
        expected: AlbumReplacementExpectation,
        uploads: Vec<AlbumUpload>,
    ) -> BoxFuture<'a, Result<AlbumReplacementResult, AlbumCacheError>>;

    fn find_albums<'a>(
        &'a self,
        provider: Provider,
        album_id: &'a str,
        codec: Option<Codec>,
    ) -> BoxFuture<'a, Result<Vec<CachedAlbum>, AlbumCacheError>>;

    fn delete_albums<'a>(
        &'a self,
        provider: Provider,
        album_id: &'a str,
        codec: Option<Codec>,
    ) -> BoxFuture<'a, Result<(), AlbumCacheError>>;
}
