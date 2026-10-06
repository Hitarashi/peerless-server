pub mod acquisition;
pub mod catalog;
mod mirror_http;
pub mod mirror_policy;
pub mod parser;
pub mod playlist;
pub mod wrapper;

pub use acquisition::{
    AcquisitionOutcome, AppleAcquisitionConfig, ApplePresentation, AppleProduction,
    AppleProductionConfig, AppleRipperDeps, AppleStreamAcquisition, WrapperKind,
    map_acquisition_outcome,
};
pub use catalog::{
    Catalog, CatalogError, ReqwestTransport, SharedCatalog, Transport, TransportError,
};
pub use mirror_http::{MirrorHttp, MirrorHttpError, ReqwestMirrorHttp};
pub use mirror_policy::{
    MANIFEST_URL, MirrorEndpoint, MirrorError, MirrorPolicy, MirrorPolicyManager,
};
pub use music::{
    AlbumTracks, ArtistTracks, CodecPreference, ParsedAlacInput, ParsedTargetItem, PlaylistData,
    PlaylistTrack, Rendition, RenditionPolicy, TrackMeta,
};
pub use parser::{extract_batch_items, parse_alac_input, parse_single_item};
pub use playlist::{
    PlaylistClient, PlaylistError, PlaylistHttp, PlaylistHttpError, ReqwestPlaylistHttp,
};
pub use wrapper::{
    AlacStreamInfo, MediaPlaylistInfo, WrapperEngine, WrapperError, WrapperLiteClient,
    WrapperTrackOutcome, WrapperUnavailableReason, parse_master_playlist, parse_media_playlist,
};
