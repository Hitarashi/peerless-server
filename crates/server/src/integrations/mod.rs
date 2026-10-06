pub mod lastfm;
pub mod listenbrainz;

pub use lastfm::{
    __path_lastfm_disconnect, __path_lastfm_login, __path_lastfm_status, LastfmLoginRequest,
    LastfmStatusResponse, lastfm_credentials, lastfm_disconnect, lastfm_login, lastfm_router,
    lastfm_status,
};
pub use listenbrainz::{
    __path_listenbrainz_disconnect, __path_listenbrainz_login, __path_listenbrainz_status,
    ListenbrainzLoginRequest, ListenbrainzStatusResponse, listenbrainz_disconnect,
    listenbrainz_login, listenbrainz_router, listenbrainz_status,
};
