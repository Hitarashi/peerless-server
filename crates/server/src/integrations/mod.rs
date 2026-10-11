pub mod lastfm;

pub use lastfm::{
    __path_lastfm_disconnect, __path_lastfm_login, __path_lastfm_status, LastfmLoginRequest,
    LastfmStatusResponse, lastfm_credentials, lastfm_disconnect, lastfm_login, lastfm_router,
    lastfm_status,
};
