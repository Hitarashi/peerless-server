//! Provider-neutral HTTP and audio streaming primitives.

mod http;
mod source_id;
mod stream_transport;

pub use http::{
    ByteStream, CHROME_USER_AGENT, ReqwestHttp, StreamBodyError, StreamHttp, StreamHttpError,
    StreamHttpResponse,
};
pub use source_id::SourceId;
pub use stream_transport::{
    AudioStreamSource, FetchEndpointOptions, ProgressCallback, StreamError, StreamTransport,
};
