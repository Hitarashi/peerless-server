//! Typed provenance for an acquired stream source.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceId {
    PrimaryMirror,
    WrapperLite { url: String },
    WrapperCandidate { endpoint: String },
    ProviderBackend { provider: String, url: String },
    NativeProvider { provider: String },
}

impl fmt::Display for SourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimaryMirror => formatter.write_str("primary mirror"),
            Self::WrapperLite { url } => write!(formatter, "wrapper-lite ({url})"),
            Self::WrapperCandidate { endpoint } => {
                write!(formatter, "wrapper candidate ({endpoint})")
            }
            Self::ProviderBackend { provider, url } => {
                write!(formatter, "{provider} backend ({url})")
            }
            Self::NativeProvider { provider } => write!(formatter, "{provider} native ripper"),
        }
    }
}
