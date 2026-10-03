//! Typed Telegram callback protocol.
//!
//! All callback payloads cross this seam once. Feature modules receive typed
//! intent and never parse callback grammar themselves.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelegramAction {
    Dashboard {
        action: DashboardAction,
        page: usize,
    },
    Cancel {
        job_id: String,
    },
    Settings(SettingsAction),
    Report(ReportAction),
    DeliverCached {
        track_id: String,
    },
    Get {
        track_id: String,
    },
    SearchClose,
    AuthPage {
        page: usize,
    },
    AuthClose,
    ConfirmDelete {
        token: String,
    },
    CancelDelete {
        token: String,
    },
    ConfirmImport {
        token: String,
    },
    CancelImport {
        token: String,
    },
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsAction {
    Close,
    Refresh,
    Mode,
    Toggle(SettingFeature),
    Limit(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingFeature {
    Apple,
    Album,
    Playlist,
    Artist,
    Txt,
    MultiLink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportAction {
    Cancel,
    Submit {
        track_id: String,
        reason: ReportReason,
    },
    Dismiss {
        report_id: String,
    },
    Delete {
        track_id: String,
        report_id: String,
    },
    Rerip {
        track_id: String,
        report_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportReason {
    Corrupted,
    Incomplete,
    Metadata,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardAction {
    Previous,
    Next,
    Refresh,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    Empty,
    Unknown,
    Malformed,
    Oversized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackAck {
    Toast(String),
    Alert(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageEffect {
    Edit(String),
    Delete,
    None,
}

/// One callback query produces exactly one acknowledgement and at most one
/// message effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackOutcome {
    pub acknowledgement: CallbackAck,
    pub effect: MessageEffect,
}

const MAX_PAYLOAD: usize = 256;

impl TelegramAction {
    pub fn decode(data: &str) -> Result<Self, DecodeError> {
        if data.is_empty() {
            return Err(DecodeError::Empty);
        }
        if data.len() > MAX_PAYLOAD {
            return Err(DecodeError::Oversized);
        }
        if data == "search_close" {
            return Ok(Self::SearchClose);
        }
        if data == "authclose" {
            return Ok(Self::AuthClose);
        }
        if data == "noop" {
            return Ok(Self::Noop);
        }
        let mut parts = data.split(':');
        let prefix = parts.next().ok_or(DecodeError::Malformed)?;
        let rest = parts.collect::<Vec<_>>();
        match prefix {
            "cancel" => one(rest).map(|job_id| Self::Cancel { job_id }),
            "cached" => one(rest).map(|track_id| Self::DeliverCached { track_id }),
            "get" => one(rest).map(|track_id| Self::Get { track_id }),
            "authpage" => one(rest)
                .and_then(|value| parse_page(&value))
                .map(|page| Self::AuthPage { page }),
            "dashboard" => {
                if rest.len() != 2 {
                    return Err(DecodeError::Malformed);
                }
                let action = match rest[0] {
                    "prev" => DashboardAction::Previous,
                    "next" => DashboardAction::Next,
                    "refresh" => DashboardAction::Refresh,
                    _ => return Err(DecodeError::Unknown),
                };
                parse_page(rest[1]).map(|page| Self::Dashboard { action, page })
            }
            "settings" => decode_settings(rest).map(Self::Settings),
            "report" => decode_report(rest).map(Self::Report),
            "delete_confirm" => one(rest)
                .and_then(|token| {
                    valid_token(&token)
                        .then_some(token)
                        .ok_or(DecodeError::Malformed)
                })
                .map(|token| Self::ConfirmDelete { token }),
            "delete_cancel" => one(rest)
                .and_then(|token| {
                    valid_token(&token)
                        .then_some(token)
                        .ok_or(DecodeError::Malformed)
                })
                .map(|token| Self::CancelDelete { token }),
            "import_confirm" => one(rest)
                .and_then(|token| {
                    valid_token(&token)
                        .then_some(token)
                        .ok_or(DecodeError::Malformed)
                })
                .map(|token| Self::ConfirmImport { token }),
            "import_cancel" => one(rest)
                .and_then(|token| {
                    valid_token(&token)
                        .then_some(token)
                        .ok_or(DecodeError::Malformed)
                })
                .map(|token| Self::CancelImport { token }),
            _ => Err(DecodeError::Unknown),
        }
    }

    pub fn encode(&self) -> String {
        match self {
            Self::Dashboard { action, page } => format!(
                "dashboard:{}:{page}",
                match action {
                    DashboardAction::Previous => "prev",
                    DashboardAction::Next => "next",
                    DashboardAction::Refresh => "refresh",
                }
            ),
            Self::Cancel { job_id } => format!("cancel:{job_id}"),
            Self::Settings(action) => encode_settings(action),
            Self::Report(action) => encode_report(action),
            Self::DeliverCached { track_id } => format!("cached:{track_id}"),
            Self::Get { track_id } => format!("get:{track_id}"),
            Self::SearchClose => "search_close".to_owned(),
            Self::AuthPage { page } => format!("authpage:{page}"),
            Self::AuthClose => "authclose".to_owned(),
            Self::ConfirmDelete { token } => format!("delete_confirm:{token}"),
            Self::CancelDelete { token } => format!("delete_cancel:{token}"),
            Self::ConfirmImport { token } => format!("import_confirm:{token}"),
            Self::CancelImport { token } => format!("import_cancel:{token}"),
            Self::Noop => "noop".to_owned(),
        }
    }
}

fn one(parts: Vec<&str>) -> Result<String, DecodeError> {
    if parts.len() == 1 && !parts[0].is_empty() {
        Ok(parts[0].to_owned())
    } else {
        Err(DecodeError::Malformed)
    }
}

fn decode_settings(parts: Vec<&str>) -> Result<SettingsAction, DecodeError> {
    match parts.as_slice() {
        ["close"] => Ok(SettingsAction::Close),
        ["refresh"] => Ok(SettingsAction::Refresh),
        ["mode"] => Ok(SettingsAction::Mode),
        ["apple"] => Ok(SettingsAction::Toggle(SettingFeature::Apple)),
        ["album"] => Ok(SettingsAction::Toggle(SettingFeature::Album)),
        ["playlist"] => Ok(SettingsAction::Toggle(SettingFeature::Playlist)),
        ["artist"] => Ok(SettingsAction::Toggle(SettingFeature::Artist)),
        ["txt"] => Ok(SettingsAction::Toggle(SettingFeature::Txt)),
        ["multilink"] => Ok(SettingsAction::Toggle(SettingFeature::MultiLink)),
        ["limit", value] => value
            .parse::<u32>()
            .map(SettingsAction::Limit)
            .map_err(|_| DecodeError::Malformed),
        _ => Err(DecodeError::Unknown),
    }
}

fn decode_report(parts: Vec<&str>) -> Result<ReportAction, DecodeError> {
    match parts.as_slice() {
        ["cancel"] => Ok(ReportAction::Cancel),
        ["sub", track_id, reason] if valid_id(track_id) => Ok(ReportAction::Submit {
            track_id: (*track_id).to_owned(),
            reason: parse_reason(reason)?,
        }),
        ["act", "dismiss", report_id] if valid_id(report_id) => Ok(ReportAction::Dismiss {
            report_id: (*report_id).to_owned(),
        }),
        ["act", "del", track_id, report_id] if valid_id(track_id) && valid_id(report_id) => {
            Ok(ReportAction::Delete {
                track_id: (*track_id).to_owned(),
                report_id: (*report_id).to_owned(),
            })
        }
        ["act", "rerip", track_id, report_id] if valid_id(track_id) && valid_id(report_id) => {
            Ok(ReportAction::Rerip {
                track_id: (*track_id).to_owned(),
                report_id: (*report_id).to_owned(),
            })
        }
        _ => Err(DecodeError::Unknown),
    }
}

fn parse_reason(value: &str) -> Result<ReportReason, DecodeError> {
    match value {
        "corrupted" => Ok(ReportReason::Corrupted),
        "incomplete" => Ok(ReportReason::Incomplete),
        "metadata" => Ok(ReportReason::Metadata),
        "other" => Ok(ReportReason::Other),
        _ => Err(DecodeError::Unknown),
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn valid_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn encode_settings(action: &SettingsAction) -> String {
    match action {
        SettingsAction::Close => "settings:close".to_owned(),
        SettingsAction::Refresh => "settings:refresh".to_owned(),
        SettingsAction::Mode => "settings:mode".to_owned(),
        SettingsAction::Toggle(feature) => format!(
            "settings:{}",
            match feature {
                SettingFeature::Apple => "apple",
                SettingFeature::Album => "album",
                SettingFeature::Playlist => "playlist",
                SettingFeature::Artist => "artist",
                SettingFeature::Txt => "txt",
                SettingFeature::MultiLink => "multilink",
            }
        ),
        SettingsAction::Limit(limit) => format!("settings:limit:{limit}"),
    }
}

fn encode_report(action: &ReportAction) -> String {
    match action {
        ReportAction::Cancel => "report:cancel".to_owned(),
        ReportAction::Submit { track_id, reason } => format!(
            "report:sub:{track_id}:{}",
            match reason {
                ReportReason::Corrupted => "corrupted",
                ReportReason::Incomplete => "incomplete",
                ReportReason::Metadata => "metadata",
                ReportReason::Other => "other",
            }
        ),
        ReportAction::Dismiss { report_id } => format!("report:act:dismiss:{report_id}"),
        ReportAction::Delete {
            track_id,
            report_id,
        } => {
            format!("report:act:del:{track_id}:{report_id}")
        }
        ReportAction::Rerip {
            track_id,
            report_id,
        } => {
            format!("report:act:rerip:{track_id}:{report_id}")
        }
    }
}

fn parse_page(value: &str) -> Result<usize, DecodeError> {
    value
        .parse::<usize>()
        .map_err(|_| DecodeError::Malformed)
        .and_then(|page| (page > 0).then_some(page).ok_or(DecodeError::Malformed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_actions_round_trip() {
        let actions = [
            TelegramAction::Dashboard {
                action: DashboardAction::Next,
                page: 2,
            },
            TelegramAction::Cancel {
                job_id: "job-1".into(),
            },
            TelegramAction::Settings(SettingsAction::Toggle(SettingFeature::Apple)),
            TelegramAction::Report(ReportAction::Dismiss {
                report_id: "7".into(),
            }),
            TelegramAction::DeliverCached {
                track_id: "1".into(),
            },
            TelegramAction::Get {
                track_id: "2".into(),
            },
            TelegramAction::SearchClose,
            TelegramAction::AuthPage { page: 3 },
            TelegramAction::AuthClose,
            TelegramAction::ConfirmDelete {
                token: "t-1".into(),
            },
            TelegramAction::CancelDelete {
                token: "t-2".into(),
            },
            TelegramAction::ConfirmImport {
                token: "t-3".into(),
            },
            TelegramAction::CancelImport {
                token: "t-4".into(),
            },
            TelegramAction::Noop,
        ];
        for action in actions {
            assert_eq!(TelegramAction::decode(&action.encode()), Ok(action));
        }
    }

    #[test]
    fn invalid_callbacks_are_rejected() {
        assert_eq!(
            TelegramAction::decode("unknown:thing"),
            Err(DecodeError::Unknown)
        );
        assert_eq!(
            TelegramAction::decode("dashboard:next:0"),
            Err(DecodeError::Malformed)
        );
        assert_eq!(
            TelegramAction::decode("cancel:"),
            Err(DecodeError::Malformed)
        );
        for payload in [
            "settings:limit:-1",
            "report:sub:123:unknown",
            "report:act:del:123",
            "delete_confirm:",
            "import_cancel:token with spaces",
        ] {
            assert!(TelegramAction::decode(payload).is_err(), "{payload}");
        }
    }
}
