//! Score-based ordering for the download queue.

use std::time::Instant;

use super::*;

/// How the user submitted the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestSource {
    Client,
    TelegramBot,
}

/// Provenance of a job, captured before any fan-out expansion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JobOrigin {
    pub source: RequestSource,
    pub expanded_from_artist: bool,
}

impl Default for JobOrigin {
    /// Bot requests are the overwhelming majority, so an unset origin scores
    /// like one instead of silently claiming the client bonus.
    fn default() -> Self {
        Self {
            source: RequestSource::TelegramBot,
            expanded_from_artist: false,
        }
    }
}

const CLIENT_POINTS: i16 = 100;
const TRACK_POINTS: i16 = 60;
const ALBUM_POINTS: i16 = 10;
const PLAYLIST_POINTS: i16 = -60;
const ARTIST_EXPANSION_POINTS: i16 = -60;
const MULTI_LINK_POINTS: i16 = -40;
const ADMIN_POINTS: i16 = -200;

pub(super) struct PriorityInput<'a> {
    pub(super) options: &'a RipTaskOptions,
    pub(super) enqueued_at: Instant,
}

/// Higher score runs sooner.
///
/// The table below is time independent on purpose: waiting time is layered on
/// separately by [`effective_score`], so `enqueued_at` only rides along for
/// that pairing and two jobs keep the same relative order until one of them
/// actually runs.
pub(super) fn base_score(input: &PriorityInput<'_>) -> i16 {
    let _ = input.enqueued_at;
    let options = input.options;
    let mut score = match options.origin.source {
        RequestSource::Client => CLIENT_POINTS,
        RequestSource::TelegramBot => 0,
    };

    match options.parsed_items.as_slice() {
        [item] => {
            score += match item.kind {
                TargetKind::Track => TRACK_POINTS,
                TargetKind::Album => ALBUM_POINTS,
                TargetKind::Playlist => PLAYLIST_POINTS,
                // A bare artist link still has to be resolved before it rips.
                TargetKind::Artist => 0,
            };
        }
        // Nothing was linked, so there is no kind signal to weigh.
        [] => {}
        [_, _, ..] => score += MULTI_LINK_POINTS,
    }

    if options.origin.expanded_from_artist {
        score += ARTIST_EXPANSION_POINTS;
    }
    if options.is_admin {
        score += ADMIN_POINTS;
    }

    score
}

/// Scales the base score up so waiting can overtake it, then adds the wait.
///
/// Waiting buys 5 points per second against a base score that is worth 100
/// points per rung, so a job one rung (20 points) behind a fresh arrival needs
/// 400 seconds of waiting to catch up. That is deliberately slow: a late
/// arrival can still jump the queue, but a backlog that is only minutes old
/// keeps the ordering it was given.
pub(crate) fn effective_score(base: i16, waited_secs: u64) -> i64 {
    i64::from(base) * 100 + waited_secs as i64 * 5
}

#[cfg(test)]
mod tests {
    use music::RenditionPolicy;

    use super::*;
    use crate::types::ParsedTargetItem;

    const BOT: JobOrigin = JobOrigin {
        source: RequestSource::TelegramBot,
        expanded_from_artist: false,
    };

    fn item(kind: TargetKind) -> ParsedTargetItem {
        ParsedTargetItem {
            id: "id".to_owned(),
            kind,
            storefront: None,
        }
    }

    fn score(parsed_items: Vec<ParsedTargetItem>, is_admin: bool, origin: JobOrigin) -> i16 {
        let options = RipTaskOptions {
            chat_id: 100,
            user_id: 42,
            user_name: None,
            delivery_chat_id: 100,
            is_group: false,
            is_force: false,
            is_cache_only: false,
            single_storefront: None,
            parsed_items,
            reply_to_message_id: None,
            is_admin,
            codec_preference: None,
            rendition_policy: RenditionPolicy::PrimaryOnly,
            origin,
        };
        base_score(&PriorityInput {
            options: &options,
            enqueued_at: Instant::now(),
        })
    }

    #[test]
    fn client_track_outranks_bot_track_by_the_source_bonus() {
        let web = JobOrigin {
            source: RequestSource::Client,
            expanded_from_artist: false,
        };
        assert_eq!(
            score(vec![item(TargetKind::Track)], false, web),
            160,
            "client + single track"
        );
        assert_eq!(score(vec![item(TargetKind::Track)], false, BOT), 60);
    }

    #[test]
    fn single_item_kinds_score_album_above_playlist() {
        assert_eq!(score(vec![item(TargetKind::Album)], false, BOT), 10);
        assert_eq!(score(vec![item(TargetKind::Playlist)], false, BOT), -60);
        assert_eq!(score(vec![item(TargetKind::Artist)], false, BOT), 0);
    }

    #[test]
    fn artist_expansion_penalises_every_expanded_album() {
        let expanded = JobOrigin {
            source: RequestSource::TelegramBot,
            expanded_from_artist: true,
        };
        assert_eq!(score(vec![item(TargetKind::Album)], false, expanded), -50);
        assert_eq!(
            score(vec![item(TargetKind::Track)], false, expanded),
            0,
            "the expansion penalty cancels the track bonus"
        );
    }

    #[test]
    fn multi_link_batches_pay_the_multi_link_penalty() {
        assert_eq!(
            score(
                vec![item(TargetKind::Track), item(TargetKind::Track)],
                false,
                BOT
            ),
            -40,
            "the multi-link rule replaces the single-item bonus"
        );
    }

    #[test]
    fn admins_are_penalised() {
        assert_eq!(
            score(vec![item(TargetKind::Album)], true, BOT),
            -190,
            "admin penalty"
        );
        assert_eq!(score(vec![item(TargetKind::Track)], true, BOT), -140);
    }

    #[test]
    fn empty_item_lists_score_without_panicking() {
        assert_eq!(score(Vec::new(), false, BOT), 0);
        assert_eq!(
            score(Vec::new(), true, JobOrigin::default()),
            -200,
            "other signals still apply"
        );
    }

    #[test]
    fn aging_lifts_equal_bases_towards_older_items() {
        assert_eq!(effective_score(0, 0), 0, "a fresh arrival has no credit");
        assert_eq!(effective_score(0, 1), 5, "one second of waiting is 5");
        assert_eq!(
            effective_score(-60, 0),
            -6000,
            "the base score is scaled by 100"
        );
        assert!(
            effective_score(0, 40) > effective_score(0, 10),
            "among equal bases the older job leads"
        );
    }

    #[test]
    fn aging_eventually_overtakes_but_only_over_a_long_wait() {
        // A 70-point base gap is 7000 scaled points, at 5 per second.
        assert!(
            effective_score(-60, 1399) < effective_score(10, 0),
            "1399 seconds is still not quite enough"
        );
        assert_eq!(
            effective_score(-60, 1400),
            effective_score(10, 0),
            "exactly 1400 seconds of waiting catches a fresh arrival"
        );
        assert!(effective_score(-60, 1401) > effective_score(10, 0));
    }
}
