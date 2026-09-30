use std::env;

use anyhow::{Context, Result, bail};
use bot::musicbrainz::RecordingMbidResolver;

const BATCH_SIZE: usize = 100;

#[derive(Default)]
struct Options {
    apply: bool,
    limit: Option<usize>,
    after_id: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let Some(options) = parse_options()? else {
        return Ok(());
    };
    let database_url = env::var("DATABASE_URL")
        .context("DATABASE_URL must be set in the environment; the backfill never loads .env")?;
    let pool = db::connect(&database_url)
        .await
        .context("connect to the configured database")?;
    let tracks = db::TracksRepository::new(pool);

    if !options.apply {
        let candidates = tracks
            .count_tracks_without_recording_mbid_with_isrc()
            .await
            .context(
                "count tracks eligible for MBID resolution; confirm the production migration has completed",
            )?;
        println!(
            "DRY RUN: {candidates} tracks have an ISRC and no recording MBID. No MusicBrainz requests or database writes were made. Run with --apply after the migration is deployed."
        );
        return Ok(());
    }

    let resolver = RecordingMbidResolver::default();
    let mut after_id = options.after_id;
    let mut scanned = 0usize;
    let mut matched = 0usize;
    let mut updated = 0usize;
    let mut already_updated = 0usize;
    let mut unmatched = 0usize;

    loop {
        let batch_limit = options
            .limit
            .map(|limit| limit.saturating_sub(scanned).min(BATCH_SIZE))
            .unwrap_or(BATCH_SIZE);
        if batch_limit == 0 {
            break;
        }
        let batch = tracks
            .find_tracks_without_recording_mbid_after(after_id, batch_limit as i64)
            .await
            .context("load tracks needing recording MBIDs; confirm the production migration has completed")?;
        if batch.is_empty() {
            break;
        }

        for track in batch {
            after_id = track.id;
            scanned += 1;
            let recording_mbid = match track.isrc.as_deref() {
                Some(isrc) => {
                    resolver
                        .resolve(isrc, &track.title, &track.artist, i64::from(track.duration))
                        .await
                }
                None => None,
            };

            if let Some(recording_mbid) = recording_mbid {
                matched += 1;
                if tracks
                    .update_recording_mbid_if_missing(track.id, &recording_mbid)
                    .await
                    .with_context(|| format!("save MBID for track row {}", track.id))?
                {
                    updated += 1;
                } else {
                    already_updated += 1;
                }
            } else {
                unmatched += 1;
            }

            if scanned % 25 == 0 {
                println!(
                    "Scanned {scanned}; matched {matched}; updated {updated}; unmatched {unmatched}; last id {after_id}."
                );
            }
        }
    }

    println!(
        "Backfill complete: scanned {scanned}; matched {matched}; updated {updated}; concurrently filled {already_updated}; unmatched {unmatched}; last id {after_id}."
    );
    Ok(())
}

fn parse_options() -> Result<Option<Options>> {
    let mut options = Options::default();
    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--apply" => options.apply = true,
            "--limit" => {
                let value = args.next().context("--limit requires a positive integer")?;
                let limit = value
                    .parse::<usize>()
                    .context("--limit requires a positive integer")?;
                if limit == 0 {
                    bail!("--limit must be greater than zero");
                }
                options.limit = Some(limit);
            }
            "--after-id" => {
                let value = args
                    .next()
                    .context("--after-id requires a non-negative integer")?;
                options.after_id = value
                    .parse::<i32>()
                    .context("--after-id requires a non-negative integer")?;
                if options.after_id < 0 {
                    bail!("--after-id must be non-negative");
                }
            }
            "--help" | "-h" => {
                println!(
                    "Usage: backfill_recording_mbids [--apply] [--limit ROWS] [--after-id ID]\n\nWithout --apply, only reports the eligible row count. --apply resolves and stores unambiguous MusicBrainz recording IDs. DATABASE_URL must be provided by the runtime environment."
                );
                return Ok(None);
            }
            _ => bail!("unknown argument {argument:?}; pass --help for usage"),
        }
    }
    Ok(Some(options))
}
