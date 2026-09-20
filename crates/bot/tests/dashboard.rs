use bot::dashboard::{DashboardJob, DashboardSnapshot, JobPhase, cancelable_job_ids, render};
use engine::orchestrator::types::{
    ByteProgress, DownloadLane, RipActivity, TrackLabel, UploadLane,
};

fn track(title: &str, artist: &str) -> TrackLabel {
    TrackLabel::new(title, artist)
}

fn downloading(title: &str, artist: &str) -> DownloadLane {
    DownloadLane::Rip(RipActivity::Downloading {
        track: track(title, artist),
        progress: ByteProgress {
            completed: 1_048_576,
            total: Some(2_097_152),
        },
    })
}

fn job(n: usize, allowed: bool) -> DashboardJob {
    DashboardJob {
        id: format!("j{n}"),
        requester_id: 10 + n as i64,
        requester_name: format!("Requester {n}"),
        header: format!("Album {n}"),
        phase: if n == 0 {
            JobPhase::Processing
        } else {
            JobPhase::Queued
        },
        queue_position: (n != 0).then_some(n as u64),
        cached: 2,
        ripped: 3,
        failed: 0,
        total: 10,
        percent: 50,
        job_activity: None,
        download: None,
        upload: None,
        is_cancel_allowed_for_viewer: allowed,
    }
}

#[test]
fn empty_dashboard_is_exact_and_pages_are_mobile_friendly() {
    let empty = DashboardSnapshot {
        ripping_mode: "sequential".into(),
        mirror_health: None,
        current_job_activity: None,
        current_download: None,
        current_upload: None,
        jobs: vec![],
    };
    assert_eq!(render(&empty, 1).0, "<b>No active downloads.</b>");
    let jobs = (0..6).map(|n| job(n, n == 0)).collect();
    let snapshot = DashboardSnapshot {
        ripping_mode: "sequential".into(),
        mirror_health: Some("healthy".into()),
        current_job_activity: None,
        current_download: None,
        current_upload: None,
        jobs,
    };
    let (text, keyboard) = render(&snapshot, 1);
    assert!(
        text.contains("Requester 0") && !text.contains("Status: Processing") && text.contains("1.")
    );
    assert!(text.contains("Status: Queued") && text.contains("Page 1/3") && keyboard.is_some());
}

#[test]
fn cancel_controls_are_only_rendered_for_allowed_rows() {
    let snapshot = DashboardSnapshot {
        ripping_mode: "sequential".into(),
        mirror_health: None,
        current_job_activity: None,
        current_download: None,
        current_upload: None,
        jobs: vec![job(0, true), job(1, false)],
    };
    let (text, keyboard) = render(&snapshot, 1);
    assert!(keyboard.is_some());
    assert!(text.contains("/cancel_j0"));
    assert!(text.contains("/cancel_j1"));
    assert_eq!(cancelable_job_ids(&snapshot, 1, false), vec!["j0"]);
    assert_eq!(cancelable_job_ids(&snapshot, 1, true), vec!["j0", "j1"]);
}

#[test]
fn header_renders_both_lane_lines_independently() {
    let both = DashboardSnapshot {
        ripping_mode: "sequential".into(),
        mirror_health: None,
        current_job_activity: None,
        current_download: Some(downloading("Song", "")),
        current_upload: Some(UploadLane::ArchiveUpload {
            archive: "Album.zip".into(),
            progress: ByteProgress {
                completed: 0,
                total: None,
            },
        }),
        jobs: vec![job(0, true)],
    };
    let (text, _) = render(&both, 1);
    assert!(text.contains(
        "<b>⬇️ Downloading:</b> <b>Song</b> <code>[■■■■■■□□□□□□] 50% (1.0/2.0 MB)</code>"
    ));
    assert!(text.contains("<b>⬆️ Uploading ZIP:</b> <b>Album.zip</b>"));

    let download_only = DashboardSnapshot {
        current_download: Some(downloading("Song", "")),
        current_upload: None,
        ..both.clone()
    };
    let (text, _) = render(&download_only, 1);
    assert!(text.contains("<b>⬇️ Downloading:</b> <b>Song</b>"));
    assert!(!text.contains("⬆️"));

    let idle = DashboardSnapshot {
        current_download: None,
        current_upload: None,
        jobs: vec![job(0, true)],
        ..both
    };
    let (text, _) = render(&idle, 1);
    assert!(!text.contains("⬇️"));
    assert!(!text.contains("⬆️"));
}

#[test]
fn upload_only_header_does_not_render_a_downloading_lane() {
    let snapshot = DashboardSnapshot {
        current_download: None,
        current_upload: Some(UploadLane::Track {
            track: track("Song", ""),
            progress: ByteProgress {
                completed: 4 * 1_048_576,
                total: None,
            },
        }),
        jobs: vec![job(0, true)],
        ..DashboardSnapshot::default()
    };

    let (text, _) = render(&snapshot, 1);
    assert!(!text.contains("⬇️"));
    assert!(text.contains("<b>⬆️ Uploading:</b> <b>Song</b> <code>4.0 MB</code>"));
}
