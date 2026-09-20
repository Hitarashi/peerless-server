//! `/settings` presentation and storefront behavior tests.

use bot::handlers::settings::{POPULAR_STOREFRONTS, render_settings_text, render_storefronts_text};
use engine::settings::{BotSettings, RippingMode, default_settings};

fn settings() -> BotSettings {
    BotSettings {
        auto_dump_enabled: false,
        ..default_settings()
    }
}

#[test]
fn settings_text_renders_expected_layout() {
    let text = render_settings_text(&settings());
    assert!(text.starts_with("<b>Bot settings and operation controls</b><br/><br/>"));
    assert!(text.contains(
        "• <b>Engine Mode:</b> <b>Live ripping</b> (Cache hits and live decryption)<br/>"
    ));
    assert!(text.contains("• <b>Apple Music Ripping:</b> Enabled<br/>"));
    assert!(text.contains("• <b>Qobuz Ripping:</b> Enabled<br/>"));
    assert!(text.contains("• <b>Album Ripping:</b> Enabled<br/>"));
    assert!(text.contains("• <b>Auto-Dump New Music:</b> Disabled<br/>"));
    assert!(text.contains("• <b>Auto-Dump Storefronts:</b> <code>US</code><br/>"));
    assert!(text.contains("• <b>Max Collection Limit:</b> <code>50 tracks</code>"));
    assert!(text.ends_with(
        "<blockquote><i>Use the buttons below to toggle settings. Owner requests bypass these limits.</i></blockquote>"
    ));
}

#[test]
fn settings_text_renders_mode_and_limit_variants() {
    let mut s = settings();
    s.ripping_mode = RippingMode::CacheOnly;
    assert!(render_settings_text(&s).contains(
        "• <b>Engine Mode:</b> <b>Cache only</b> (Serves cached songs; live decryption blocked)<br/>"
    ));
    s.ripping_mode = RippingMode::Paused;
    assert!(render_settings_text(&s).contains(
        "• <b>Engine Mode:</b> ! <b>Paused</b> (Ripping commands suspended for regular users)<br/>"
    ));
    s.max_collection_tracks = 0;
    assert!(
        render_settings_text(&s).contains("• <b>Max Collection Limit:</b> <code>Unlimited</code>")
    );
}

#[test]
fn storefronts_text_renders_expected_layout() {
    let text = render_storefronts_text(&settings());
    assert!(text.starts_with("<b>Auto-dump storefront configuration</b><br/><br/>"));
    assert!(text.contains("• <b>Active Storefronts:</b> <code>US</code>"));
    assert!(
        text.ends_with(
            "<i>You can also use:</i> <code>/settings storefronts add &lt;code&gt;</code>"
        )
    );
}

#[test]
fn popular_storefronts_match_oracle_order() {
    assert_eq!(
        POPULAR_STOREFRONTS,
        ["us", "gb", "jp", "in", "ca", "au", "de", "fr"]
    );
}

#[test]
fn db_settings_store_round_trips_auto_dump_and_storefronts() {
    // The db store is exercised through a tokio Postgres harness elsewhere;
    // here we pin the pure settings field semantics the UI depends on.
    let mut s = settings();
    assert!(!s.auto_dump_enabled);
    s.auto_dump_enabled = true;
    assert!(s.auto_dump_enabled);
    s.auto_dump_storefronts.push("jp".to_owned());
    assert_eq!(s.auto_dump_storefronts, vec!["us", "jp"]);
}

#[test]
fn settings_text_renders_provider_toggles() {
    let mut s = settings();
    s.apple_rip_enabled = false;
    assert!(render_settings_text(&s).contains("• <b>Apple Music Ripping:</b> Disabled<br/>"));
    assert!(render_settings_text(&s).contains("• <b>Qobuz Ripping:</b> Enabled<br/>"));

    s.apple_rip_enabled = true;
    s.qobuz_rip_enabled = false;
    assert!(render_settings_text(&s).contains("• <b>Apple Music Ripping:</b> Enabled<br/>"));
    assert!(render_settings_text(&s).contains("• <b>Qobuz Ripping:</b> Disabled<br/>"));
}

#[test]
fn provider_setting_callbacks_round_trip() {
    use bot::interaction::{SettingFeature, SettingsAction, TelegramAction};

    let apple_action = TelegramAction::Settings(SettingsAction::Toggle(SettingFeature::Apple));
    let qobuz_action = TelegramAction::Settings(SettingsAction::Toggle(SettingFeature::Qobuz));

    assert_eq!(apple_action.encode(), "settings:apple");
    assert_eq!(qobuz_action.encode(), "settings:qobuz");

    assert_eq!(
        TelegramAction::decode("settings:apple").unwrap(),
        apple_action
    );
    assert_eq!(
        TelegramAction::decode("settings:qobuz").unwrap(),
        qobuz_action
    );
}
