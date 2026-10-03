//! `/settings` presentation behavior tests.

use bot::handlers::settings::render_settings_text;
use engine::settings::{BotSettings, RippingMode, default_settings};

fn settings() -> BotSettings {
    default_settings()
}

#[test]
fn settings_text_renders_expected_layout() {
    let text = render_settings_text(&settings());
    assert!(text.starts_with("<b>Bot settings and operation controls</b><br/><br/>"));
    assert!(text.contains(
        "• <b>Engine Mode:</b> <b>Live ripping</b> (Cache hits and live decryption)<br/>"
    ));
    assert!(text.contains("• <b>Apple Music Ripping:</b> Enabled<br/>"));
    assert!(text.contains("• <b>Album Ripping:</b> Enabled<br/>"));
    assert!(text.contains("• <b>Max Collection Limit:</b> <code>50 tracks</code>"));
    assert!(text.ends_with(
        "<blockquote><i>Use the buttons below to toggle settings. Set or clear the API URL with <code>/settings lyricsporn_url &lt;URL|clear&gt;</code>. Owner requests bypass ripping limits.</i></blockquote>"
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
fn settings_text_renders_apple_toggle() {
    let mut s = settings();
    s.apple_rip_enabled = false;
    assert!(render_settings_text(&s).contains("• <b>Apple Music Ripping:</b> Disabled<br/>"));

    s.apple_rip_enabled = true;
    assert!(render_settings_text(&s).contains("• <b>Apple Music Ripping:</b> Enabled<br/>"));
}

#[test]
fn provider_setting_callbacks_round_trip() {
    use bot::interaction::{SettingFeature, SettingsAction, TelegramAction};

    let apple_action = TelegramAction::Settings(SettingsAction::Toggle(SettingFeature::Apple));
    assert_eq!(apple_action.encode(), "settings:apple");

    assert_eq!(
        TelegramAction::decode("settings:apple").unwrap(),
        apple_action
    );
}
