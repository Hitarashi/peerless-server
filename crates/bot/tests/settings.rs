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
    assert!(text.ends_with(
        "<blockquote><i>Select a setting below to view details, learn how to configure it, or change values.</i></blockquote>"
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

#[test]
fn catalog_and_detail_callbacks_round_trip() {
    use bot::interaction::{SettingPage, SettingsAction, TelegramAction};

    let pages = [
        (SettingPage::Catalog, "settings:view:catalog"),
        (SettingPage::Mode, "settings:view:mode"),
        (SettingPage::Apple, "settings:view:apple"),
        (SettingPage::Scopes, "settings:view:scopes"),
        (SettingPage::Lyricsporn, "settings:view:lyricsporn"),
        (SettingPage::Stream, "settings:view:stream"),
        (SettingPage::Storefront, "settings:view:storefront"),
    ];

    for (page, expected_code) in pages {
        let action = TelegramAction::Settings(SettingsAction::View(page));
        assert_eq!(action.encode(), expected_code);
        assert_eq!(TelegramAction::decode(expected_code).unwrap(), action);
    }

    let set_mode = TelegramAction::Settings(SettingsAction::SetMode(RippingMode::Paused));
    assert_eq!(set_mode.encode(), "settings:set_mode:paused");
    assert_eq!(
        TelegramAction::decode("settings:set_mode:paused").unwrap(),
        set_mode
    );

    let clear_lp = TelegramAction::Settings(SettingsAction::ClearLyricsporn);
    assert_eq!(clear_lp.encode(), "settings:clear:lyricsporn");
    assert_eq!(
        TelegramAction::decode("settings:clear:lyricsporn").unwrap(),
        clear_lp
    );

    let clear_stream = TelegramAction::Settings(SettingsAction::ClearStreamUrl);
    assert_eq!(clear_stream.encode(), "settings:clear:stream");
    assert_eq!(
        TelegramAction::decode("settings:clear:stream").unwrap(),
        clear_stream
    );

    let sf = TelegramAction::Settings(SettingsAction::SetStorefront("jp".to_string()));
    assert_eq!(sf.encode(), "settings:sf:jp");
    assert_eq!(TelegramAction::decode("settings:sf:jp").unwrap(), sf);
}
