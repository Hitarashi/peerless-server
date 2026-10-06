use crate::settings::BotSettings;

pub trait TaskBookkeeping: Send + Sync {
    fn settings_snapshot(&self) -> BotSettings;
}
