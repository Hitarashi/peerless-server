//! Dynamic token and secret scraper for Qobuz web player bundles.

use std::collections::HashMap;

use base64::Engine;
use regex::Regex;

pub const FALLBACK_APP_IDS: &[&str] = &["798273057", "712108764", "598418042", "285473729"];
pub const FALLBACK_SECRETS: &[&str] = &[
    "f69a7734686cb9427629378a4b7ac381",
    "806331c3b0b641da923b890aed01d04a",
    "abb21364945c0583309667d13ca3d93a",
    "2da103d1587d55f0b50dc3e3a47da2c8",
    "d012ec6a256a427fef69b44122d259e8",
];

const BASE_URL: &str = "https://play.qobuz.com";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

pub struct ScrapedTokens {
    pub app_id: String,
    pub secrets: Vec<String>,
}

pub struct BundleScraper {
    client: reqwest::Client,
}

impl Default for BundleScraper {
    fn default() -> Self {
        Self::new()
    }
}

impl BundleScraper {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap_or_default();
        Self { client }
    }

    pub async fn get_tokens(&self) -> ScrapedTokens {
        match self.scrape_internal().await {
            Ok(tokens) => tokens,
            Err(e) => {
                tracing::warn!("Dynamic Qobuz bundle scrape failed, using fallbacks: {e}");
                ScrapedTokens {
                    app_id: FALLBACK_APP_IDS[0].to_owned(),
                    secrets: FALLBACK_SECRETS.iter().map(|&s| s.to_owned()).collect(),
                }
            }
        }
    }

    async fn scrape_internal(
        &self,
    ) -> Result<ScrapedTokens, Box<dyn std::error::Error + Send + Sync>> {
        let login_html = self
            .client
            .get(format!("{BASE_URL}/login"))
            .header("User-Agent", USER_AGENT)
            .send()
            .await?
            .text()
            .await?;

        let bundle_regex =
            Regex::new(r#"<script[^>]+src="(?P<url>/resources/[^"/]+/bundle\.js)""#)?;
        let bundle_path = if let Some(caps) = bundle_regex.captures(&login_html) {
            caps.name("url").map(|m| m.as_str().to_owned())
        } else {
            let sec_regex = Regex::new(r#"src="(?P<url>/resources/[^"]*bundle\.js)""#)?;
            sec_regex
                .captures(&login_html)
                .and_then(|c| c.name("url").map(|m| m.as_str().to_owned()))
        };

        let bundle_path = bundle_path.ok_or("Could not find bundle.js path in login page")?;
        let full_url = if bundle_path.starts_with("http") {
            bundle_path
        } else {
            format!("{BASE_URL}{bundle_path}")
        };

        let bundle_content = self
            .client
            .get(&full_url)
            .header("User-Agent", USER_AGENT)
            .send()
            .await?
            .text()
            .await?;

        let app_id = Self::extract_app_id(&bundle_content);
        let secrets = Self::extract_secrets(&bundle_content);

        Ok(ScrapedTokens { app_id, secrets })
    }

    fn extract_app_id(content: &str) -> String {
        let app_id_regex = Regex::new(r#"production:\{api:\{appId:"(?P<id>\d{9})""#).ok();
        if let Some(re) = app_id_regex
            && let Some(caps) = re.captures(content)
            && let Some(m) = caps.name("id")
        {
            return m.as_str().to_owned();
        }

        let secondary = Regex::new(r#"appId:"(?P<id>\d{9})""#).ok();
        if let Some(re) = secondary
            && let Some(caps) = re.captures(content)
            && let Some(m) = caps.name("id")
        {
            return m.as_str().to_owned();
        }

        FALLBACK_APP_IDS[0].to_owned()
    }

    fn extract_secrets(content: &str) -> Vec<String> {
        let mut secrets_map: HashMap<String, Vec<String>> = HashMap::new();

        let seed_regex = Regex::new(
            r#"[a-z]\.initialSeed\("(?P<seed>[\w=]+)",window\.utimezone\.(?P<tz>[a-z]+)\)"#,
        )
        .ok();

        if let Some(re) = seed_regex {
            for caps in re.captures_iter(content) {
                if let (Some(seed), Some(tz)) = (caps.name("seed"), caps.name("tz")) {
                    secrets_map
                        .entry(tz.as_str().to_ascii_lowercase())
                        .or_default()
                        .push(seed.as_str().to_owned());
                }
            }
        }

        if secrets_map.is_empty() {
            return FALLBACK_SECRETS.iter().map(|&s| s.to_owned()).collect();
        }

        let tz_names: Vec<String> = secrets_map
            .keys()
            .map(|k| {
                let mut c = k.chars();
                match c.next() {
                    None => String::new(),
                    Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
                }
            })
            .collect();

        let tz_pattern = tz_names.join("|");
        if let Ok(extras_regex) = Regex::new(&format!(
            r#"name:"\w+/(?P<tz>{tz_pattern})",info:"(?P<info>[\w=]+)",extras:"(?P<extras>[\w=]+)""#
        )) {
            for caps in extras_regex.captures_iter(content) {
                if let (Some(tz), Some(info), Some(extras)) =
                    (caps.name("tz"), caps.name("info"), caps.name("extras"))
                {
                    let tz_key = tz.as_str().to_ascii_lowercase();
                    if let Some(vec) = secrets_map.get_mut(&tz_key) {
                        vec.push(info.as_str().to_owned());
                        vec.push(extras.as_str().to_owned());
                    }
                }
            }
        }

        let mut decoded_secrets = Vec::new();
        for parts in secrets_map.values() {
            let combined = parts.join("");
            if combined.len() > 44 {
                let raw_b64 = &combined[..combined.len() - 44];
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(raw_b64)
                    && bytes.len() == 32
                    && let Ok(s) = String::from_utf8(bytes)
                {
                    decoded_secrets.push(s);
                }
            }
        }

        for &fb in FALLBACK_SECRETS {
            if !decoded_secrets.iter().any(|s| s == fb) {
                decoded_secrets.push(fb.to_owned());
            }
        }

        decoded_secrets
    }
}
