use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use sha2::{Digest, Sha256};

use crate::DbError;

#[derive(Clone)]
pub struct CryptoCipher {
    cipher: Aes256Gcm,
}

impl CryptoCipher {
    pub fn new(secret_key: &str) -> Result<Self, DbError> {
        let key = Sha256::digest(secret_key.as_bytes());
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|e| DbError::Row(format!("Failed to initialize cipher: {e}")))?;
        Ok(Self { cipher })
    }

    pub fn from_env() -> Result<Self, DbError> {
        let secret_key = std::env::var("APP_KEY")
            .map_err(|_| DbError::Row("APP_KEY is required in environment".into()))?;
        Self::new(&secret_key)
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<String, DbError> {
        let nonce_bytes: [u8; 12] = rand::random();
        let nonce = Nonce::from(nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .map_err(|e| DbError::Row(format!("encryption failed: {e}")))?;
        let mut combined = Vec::with_capacity(12 + ciphertext.len());
        combined.extend_from_slice(&nonce_bytes);
        combined.extend_from_slice(&ciphertext);
        Ok(BASE64.encode(&combined))
    }

    pub fn decrypt(&self, base64_ciphertext: &str) -> Result<Option<String>, DbError> {
        let decoded = BASE64
            .decode(base64_ciphertext)
            .map_err(|e| DbError::Row(format!("base64 decode failed: {e}")))?;
        if decoded.len() < 12 {
            return Ok(None);
        }
        let (nonce_bytes, ciphertext) = decoded.split_at(12);
        let nonce = Nonce::try_from(nonce_bytes)
            .map_err(|e| DbError::Row(format!("invalid nonce: {e}")))?;
        let plaintext = self
            .cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|e| DbError::Row(format!("decryption failed: {e}")))?;
        let s = String::from_utf8(plaintext)
            .map_err(|e| DbError::Row(format!("utf8 decode failed: {e}")))?;
        Ok(Some(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let cipher = CryptoCipher::new("test_secret_key_12345").unwrap();
        let plaintext = "my_super_secret_lastfm_session_key";
        let encrypted = cipher.encrypt(plaintext).unwrap();
        assert_ne!(encrypted, plaintext);
        let decrypted = cipher.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, Some(plaintext.to_string()));
    }

    #[test]
    fn test_decrypt_invalid() {
        let cipher = CryptoCipher::new("test_secret_key_12345").unwrap();
        let result = cipher.decrypt("invalid_base64!!!");
        assert!(result.is_err());

        let short_base64 = BASE64.encode(b"short");
        assert_eq!(cipher.decrypt(&short_base64).unwrap(), None);
    }
}
