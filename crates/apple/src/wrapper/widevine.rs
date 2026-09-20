//! Widevine CDM: license request generation and key unwrapping for the
//! CENC (ISO-23001-7) AAC path served by wrapper-lite `/webplayback`.
//!
//! Ported from `apple-music-downloader/internal/widevine-rip/{cdm,key}`
//! (Go). The protobuf wire format is encoded by hand: only a handful of
//! proto2 messages are involved and the field numbers are frozen by the
//! Widevine protocol, so a generated-code dependency is not warranted.
//!
//! Flow:
//! 1. Build a `WidevineCencHeader` PSSH from the playlist KID and hand it
//!    to [`Cdm::new`].
//! 2. [`Cdm::license_request`] signs a `SignedLicenseRequest` with the
//!    device private key (RSA-PSS/SHA-1) and base64-encodes it for
//!    wrapper-lite's `/license` relay.
//! 3. [`Cdm::content_keys`] unwraps the license response: RSA-OAEP/SHA-1
//!    session key -> AES-CMAC-derived key -> AES-CBC per-key decrypt ->
//!    PKCS#7 unpad -> content keys.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use cbc::cipher::{BlockModeDecrypt, KeyIvInit, block_padding::Pkcs7};
use cmac::{Cmac, KeyInit, Mac};

use super::client::WrapperError;

/// L3 device private key (PKCS#1 PEM), shared with the Go reference
/// implementation.
const DEVICE_PRIVATE_KEY_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA2bO3yvFwNnIHsbDl3MTjKdDsiBWsuZWOGVxInFWAVMp+nffG
YlquTKpJurEry95yprcRB3hYhvA5ghsACidcWPDEPVqqRZ7YXLevyUA+Sn2Jxpvt
OcwyFHbSwruNxprWOkHCT774O4L/wJUt5x2C4iFCrJByjw0omN8u+EHdavvH7ZPn
b3/EZp/cpZa9/+HOkutvBHBvaPp18F8JQhzUQ9MwLuDFTr+QLDB5+Y57Je2tNYDK
xD1K+Ed5Ja0A4OKhPKIwPwPre0nt5scjLba3LSAKtKxiGqFtWO4U7Tf1YrdjJv2o
9o8Sf8qcnbpzvQ4KwFqehuJnB7+W7mdJJw12PQIDAQABAoIBACE32wOMc6LbI3Fp
nKljIYZv6qeZJxHqUBRukGXKZhqKC2fvNsYrMA1irn1eK2CgQL5PkLmjE18DqMLB
e/AQsXagxlDWVMTqx/jdzmTW+KpFHZDAmiIHllypBN/R3oA/gBDDl/KzIQ1zn7Kz
EJ4DUsVObe4G3HQXfepVo8Udx7tbB7X6wHe2kEgFyY3lPdvubik0C4t4ipSD79y7
SfW7XVA5XUQmqN4U2kWM0uSwzd4BA7hqyScJsygf6KgpMWPS2xFZEZQRUpYcBH48
E7YqNrrlYP3yaQ+9Jx56kKS0mvv3vUXS7AfUbU8CiHwD9I3BGwswEUueOGGVeXbx
tFF8s8ECgYEA97BDcL/bt+r3qJF0dxtMB5ZngJbFx9RdsblYepVpblr2UfxnFttO
PoNSKa4W36HuDsun49dkaoABJWdtZs2Hy6q+xvEgozvhMaBVE3spnWnzCT1yTMYL
G02uDEl0dPiTg116bVElaswtqMXvnnpbOTMTe7Ig9sWiUW/GH9RM+N8CgYEA4QHb
+OA0BfczbVQP9B+plt4mAuu4BDm4GPwq1yXOWo3Ct8Ik+HeY1hqOObpfyQMAza+E
e/kP6W8vXpiElGrmiUbTXK4Rzmf+yYeOrvl3D80bFq4GtDNAIQD3jpj6zjlT+Gzw
I501gRx5iPl4fSccRSdpoeri7F9ANtc6EEGFyGMCgYEAjMznWYXHGkL47BtbkIW0
769BQSj0X4dKh8gsEusylugglDSeSbD7RrASGd175T7A/CorU2rTC3OesyubVlBJ
/K4gaykRe5mDh1l0Y3GlE3XyEXObsSb3k1rSMOvkxsWz3X5bJR923MIaxpFWiMlX
aCmvzqZQ9NceUZrvjpJ5+xMCgYAJa8KCESEcftUwZqykVA8Nug9tX+E8jA4hPa2t
hG+3augUOZTCsn87t7Dsydjo2a9W7Vpmtm7sHzOkik5CyJcOeGCxKLimI8SPO5XF
zbwmdTgFIxQ0x1CQETJMTityJwRVCnqjgxmSZlbQXWGmG9UbMCNEHEmUDAjsQuaz
d4racQKBgQDR1Y2kalvleYGrhwcA8LTnIh0rYEfAt9YxNmTi5qDKf5QPvUP2v+WO
fSB5coUqR8LBweHE5V8JgFt74fdLBqZV/k2z/dI0r+EQWmpZ2uPEC0Khk/Sb9iRD
fH7at3PMusrkwZCGZ8beFEAr6icXclV08nPCNGB6WckacfzpAj8Azg==
-----END RSA PRIVATE KEY-----";

/// L3 device client ID blob (base64), shared with the Go reference
/// implementation.
const DEVICE_CLIENT_ID_B64: &str = "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";

/// aes-cbc crate alias for license key unwrapping.
type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;

/// Minimal protobuf writer: each entry is `field_number << 3 | wire_type`.
struct PbWriter {
    buf: Vec<u8>,
}

impl PbWriter {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    fn varint(&mut self, field: u32, value: u64) {
        self.tag(field, 0);
        self.put_varint(value);
    }

    fn bytes(&mut self, field: u32, value: &[u8]) {
        self.tag(field, 2);
        self.put_varint(value.len() as u64);
        self.buf.extend_from_slice(value);
    }

    fn message(&mut self, field: u32, body: &[u8]) {
        self.bytes(field, body);
    }

    fn tag(&mut self, field: u32, wire_type: u8) {
        self.put_varint(((field as u64) << 3) | wire_type as u64);
    }

    fn put_varint(&mut self, mut value: u64) {
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                self.buf.push(byte);
                break;
            }
            self.buf.push(byte | 0x80);
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.buf
    }
}

/// Minimal protobuf reader for the two messages we decode.
struct PbReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> PbReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Next `(field, wire_type)`, or `None` at the end.
    fn next(&mut self) -> Option<(u32, u8)> {
        if self.pos >= self.data.len() {
            return None;
        }
        let tag = self.read_varint()?;
        Some(((tag >> 3) as u32, (tag & 0x7) as u8))
    }

    fn read_varint(&mut self) -> Option<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *self.data.get(self.pos)?;
            self.pos += 1;
            value |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }

    fn read_bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.read_varint()? as usize;
        let start = self.pos;
        self.pos = (self.pos).checked_add(len)?;
        self.data.get(start..self.pos)
    }

    fn skip(&mut self, wire_type: u8) -> Option<()> {
        match wire_type {
            0 => {
                self.read_varint()?;
            }
            1 => self.pos = self.pos.checked_add(8)?,
            2 => {
                let len = self.read_varint()? as usize;
                self.pos = self.pos.checked_add(len)?;
            }
            5 => self.pos = self.pos.checked_add(4)?,
            _ => return None,
        }
        if self.pos > self.data.len() {
            return None;
        }
        Some(())
    }
}

/// A decrypted content key, identified by its key id.
#[derive(Debug, Clone)]
pub struct ContentKey {
    pub key_id: Vec<u8>,
    pub value: [u8; 16],
}

/// One Widevine CDM session bound to a PSSH.
pub struct Cdm {
    private_key: rsa::RsaPrivateKey,
    client_id: Vec<u8>,
    /// The CENC header the license is requested for (serialized).
    pssh_body: Vec<u8>,
    session_id: [u8; 32],
    /// Serialized `LicenseRequest` message — needed again for the CMAC
    /// derivation when unwrapping the license response.
    request_body: Vec<u8>,
}

impl Cdm {
    /// Build a CDM over the default L3 device and a PSSH built from the
    /// playlist KID.
    pub fn new(kid: &[u8]) -> Result<Self, WrapperError> {
        use rsa::pkcs1::DecodeRsaPrivateKey;
        let private_key = rsa::RsaPrivateKey::from_pkcs1_pem(DEVICE_PRIVATE_KEY_PEM)
            .map_err(|e| WrapperError::Message(format!("Parse device private key: {e}")))?;
        let client_id = B64
            .decode(DEVICE_CLIENT_ID_B64)
            .map_err(|e| WrapperError::Message(format!("Decode device client id: {e}")))?;

        // WidevineCencHeader (proto2):
        //   1: algorithm (enum, AESCTR = 1)
        //   2: key_id (repeated bytes)
        //   3: provider (string)
        //   4: content_id (bytes)
        //   5: policy (string)
        // The Go flow builds the header with contentId "" (base64 of the
        // empty string) and empty provider/policy.
        let mut cenc = PbWriter::new();
        cenc.varint(1, 1); // algorithm = AESCTR
        cenc.bytes(2, kid);
        cenc.bytes(3, b""); // provider
        cenc.bytes(4, b""); // content_id (base64 of "")
        cenc.bytes(5, b""); // policy
        let pssh_body = cenc.into_inner();

        let mut session_id = [0u8; 32];
        let alphabet = b"ABCDEF0123456789";
        for byte in &mut session_id[..16] {
            *byte = alphabet[rand::random::<u32>() as usize % alphabet.len()];
        }
        session_id[16] = b'0';
        session_id[17] = b'1';
        for byte in &mut session_id[18..] {
            *byte = b'0';
        }

        Ok(Self {
            private_key,
            client_id,
            pssh_body,
            session_id,
            request_body: Vec::new(),
        })
    }

    /// The PSSH we request for: 32 dummy bytes + CENC header, base64 — the
    /// exact shape wrapper-lite's `/license` expects in `uri`.
    pub fn pssh_b64(&self) -> String {
        let mut w = PbWriter::new();
        w.message(1, &self.pssh_body);
        let mut pssh = Vec::with_capacity(32 + self.pssh_body.len());
        pssh.extend_from_slice(b"0123456789abcdef0123456789abcdef");
        pssh.extend_from_slice(&w.into_inner());
        B64.encode(pssh)
    }

    /// Build and sign the license request; returns the base64 challenge
    /// for wrapper-lite `/license`.
    pub fn license_request(&mut self) -> Result<String, WrapperError> {
        // LicenseRequest (proto2):
        //   1: ClientId        (ClientIdentification message, decoded client id)
        //   2: ContentId       (ContentIdentification)
        //   3: Type            (enum, NEW = 1)
        //   4: RequestTime     (uint32)
        //   6: ProtocolVersion (enum, CURRENT = 21)
        //   7: KeyControlNonce (uint32)
        let mut content_cenc = PbWriter::new();
        content_cenc.message(1, &self.pssh_body); // Pssh
        content_cenc.varint(2, 1); // LicenseType = DEFAULT
        content_cenc.bytes(3, &self.session_id); // RequestId

        let mut content_id = PbWriter::new();
        content_id.message(1, &content_cenc.into_inner()); // CencId

        let mut request = PbWriter::new();
        request.bytes(1, &self.client_id); // ClientId (already a ClientIdentification message)
        request.message(2, &content_id.into_inner()); // ContentId
        request.varint(3, 1); // Type = NEW
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        request.varint(4, now); // RequestTime
        request.varint(6, 21); // ProtocolVersion = CURRENT
        let nonce = rand::random::<u32>() as u64;
        request.varint(7, nonce); // KeyControlNonce
        let request_body = request.into_inner();

        // SignedLicenseRequest:
        //   1: Type      (enum, LICENSE_REQUEST = 1)
        //   2: Msg       (LicenseRequest)
        //   3: Signature (bytes, RSA-PSS/SHA-1 over Msg)
        let mut hasher = sha1::Sha1::new();
        use sha1::Digest;
        hasher.update(&request_body);
        let digest = hasher.finalize();

        // RSA-PSS with SHA-1, salt length = hash length (20).
        let signing_key =
            rsa::pss::SigningKey::<sha1::Sha1>::new_with_salt_len(self.private_key.clone(), 20);
        use rsa::signature::{SignatureEncoding, hazmat::PrehashSigner};
        let signature = signing_key
            .sign_prehash(&digest)
            .map_err(|e| WrapperError::Message(format!("Sign license request: {e}")))?
            .to_vec();
        let mut signed = PbWriter::new();
        signed.varint(1, 1); // Type = LICENSE_REQUEST
        signed.bytes(2, &request_body);
        signed.bytes(3, &signature);
        self.request_body = request_body;

        Ok(B64.encode(signed.into_inner()))
    }

    /// Unwrap a license response into content keys.
    pub fn content_keys(&self, license_b64: &str) -> Result<Vec<ContentKey>, WrapperError> {
        let license = B64
            .decode(license_b64)
            .map_err(|e| WrapperError::Message(format!("Decode license: {e}")))?;

        // SignedLicense:
        //   2: Msg (License)
        //   4: SessionKey (RSA-OAEP/SHA-1 wrapped)
        let mut session_key_enc = None;
        let mut license_msg = None;
        let mut reader = PbReader::new(&license);
        while let Some((field, wire)) = reader.next() {
            match (field, wire) {
                (2, 2) => license_msg = reader.read_bytes().map(|b| b.to_vec()),
                (4, 2) => session_key_enc = reader.read_bytes().map(|b| b.to_vec()),
                _ => {
                    reader.skip(wire).ok_or(malformed("license field"))?;
                }
            }
        }
        let license_msg =
            license_msg.ok_or_else(|| WrapperError::Message("License has no Msg".into()))?;
        let session_key_enc = session_key_enc
            .ok_or_else(|| WrapperError::Message("License has no SessionKey".into()))?;

        // Unwrap the session key with RSA-OAEP/SHA-1.
        let session_key = self
            .private_key
            .decrypt(rsa::Oaep::new::<sha1::Sha1>(), &session_key_enc)
            .map_err(|e| WrapperError::Message(format!("Unwrap session key: {e}")))?;

        // Derive the key-encryption key:
        //   {0x01}"ENCRYPTION"\0 || request_body || {0,0,0,0x80}, AES-CMAC.
        let mut kek_input = Vec::with_capacity(12 + self.request_body.len() + 4);
        kek_input.extend_from_slice(&[
            0x01, b'E', b'N', b'C', b'R', b'Y', b'P', b'T', b'I', b'O', b'N', 0,
        ]);
        kek_input.extend_from_slice(&self.request_body);
        kek_input.extend_from_slice(&[0, 0, 0, 0x80]);
        let mut mac = Cmac::<aes::Aes128>::new_from_slice(&session_key)
            .map_err(|e| WrapperError::Message(format!("Session key size: {e}")))?;
        mac.update(&kek_input);
        let kek = mac.finalize().into_bytes();
        let kek: [u8; 16] = kek
            .as_slice()
            .try_into()
            .map_err(|_| WrapperError::Message("Derived KEK is not 16 bytes".into()))?;

        // License:
        //   3: Key (repeated KeyContainer)
        // KeyContainer:
        //   1: Id (bytes)
        //   2: Iv (bytes)
        //   3: Key (bytes, AES-CBC wrapped with the KEK)
        //   4: Type (enum, CONTENT = 2)
        let mut keys = Vec::new();
        let mut reader = PbReader::new(&license_msg);
        while let Some((field, wire)) = reader.next() {
            if field == 3 && wire == 2 {
                let container = reader.read_bytes().ok_or(malformed("key container"))?;
                let mut id = None;
                let mut iv = None;
                let mut wrapped = None;
                let mut key_type = None;
                let mut inner = PbReader::new(container);
                while let Some((f, w)) = inner.next() {
                    match (f, w) {
                        (1, 2) => id = inner.read_bytes().map(|b| b.to_vec()),
                        (2, 2) => iv = inner.read_bytes().map(|b| b.to_vec()),
                        (3, 2) => wrapped = inner.read_bytes().map(|b| b.to_vec()),
                        (4, 0) => key_type = inner.read_varint().map(|v| v as u32),
                        _ => {
                            inner.skip(w).ok_or(malformed("key field"))?;
                        }
                    }
                }
                // Only CONTENT keys decrypt audio; SIGNING keys exist to
                // verify the license itself.
                if key_type == Some(2) {
                    let id = id.ok_or_else(|| WrapperError::Message("Key has no Id".into()))?;
                    let iv = iv.ok_or_else(|| WrapperError::Message("Key has no Iv".into()))?;
                    let wrapped =
                        wrapped.ok_or_else(|| WrapperError::Message("Key has no Key".into()))?;
                    let decrypted = unwrap_key(&kek, &iv, &wrapped)?;
                    if decrypted.len() < 16 {
                        return Err(WrapperError::Message(
                            "Unwrapped license key shorter than 16 bytes".into(),
                        ));
                    }
                    let mut value = [0u8; 16];
                    value.copy_from_slice(&decrypted[..16]);
                    keys.push(ContentKey { key_id: id, value });
                }
            } else {
                reader.skip(wire).ok_or(malformed("license key field"))?;
            }
        }
        Ok(keys)
    }
}

fn malformed(what: &str) -> WrapperError {
    WrapperError::Message(format!("Malformed license message ({what})"))
}

/// AES-128-CBC decrypt + PKCS#7 unpad of one wrapped license key.
fn unwrap_key(kek: &[u8; 16], iv: &[u8], wrapped: &[u8]) -> Result<Vec<u8>, WrapperError> {
    if iv.len() != 16 {
        return Err(WrapperError::Message(format!(
            "License key IV has {} bytes (want 16)",
            iv.len()
        )));
    }
    if !wrapped.len().is_multiple_of(16) || wrapped.is_empty() {
        return Err(WrapperError::Message(
            "Wrapped license key is not block aligned".into(),
        ));
    }
    let decryptor = Aes128CbcDec::new_from_slices(kek, iv)
        .map_err(|e| WrapperError::Message(format!("Key unwrap init: {e}")))?;
    let mut buf = wrapped.to_vec();
    let plain = decryptor
        .decrypt_padded::<Pkcs7>(&mut buf)
        .map_err(|e| WrapperError::Message(format!("Unwrap license key: {e}")))?;
    Ok(plain.to_vec())
}

/// Recreate the PSSH from a raw KID extracted out of the playlist's
/// `#EXT-X-KEY` data URI. This mirrors the Go `GetPSSH("", kidBase64)`:
/// the CENC header carries the KID, an empty provider, an empty
/// (base64-of-empty) content id and an empty policy.
pub fn pssh_from_kid(kid: &[u8]) -> Vec<u8> {
    let mut cenc = PbWriter::new();
    cenc.varint(1, 1); // algorithm = AESCTR
    cenc.bytes(2, kid);
    cenc.bytes(3, b""); // provider
    cenc.bytes(4, b""); // content_id
    cenc.bytes(5, b""); // policy
    let mut pssh = Vec::with_capacity(32 + 16);
    pssh.extend_from_slice(b"0123456789abcdef0123456789abcdef");
    pssh.extend_from_slice(&cenc.into_inner());
    pssh
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdm_builds_a_well_formed_license_request() {
        let kid = [1u8; 16];
        let mut cdm = Cdm::new(&kid).expect("cdm");
        let challenge = cdm.license_request().expect("challenge");
        let raw = B64.decode(challenge).expect("b64");
        // SignedLicenseRequest.Type = LICENSE_REQUEST is the first field.
        assert_eq!(raw[0] >> 3, 1);
        assert_eq!(raw[0] & 0x7, 0);
        // Two requests must differ (nonce + timestamp).
        let other = cdm.license_request().expect("challenge 2");
        let raw2 = B64.decode(other).expect("b64 2");
        assert_ne!(raw, raw2);
    }

    #[test]
    fn pssh_from_kid_matches_go_layout() {
        let kid = [7u8; 16];
        let pssh = pssh_from_kid(&kid);
        // 32-byte widevine magic prefix.
        assert_eq!(&pssh[..32], b"0123456789abcdef0123456789abcdef");
        // Field 1 (algorithm = AESCTR) then field 2 (KID).
        assert_eq!(pssh[32], 0x08);
        assert_eq!(pssh[33], 0x01);
        assert_eq!(pssh[34], 0x12);
        assert_eq!(pssh[35], 16);
        assert_eq!(&pssh[36..52], &kid);
    }

    #[test]
    fn unwrap_key_round_trips_pkcs7_padded_cbc() {
        // AES-CBC-encrypt 16 key bytes with PKCS#7 padding, then unwrap.
        use cbc::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::Pkcs7};
        type Enc = cbc::Encryptor<aes::Aes128>;
        let kek = [0x42u8; 16];
        let iv = [0x99u8; 16];
        let key_bytes = [0xABu8; 16];
        let wrapped = Enc::new_from_slices(&kek, &iv)
            .expect("enc init")
            .encrypt_padded_vec::<Pkcs7>(&key_bytes);
        assert_eq!(wrapped.len(), 32);
        let plain = unwrap_key(&kek, &iv, &wrapped).expect("unwrap");
        assert_eq!(plain, key_bytes.to_vec());
    }
}
