//! TLS client config that trusts exactly one certificate, identified by the
//! SHA-256 fingerprint of its DER encoding (as printed by `st-server`).

use std::fmt;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint(pub [u8; 32]);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("certificate fingerprint must be 64 hex digits (colons and spaces allowed)")]
pub struct BadFingerprint;

impl Fingerprint {
    pub fn of(cert_der: &[u8]) -> Self {
        Self(Sha256::digest(cert_der).into())
    }

    /// Accepts `AB:CD:…`, `abcd…`, optionally prefixed with `sha256:` or `SHA256 Fingerprint=`.
    pub fn parse(s: &str) -> Result<Self, BadFingerprint> {
        let s = s.trim();
        let s = s.rsplit_once(['=', ' ']).map_or(s, |(_, r)| r);
        let s = s
            .strip_prefix("sha256:")
            .or_else(|| s.strip_prefix("SHA256:"))
            .unwrap_or(s);
        let hex: Vec<u8> = s.bytes().filter(|b| !matches!(b, b':' | b'-')).collect();
        if hex.len() != 64 {
            return Err(BadFingerprint);
        }
        let mut out = [0u8; 32];
        for (o, pair) in out.iter_mut().zip(hex.as_chunks::<2>().0) {
            let pair = std::str::from_utf8(pair).map_err(|_| BadFingerprint)?;
            *o = u8::from_str_radix(pair, 16).map_err(|_| BadFingerprint)?;
        }
        Ok(Self(out))
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, b) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(":")?;
            }
            write!(f, "{b:02X}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fingerprint({self})")
    }
}

#[derive(Debug)]
struct PinnedVerifier {
    pin: Fingerprint,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let got = Fingerprint::of(end_entity);
        if got == self.pin {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "server certificate fingerprint {got} does not match the pinned {}",
                self.pin
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn client_config(pin: Fingerprint) -> Arc<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("ring supports the default protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier { pin, provider }))
        .with_no_client_auth();
    Arc::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_formats() {
        let fp = Fingerprint([0xab; 32]);
        let colon = fp.to_string();
        assert_eq!(colon.len(), 95);
        assert_eq!(Fingerprint::parse(&colon), Ok(fp));
        assert_eq!(Fingerprint::parse(&colon.to_lowercase()), Ok(fp));
        assert_eq!(Fingerprint::parse(&"ab".repeat(32)), Ok(fp));
        assert_eq!(
            Fingerprint::parse(&format!("sha256:{}", "AB".repeat(32))),
            Ok(fp)
        );
        assert_eq!(
            Fingerprint::parse(&format!("sha256 Fingerprint={colon}")),
            Ok(fp)
        );
        assert_eq!(Fingerprint::parse("abcd"), Err(BadFingerprint));
        assert_eq!(Fingerprint::parse(&"zz".repeat(32)), Err(BadFingerprint));
    }
}
