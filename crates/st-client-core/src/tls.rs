//! TLS client config that trusts exactly one certificate, identified by the
//! SHA-256 fingerprint of its DER encoding (as printed by `st-server`).

use std::fmt;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use st_protocol::{cert_fingerprint, format_fingerprint, parse_fingerprint};

/// Name sent in SNI; the server's self-signed certificate is issued for it.
pub const SERVER_NAME: &str = "st-server";

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint(pub [u8; 32]);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("certificate fingerprint must be 64 hex digits (colons and spaces allowed)")]
pub struct BadFingerprint;

impl Fingerprint {
    pub fn of(cert_der: &[u8]) -> Self {
        Self(cert_fingerprint(cert_der))
    }

    /// Accepts `AB:CD:…` or `abcd…`, optionally prefixed with `sha256:`, as
    /// printed by `st-server show`.
    pub fn parse(s: &str) -> Result<Self, BadFingerprint> {
        parse_fingerprint(s).map(Self).ok_or(BadFingerprint)
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_fingerprint(&self.0))
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
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("TLS 1.2 is not supported".into()))
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
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("ring supports TLS 1.3")
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
        assert_eq!(Fingerprint::parse(&format!("sha256:{colon}")), Ok(fp));
        assert_eq!(Fingerprint::parse("abcd"), Err(BadFingerprint));
        assert_eq!(Fingerprint::parse(&"zz".repeat(32)), Err(BadFingerprint));
    }
}
