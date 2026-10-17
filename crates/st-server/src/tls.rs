use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use rand::distr::{Alphanumeric, SampleString};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::TlsAcceptor;

const CERT_FILE: &str = "cert.der";
const KEY_FILE: &str = "key.der";
const TOKEN_FILE: &str = "token";

pub struct TlsIdentity {
    pub cert_der: Vec<u8>,
    key_der: Vec<u8>,
}

impl TlsIdentity {
    pub fn generate() -> anyhow::Result<Self> {
        let ck = rcgen::generate_simple_self_signed(vec!["st-server".to_string()])?;
        Ok(Self {
            cert_der: ck.cert.der().to_vec(),
            key_der: ck.key_pair.serialize_der(),
        })
    }

    /// Loads `cert.der`/`key.der` from `dir`, generating them on first start.
    pub fn load_or_generate(dir: &Path) -> anyhow::Result<Self> {
        let cert_path = dir.join(CERT_FILE);
        let key_path = dir.join(KEY_FILE);
        if cert_path.exists() && key_path.exists() {
            return Ok(Self {
                cert_der: std::fs::read(&cert_path)
                    .with_context(|| format!("reading {}", cert_path.display()))?,
                key_der: std::fs::read(&key_path)
                    .with_context(|| format!("reading {}", key_path.display()))?,
            });
        }
        let id = Self::generate()?;
        create_private_dir(dir)?;
        write_private(&key_path, &id.key_der)?;
        std::fs::write(&cert_path, &id.cert_der)
            .with_context(|| format!("writing {}", cert_path.display()))?;
        tracing::info!(path = %cert_path.display(), "generated new self-signed TLS certificate");
        Ok(id)
    }

    pub fn fingerprint(&self) -> String {
        st_protocol::format_fingerprint(&st_protocol::cert_fingerprint(&self.cert_der))
    }

    pub fn acceptor(&self) -> anyhow::Result<TlsAcceptor> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(self.cert_der.clone())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone())),
            )
            .context("bad TLS key/certificate")?;
        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

pub fn generate_token() -> String {
    Alphanumeric.sample_string(&mut rand::rng(), 32)
}

/// Reads `dir/token`, creating it with a random token if missing. Returns the
/// token and whether it was just generated.
pub fn load_or_generate_token(dir: &Path) -> anyhow::Result<(String, bool)> {
    let path = dir.join(TOKEN_FILE);
    if path.exists() {
        let t = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        return Ok((t.trim().to_string(), false));
    }
    let t = generate_token();
    create_private_dir(dir)?;
    write_private(&path, t.as_bytes())?;
    Ok((t, true))
}

fn create_private_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn write_private(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    use std::io::Write;
    opts.open(path)
        .and_then(|mut f| f.write_all(data))
        .with_context(|| format!("writing {}", path.display()))
}

/// Constant-time comparison so the token cannot be guessed byte by byte.
pub fn token_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_persists_and_fingerprint_is_stable() {
        let dir = tempfile::tempdir().unwrap();
        let a = TlsIdentity::load_or_generate(dir.path()).unwrap();
        let b = TlsIdentity::load_or_generate(dir.path()).unwrap();
        assert_eq!(a.fingerprint(), b.fingerprint());
        assert!(st_protocol::parse_fingerprint(&a.fingerprint()).is_some());
        a.acceptor().unwrap();
    }

    #[test]
    fn token_file() {
        let dir = tempfile::tempdir().unwrap();
        let (t1, new1) = load_or_generate_token(dir.path()).unwrap();
        let (t2, new2) = load_or_generate_token(dir.path()).unwrap();
        assert!(new1 && !new2);
        assert_eq!(t1, t2);
        assert_eq!(t1.len(), 32);
        assert!(token_eq(&t1, &t2));
        assert!(!token_eq(&t1, "x"));
    }
}
