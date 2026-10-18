//! IPv6 availability.
//!
//! On hosts booted with `ipv6.disable=1`, and in some containers (OpenVZ,
//! LXC), the kernel has no IPv6 stack and creating any `AF_INET6` socket fails
//! with `EAFNOSUPPORT`. librqbit binds `[::]` by default, so the server must
//! detect this and stay on IPv4.

use std::net::{Ipv6Addr, UdpSocket};

/// Whether an IPv6 socket can be created and bound.
pub fn ipv6_available() -> bool {
    match UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0)) {
        Ok(_) => true,
        Err(e) => {
            tracing::debug!("IPv6 socket test failed: {e}");
            false
        }
    }
}

/// Applies the `ipv6` setting: `None` detects, `Some(true)` requires IPv6.
pub fn resolve_ipv6(setting: Option<bool>) -> anyhow::Result<bool> {
    let available = || ipv6_available();
    match setting {
        Some(false) => Ok(false),
        Some(true) => {
            anyhow::ensure!(
                available(),
                "ipv6 = true, but this host cannot create IPv6 sockets (IPv6 disabled in the kernel?)"
            );
            Ok(true)
        }
        None => {
            let v6 = available();
            if v6 {
                tracing::info!("IPv6 available, BitTorrent uses IPv4 and IPv6");
            } else {
                tracing::warn!("IPv6 is not available on this host, BitTorrent uses IPv4 only");
            }
            Ok(v6)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_false_never_probes() {
        assert!(!resolve_ipv6(Some(false)).unwrap());
    }

    /// `scripts/test-without-ipv6.sh` sets this to prove its shim is active.
    #[test]
    fn probe_detects_missing_ipv6_stack() {
        if std::env::var_os("ST_EXPECT_NO_IPV6").is_some() {
            assert!(!ipv6_available());
            assert!(resolve_ipv6(Some(true)).is_err());
        }
    }

    #[test]
    fn auto_matches_probe() {
        assert_eq!(resolve_ipv6(None).unwrap(), ipv6_available());
        assert_eq!(resolve_ipv6(Some(true)).is_ok(), ipv6_available());
    }
}
