use sha2::{Digest, Sha256};

/// SHA-256 of a DER-encoded certificate; what the client pins.
pub fn cert_fingerprint(cert_der: &[u8]) -> [u8; 32] {
    Sha256::digest(cert_der).into()
}

/// Formats as uppercase hex pairs separated by `:` (`AB:CD:...`).
pub fn format_fingerprint(fp: &[u8; 32]) -> String {
    fp.iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// Parses 64 hex digits, ignoring case, `:`, spaces and an optional
/// `sha256:` prefix.
pub fn parse_fingerprint(s: &str) -> Option<[u8; 32]> {
    let s = s.trim();
    let s = s
        .strip_prefix("sha256:")
        .or_else(|| s.strip_prefix("SHA256:"))
        .unwrap_or(s);
    let digits: Vec<u8> = s
        .bytes()
        .filter(|c| !matches!(c, b':' | b' ' | b'-'))
        .collect();
    if digits.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in digits.chunks(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out[i] = (hi * 16 + lo) as u8;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_parse_roundtrip() {
        let fp = cert_fingerprint(b"cert");
        let s = format_fingerprint(&fp);
        assert_eq!(s.len(), 32 * 3 - 1);
        assert_eq!(parse_fingerprint(&s), Some(fp));
        assert_eq!(
            parse_fingerprint(&s.replace(':', "").to_lowercase()),
            Some(fp)
        );
        assert_eq!(parse_fingerprint(&format!("sha256:{s}")), Some(fp));
        assert_eq!(parse_fingerprint("AB:CD"), None);
        assert_eq!(parse_fingerprint(&"zz".repeat(32)), None);
    }
}
