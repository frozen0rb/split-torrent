pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

pub fn speed(n: u64) -> String {
    if n == 0 {
        "–".into()
    } else {
        format!("{}/s", bytes(n))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn human_units() {
        assert_eq!(super::bytes(0), "0 B");
        assert_eq!(super::bytes(1023), "1023 B");
        assert_eq!(super::bytes(1536), "1.5 KiB");
        assert_eq!(super::bytes(384 << 20), "384.0 MiB");
        assert_eq!(super::bytes(5 << 40), "5.0 TiB");
        assert_eq!(super::speed(0), "–");
        assert_eq!(super::speed(2 << 20), "2.0 MiB/s");
    }
}
