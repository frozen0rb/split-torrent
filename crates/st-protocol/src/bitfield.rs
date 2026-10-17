use serde::{Deserialize, Serialize};

/// Piece bitfield in BitTorrent order: bit 7 of byte 0 is piece 0.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bitfield {
    len: u32,
    #[serde(with = "serde_bytes_compat")]
    bytes: Vec<u8>,
}

impl Bitfield {
    pub fn new(len: u32) -> Self {
        Self {
            len,
            bytes: vec![0; Self::byte_len(len)],
        }
    }

    pub fn full(len: u32) -> Self {
        let mut b = Self::new(len);
        for i in 0..len {
            b.set(i, true);
        }
        b
    }

    /// Builds a bitfield from raw bytes. Returns `None` if `bytes` has the
    /// wrong size or spare trailing bits are set.
    pub fn from_bytes(bytes: Vec<u8>, len: u32) -> Option<Self> {
        if bytes.len() != Self::byte_len(len) {
            return None;
        }
        let b = Self { len, bytes };
        let spare = (b.bytes.len() as u64 * 8 - u64::from(len)) as u32;
        if spare > 0 {
            let mask = (1u8 << spare) - 1;
            if b.bytes.last().is_some_and(|last| last & mask != 0) {
                return None;
            }
        }
        Some(b)
    }

    fn byte_len(len: u32) -> usize {
        (len as usize).div_ceil(8)
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Out-of-range indices read as `false`.
    pub fn get(&self, index: u32) -> bool {
        if index >= self.len {
            return false;
        }
        self.bytes[(index / 8) as usize] & (0x80 >> (index % 8)) != 0
    }

    /// # Panics
    /// If `index >= len`.
    pub fn set(&mut self, index: u32, value: bool) {
        assert!(index < self.len, "bit {index} out of range {}", self.len);
        let byte = &mut self.bytes[(index / 8) as usize];
        let mask = 0x80 >> (index % 8);
        if value {
            *byte |= mask;
        } else {
            *byte &= !mask;
        }
    }

    pub fn count_ones(&self) -> u32 {
        self.bytes.iter().map(|b| b.count_ones()).sum()
    }

    pub fn is_full(&self) -> bool {
        self.count_ones() == self.len
    }

    pub fn iter_ones(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.len).filter(|&i| self.get(i))
    }

    pub fn iter_zeros(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.len).filter(|&i| !self.get(i))
    }
}

/// Serializes `Vec<u8>` as a serde byte string (one length prefix + raw bytes).
mod serde_bytes_compat {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = Vec<u8>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("bytes")
            }
            fn visit_bytes<E>(self, v: &[u8]) -> Result<Vec<u8>, E> {
                Ok(v.to_vec())
            }
            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
                Ok(v)
            }
        }
        d.deserialize_byte_buf(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_msb_first() {
        let mut b = Bitfield::new(10);
        assert_eq!(b.as_bytes().len(), 2);
        b.set(0, true);
        b.set(9, true);
        assert_eq!(b.as_bytes(), &[0x80, 0x40]);
        assert!(b.get(0) && b.get(9) && !b.get(1));
        assert!(!b.get(100));
        assert_eq!(b.count_ones(), 2);
        assert_eq!(b.iter_ones().collect::<Vec<_>>(), vec![0, 9]);
        b.set(0, false);
        assert_eq!(b.iter_ones().collect::<Vec<_>>(), vec![9]);
        assert_eq!(b.iter_zeros().count(), 9);
    }

    #[test]
    fn full_and_from_bytes() {
        let f = Bitfield::full(10);
        assert!(f.is_full());
        assert_eq!(f.as_bytes(), &[0xff, 0xc0]);
        assert_eq!(Bitfield::from_bytes(vec![0xff, 0xc0], 10), Some(f));
        assert_eq!(Bitfield::from_bytes(vec![0xff, 0xc1], 10), None);
        assert_eq!(Bitfield::from_bytes(vec![0xff], 10), None);
        assert!(Bitfield::from_bytes(vec![0xff], 8).unwrap().is_full());
        assert!(Bitfield::new(0).is_empty());
    }

    #[test]
    #[should_panic]
    fn set_out_of_range_panics() {
        Bitfield::new(3).set(3, true);
    }
}
