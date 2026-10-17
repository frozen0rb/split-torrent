//! Minimal bencode decoder/encoder, enough for `.torrent` metainfo.

use std::collections::BTreeMap;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid bencode at byte {pos}: {what}")]
pub struct DecodeError {
    pub pos: usize,
    pub what: &'static str,
}

const MAX_DEPTH: usize = 64;

impl Value {
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Dict(d) => d.get(key.as_bytes()),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Value::Int(i) => out.extend_from_slice(format!("i{i}e").as_bytes()),
            Value::Bytes(b) => {
                out.extend_from_slice(format!("{}:", b.len()).as_bytes());
                out.extend_from_slice(b);
            }
            Value::List(l) => {
                out.push(b'l');
                l.iter().for_each(|v| v.encode(out));
                out.push(b'e');
            }
            Value::Dict(d) => {
                out.push(b'd');
                for (k, v) in d {
                    Value::Bytes(k.clone()).encode(out);
                    v.encode(out);
                }
                out.push(b'e');
            }
        }
    }
}

/// Decodes one complete value; trailing bytes are an error.
pub fn decode(buf: &[u8]) -> Result<Value, DecodeError> {
    let mut p = Parser { buf, pos: 0 };
    let v = p.value(0, None)?;
    if p.pos != buf.len() {
        return Err(p.err("trailing data"));
    }
    Ok(v)
}

/// Decodes `buf` and also returns the raw byte range of the top-level
/// dictionary's value under `key` (used for the info-hash).
pub fn decode_with_span(
    buf: &[u8],
    key: &[u8],
) -> Result<(Value, Option<Range<usize>>), DecodeError> {
    let mut p = Parser { buf, pos: 0 };
    let mut span = None;
    let v = p.value(0, Some((key, &mut span)))?;
    if p.pos != buf.len() {
        return Err(p.err("trailing data"));
    }
    Ok((v, span))
}

struct Parser<'a> {
    buf: &'a [u8],
    pos: usize,
}

type SpanReq<'k, 's> = Option<(&'k [u8], &'s mut Option<Range<usize>>)>;

impl Parser<'_> {
    fn err(&self, what: &'static str) -> DecodeError {
        DecodeError {
            pos: self.pos,
            what,
        }
    }

    fn peek(&self) -> Result<u8, DecodeError> {
        self.buf
            .get(self.pos)
            .copied()
            .ok_or_else(|| self.err("unexpected end"))
    }

    fn value(&mut self, depth: usize, span: SpanReq) -> Result<Value, DecodeError> {
        if depth > MAX_DEPTH {
            return Err(self.err("nesting too deep"));
        }
        match self.peek()? {
            b'i' => {
                self.pos += 1;
                let n = self.number(b'e')?;
                Ok(Value::Int(n))
            }
            b'l' => {
                self.pos += 1;
                let mut l = Vec::new();
                while self.peek()? != b'e' {
                    l.push(self.value(depth + 1, None)?);
                }
                self.pos += 1;
                Ok(Value::List(l))
            }
            b'd' => {
                self.pos += 1;
                let mut d = BTreeMap::new();
                let (want, slot) = match span {
                    Some((k, s)) => (Some(k), Some(s)),
                    None => (None, None),
                };
                let mut slot = slot;
                while self.peek()? != b'e' {
                    let k = self.bytes()?;
                    let start = self.pos;
                    let v = self.value(depth + 1, None)?;
                    if want == Some(k.as_slice()) {
                        if let Some(s) = slot.as_deref_mut() {
                            *s = Some(start..self.pos);
                        }
                    }
                    d.insert(k, v);
                }
                self.pos += 1;
                Ok(Value::Dict(d))
            }
            b'0'..=b'9' => Ok(Value::Bytes(self.bytes()?)),
            _ => Err(self.err("unexpected byte")),
        }
    }

    fn number(&mut self, end: u8) -> Result<i64, DecodeError> {
        let start = self.pos;
        while self.peek()? != end {
            self.pos += 1;
        }
        let s =
            std::str::from_utf8(&self.buf[start..self.pos]).map_err(|_| self.err("bad number"))?;
        self.pos += 1;
        s.parse().map_err(|_| self.err("bad number"))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        if !self.peek()?.is_ascii_digit() {
            return Err(self.err("expected byte string"));
        }
        let len = self.number(b':')?;
        let len = usize::try_from(len).map_err(|_| self.err("negative length"))?;
        let end = self
            .pos
            .checked_add(len)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| self.err("string past end"))?;
        let b = self.buf[self.pos..end].to_vec();
        self.pos = end;
        Ok(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_span() {
        let src = b"d4:infod4:name3:abce3:numi-5e1:xl1:ai1eee";
        let (v, span) = decode_with_span(src, b"info").unwrap();
        assert_eq!(&src[span.unwrap()], b"d4:name3:abce");
        assert_eq!(v.get("num").unwrap().as_int(), Some(-5));
        let mut out = Vec::new();
        v.encode(&mut out);
        assert_eq!(out, src);
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode(b"").is_err());
        assert!(decode(b"i1").is_err());
        assert!(decode(b"5:ab").is_err());
        assert!(decode(b"i1ei2e").is_err());
        assert!(decode(b"x").is_err());
        assert!(decode(&[b'l'; 1000]).is_err());
    }
}
