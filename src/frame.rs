use bytes::{Buf, Bytes};
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FrameError {
    #[error("not enough data is available to parse a frame")]
    Incomplete,
    #[error("invalid frame format: {0}")]
    Other(String),
}

impl From<String> for FrameError {
    fn from(src: String) -> FrameError {
        FrameError::Other(src)
    }
}

impl From<&str> for FrameError {
    fn from(src: &str) -> FrameError {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for FrameError {
    fn from(_src: FromUtf8Error) -> FrameError {
        "protocol error; invalid utf8 string".into()
    }
}

impl From<TryFromIntError> for FrameError {
    fn from(_src: TryFromIntError) -> FrameError {
        "protocol error; invalid integer conversion".into()
    }
}

impl Frame {
    /// Check if an entire frame can be decoded from `src`.
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), FrameError> {
        if !src.has_remaining() {
            return Err(FrameError::Incomplete);
        }

        match peek_u8(src)? {
            b'+' => {
                get_u8(src)?;
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_u8(src)?;
                get_line(src)?;
                Ok(())
            }
            b':' => {
                get_u8(src)?;
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                get_u8(src)?;
                if peek_u8(src)? == b'-' {
                    let line = get_line(src)?;
                    if line == b"-1" {
                        return Ok(());
                    }
                    return Err("invalid null bulk string".into());
                }

                let len = get_decimal(src)?;
                if len < 0 {
                    return Err("negative bulk string length".into());
                }
                let len = usize::try_from(len)?;

                // Skip len bytes + CRLF (\r\n)
                if src.remaining() < len + 2 {
                    return Err(FrameError::Incomplete);
                }
                src.advance(len);
                if get_u8(src)? != b'\r' || get_u8(src)? != b'\n' {
                    return Err("bulk string not followed by CRLF".into());
                }
                Ok(())
            }
            b'*' => {
                get_u8(src)?;
                let len = get_decimal(src)?;
                if len < 0 {
                    return Err("negative array length".into());
                }
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            _ => {
                // Inline command support (e.g. PING_INLINE, telnet)
                let _ = get_line(src)?;
                Ok(())
            }
        }
    }

    /// Parse a frame from `src`. Assumes `check` succeeded.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
        match peek_u8(src)? {
            b'+' => {
                get_u8(src)?;
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Simple(string))
            }
            b'-' => {
                get_u8(src)?;
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Error(string))
            }
            b':' => {
                get_u8(src)?;
                let val = get_decimal(src)?;
                Ok(Frame::Integer(val))
            }
            b'$' => {
                get_u8(src)?;
                if peek_u8(src)? == b'-' {
                    let line = get_line(src)?;
                    if line == b"-1" {
                        return Ok(Frame::Null);
                    }
                    return Err("invalid null bulk string".into());
                }

                let len = get_decimal(src)?;
                let len = usize::try_from(len)?;

                let n = len;
                if src.remaining() < n + 2 {
                    return Err(FrameError::Incomplete);
                }

                let data = Bytes::copy_from_slice(&src.chunk()[..n]);
                src.advance(n);

                if get_u8(src)? != b'\r' || get_u8(src)? != b'\n' {
                    return Err("bulk string not followed by CRLF".into());
                }

                Ok(Frame::Bulk(data))
            }
            b'*' => {
                get_u8(src)?;
                let len = get_decimal(src)?;
                let len = usize::try_from(len)?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => {
                // Inline command: split by whitespace
                let line = get_line(src)?;
                let s = std::str::from_utf8(line).map_err(|_| "invalid utf8 inline command")?;
                let parts: Vec<Frame> = s
                    .split_whitespace()
                    .map(|p| Frame::Bulk(Bytes::copy_from_slice(p.as_bytes())))
                    .collect();
                Ok(Frame::Array(parts))
            }
        }
    }

    /// Serialize the frame into RESP format and append to `dst`.
    pub fn write_to_buf(&self, dst: &mut Vec<u8>) {
        match self {
            Frame::Simple(val) => {
                dst.push(b'+');
                dst.extend_from_slice(val.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Error(val) => {
                dst.push(b'-');
                dst.extend_from_slice(val.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Integer(val) => {
                dst.push(b':');
                dst.extend_from_slice(val.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Null => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            Frame::Bulk(val) => {
                dst.push(b'$');
                dst.extend_from_slice(val.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(val);
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Array(frames) => {
                dst.push(b'*');
                dst.extend_from_slice(frames.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for frame in frames {
                    frame.write_to_buf(dst);
                }
            }
        }
    }

    /// Helper to convert Frame to a debug string
    pub fn to_error(&self) -> Option<&str> {
        match self {
            Frame::Error(msg) => Some(msg),
            _ => None,
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, FrameError> {
    if !src.has_remaining() {
        return Err(FrameError::Incomplete);
    }
    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, FrameError> {
    if !src.has_remaining() {
        return Err(FrameError::Incomplete);
    }
    Ok(src.get_u8())
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], FrameError> {
    let start = src.position() as usize;
    let end = src.get_ref().len();

    for i in start..end {
        if src.get_ref()[i] == b'\r' && i + 1 < end && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(FrameError::Incomplete)
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, FrameError> {
    let line = get_line(src)?;
    let s = std::str::from_utf8(line).map_err(|_| FrameError::Other("invalid utf8 integer".into()))?;
    s.parse::<i64>().map_err(|_| FrameError::Other("invalid integer digits".into()))
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Frame::Simple(s) => write!(f, "\"{}\"", s),
            Frame::Error(e) => write!(f, "ERR: {}", e),
            Frame::Integer(i) => write!(f, "{}", i),
            Frame::Bulk(b) => match std::str::from_utf8(b) {
                Ok(s) => write!(f, "\"{}\"", s),
                Err(_) => write!(f, "{:?}", b),
            },
            Frame::Null => write!(f, "(nil)"),
            Frame::Array(arr) => {
                write!(f, "[")?;
                for (i, item) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string() {
        let mut cursor = Cursor::new(&b"+OK\r\n"[..]);
        assert!(Frame::check(&mut cursor).is_ok());
        cursor.set_position(0);
        assert_eq!(Frame::parse(&mut cursor).unwrap(), Frame::Simple("OK".to_string()));
    }

    #[test]
    fn test_error() {
        let mut cursor = Cursor::new(&b"-ERR unknown command\r\n"[..]);
        assert!(Frame::check(&mut cursor).is_ok());
        cursor.set_position(0);
        assert_eq!(
            Frame::parse(&mut cursor).unwrap(),
            Frame::Error("ERR unknown command".to_string())
        );
    }

    #[test]
    fn test_integer() {
        let mut cursor = Cursor::new(&b":1000\r\n"[..]);
        assert!(Frame::check(&mut cursor).is_ok());
        cursor.set_position(0);
        assert_eq!(Frame::parse(&mut cursor).unwrap(), Frame::Integer(1000));

        let mut neg_cursor = Cursor::new(&b":-42\r\n"[..]);
        assert!(Frame::check(&mut neg_cursor).is_ok());
        neg_cursor.set_position(0);
        assert_eq!(Frame::parse(&mut neg_cursor).unwrap(), Frame::Integer(-42));
    }

    #[test]
    fn test_bulk_string() {
        let mut cursor = Cursor::new(&b"$5\r\nhello\r\n"[..]);
        assert!(Frame::check(&mut cursor).is_ok());
        cursor.set_position(0);
        assert_eq!(
            Frame::parse(&mut cursor).unwrap(),
            Frame::Bulk(Bytes::from_static(b"hello"))
        );
    }

    #[test]
    fn test_null_bulk_string() {
        let mut cursor = Cursor::new(&b"$-1\r\n"[..]);
        assert!(Frame::check(&mut cursor).is_ok());
        cursor.set_position(0);
        assert_eq!(Frame::parse(&mut cursor).unwrap(), Frame::Null);
    }

    #[test]
    fn test_array() {
        let mut cursor = Cursor::new(&b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"[..]);
        assert!(Frame::check(&mut cursor).is_ok());
        cursor.set_position(0);
        assert_eq!(
            Frame::parse(&mut cursor).unwrap(),
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"GET")),
                Frame::Bulk(Bytes::from_static(b"key")),
            ])
        );
    }

    #[test]
    fn test_incomplete_frame() {
        let mut cursor = Cursor::new(&b"*2\r\n$3\r\nGET\r\n"[..]);
        assert_eq!(Frame::check(&mut cursor).unwrap_err(), FrameError::Incomplete);
    }

    #[test]
    fn test_serialization() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            Frame::Bulk(Bytes::from_static(b"name")),
            Frame::Bulk(Bytes::from_static(b"antigravity")),
        ]);
        let mut buf = Vec::new();
        frame.write_to_buf(&mut buf);
        assert_eq!(
            buf,
            b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$11\r\nantigravity\r\n"
        );
    }

    #[test]
    fn test_inline_command() {
        let mut cursor = Cursor::new(&b"PING\r\n"[..]);
        assert!(Frame::check(&mut cursor).is_ok());
        cursor.set_position(0);
        assert_eq!(
            Frame::parse(&mut cursor).unwrap(),
            Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))])
        );

        let mut cursor2 = Cursor::new(&b"SET key val\r\n"[..]);
        assert!(Frame::check(&mut cursor2).is_ok());
        cursor2.set_position(0);
        assert_eq!(
            Frame::parse(&mut cursor2).unwrap(),
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"SET")),
                Frame::Bulk(Bytes::from_static(b"key")),
                Frame::Bulk(Bytes::from_static(b"val")),
            ])
        );
    }
}

