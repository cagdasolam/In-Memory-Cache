use bytes::Bytes;
use crate::db::Db;
use crate::frame::Frame;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Set {
    key: Bytes,
    value: Bytes,
    expires_at: Option<Instant>,
}

impl Set {
    pub fn new(key: Bytes, value: Bytes, expires_at: Option<Instant>) -> Self {
        Self {
            key,
            value,
            expires_at,
        }
    }

    pub fn parse_frames(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'set' command".into()),
        };

        let value = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'set' command".into()),
        };

        let mut expire_duration = None;

        while let Some(arg) = args.next() {
            let opt = match arg {
                Frame::Bulk(b) => {
                    String::from_utf8(b.to_vec()).map_err(|_| "ERR syntax error")?
                }
                Frame::Simple(s) => s,
                _ => return Err("ERR syntax error".into()),
            };

            match opt.to_uppercase().as_str() {
                "EX" => {
                    let sec_frame = args.next().ok_or_else(|| "ERR syntax error")?;
                    let sec_str = match sec_frame {
                        Frame::Bulk(b) => String::from_utf8(b.to_vec())
                            .map_err(|_| "ERR value is not an integer or out of range")?,
                        Frame::Simple(s) => s,
                        Frame::Integer(i) => i.to_string(),
                        _ => return Err("ERR syntax error".into()),
                    };
                    let secs: u64 = sec_str
                        .parse()
                        .map_err(|_| "ERR value is not an integer or out of range")?;
                    expire_duration = Some(Duration::from_secs(secs));
                }
                "PX" => {
                    let ms_frame = args.next().ok_or_else(|| "ERR syntax error")?;
                    let ms_str = match ms_frame {
                        Frame::Bulk(b) => String::from_utf8(b.to_vec())
                            .map_err(|_| "ERR value is not an integer or out of range")?,
                        Frame::Simple(s) => s,
                        Frame::Integer(i) => i.to_string(),
                        _ => return Err("ERR syntax error".into()),
                    };
                    let ms: u64 = ms_str
                        .parse()
                        .map_err(|_| "ERR value is not an integer or out of range")?;
                    expire_duration = Some(Duration::from_millis(ms));
                }
                _ => return Err("ERR syntax error".into()),
            }
        }

        let expires_at = expire_duration.map(|d| Instant::now() + d);
        Ok(Self {
            key,
            value,
            expires_at,
        })
    }

    pub fn apply(self, db: &Db) -> Frame {
        db.set(self.key, self.value, self.expires_at);
        Frame::Simple("OK".to_string())
    }
}
