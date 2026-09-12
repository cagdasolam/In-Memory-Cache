use bytes::Bytes;
use crate::db::Db;
use crate::frame::Frame;
use std::time::Duration;

#[derive(Debug)]
pub struct Expire {
    key: Bytes,
    duration: Duration,
}

impl Expire {
    pub fn new(key: Bytes, duration: Duration) -> Self {
        Self { key, duration }
    }

    pub fn parse_expire(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'expire' command".into()),
        };

        let sec_frame = args
            .next()
            .ok_or_else(|| "ERR wrong number of arguments for 'expire' command")?;

        let sec_str = match sec_frame {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| "ERR value is not an integer or out of range")?,
            Frame::Simple(s) => s,
            Frame::Integer(i) => i.to_string(),
            _ => return Err("ERR value is not an integer or out of range".into()),
        };

        let secs: u64 = sec_str
            .parse()
            .map_err(|_| "ERR value is not an integer or out of range")?;

        if args.next().is_some() {
            return Err("ERR syntax error".into());
        }

        Ok(Self {
            key,
            duration: Duration::from_secs(secs),
        })
    }

    pub fn parse_pexpire(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'pexpire' command".into()),
        };

        let ms_frame = args
            .next()
            .ok_or_else(|| "ERR wrong number of arguments for 'pexpire' command")?;

        let ms_str = match ms_frame {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| "ERR value is not an integer or out of range")?,
            Frame::Simple(s) => s,
            Frame::Integer(i) => i.to_string(),
            _ => return Err("ERR value is not an integer or out of range".into()),
        };

        let ms: u64 = ms_str
            .parse()
            .map_err(|_| "ERR value is not an integer or out of range")?;

        if args.next().is_some() {
            return Err("ERR syntax error".into());
        }

        Ok(Self {
            key,
            duration: Duration::from_millis(ms),
        })
    }

    pub fn apply(self, db: &Db) -> Frame {
        let success = db.expire(&self.key, self.duration);
        Frame::Integer(if success { 1 } else { 0 })
    }
}

