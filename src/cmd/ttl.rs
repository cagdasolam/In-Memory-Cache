use bytes::Bytes;
use crate::db::Db;
use crate::frame::Frame;

#[derive(Debug)]
pub struct Ttl {
    key: Bytes,
    millis: bool,
}

impl Ttl {
    pub fn new(key: Bytes, millis: bool) -> Self {
        Self { key, millis }
    }

    pub fn parse_frames(mut args: std::vec::IntoIter<Frame>, millis: bool) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for command".into()),
        };

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for command".into());
        }

        Ok(Self { key, millis })
    }

    pub fn apply(self, db: &Db) -> Frame {
        let res = if self.millis {
            db.pttl(&self.key)
        } else {
            db.ttl(&self.key)
        };
        Frame::Integer(res)
    }
}

