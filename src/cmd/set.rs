use bytes::Bytes;
use crate::db::Db;
use crate::frame::Frame;

#[derive(Debug)]
pub struct Set {
    key: Bytes,
    value: Bytes,
}

impl Set {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
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

        if args.next().is_some() {
            // Note: in Phase 2, options like EX, PX will be parsed here
            return Err("ERR syntax error".into());
        }

        Ok(Self { key, value })
    }

    pub fn apply(self, db: &Db) -> Frame {
        db.set(self.key, self.value);
        Frame::Simple("OK".to_string())
    }
}

