use bytes::Bytes;
use crate::db::Db;
use crate::frame::Frame;

#[derive(Debug)]
pub struct Get {
    key: Bytes,
}

impl Get {
    pub fn new(key: Bytes) -> Self {
        Self { key }
    }

    pub fn parse_frames(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'get' command".into()),
        };

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'get' command".into());
        }

        Ok(Self { key })
    }

    pub fn apply(self, db: &Db) -> Frame {
        match db.get(&self.key) {
            Some(value) => Frame::Bulk(value),
            None => Frame::Null,
        }
    }
}

