use bytes::Bytes;
use crate::db::Db;
use crate::frame::Frame;

#[derive(Debug)]
pub struct Del {
    keys: Vec<Bytes>,
}

impl Del {
    pub fn new(keys: Vec<Bytes>) -> Self {
        Self { keys }
    }

    pub fn parse_frames(args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let mut keys = Vec::new();
        for frame in args {
            let key = match frame {
                Frame::Bulk(b) => b,
                Frame::Simple(s) => Bytes::from(s),
                _ => return Err("ERR invalid argument for 'del' command".into()),
            };
            keys.push(key);
        }

        if keys.is_empty() {
            return Err("ERR wrong number of arguments for 'del' command".into());
        }

        Ok(Self { keys })
    }

    pub fn apply(self, db: &Db) -> Frame {
        let count = db.del(&self.keys);
        Frame::Integer(count as i64)
    }
}

