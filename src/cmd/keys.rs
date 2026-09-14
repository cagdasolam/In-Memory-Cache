use crate::db::Db;
use crate::frame::Frame;
use bytes::Bytes;

#[derive(Debug)]
pub struct Keys {
    pattern: Bytes,
}

impl Keys {
    pub fn new(pattern: Bytes) -> Self {
        Self { pattern }
    }

    pub fn parse_frames(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let pattern_frame = args.next().ok_or_else(|| {
            crate::Error::from("ERR wrong number of arguments for 'keys' command")
        })?;

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'keys' command".into());
        }

        let pattern = match pattern_frame {
            Frame::Bulk(b) => b,
            Frame::Simple(s) => Bytes::from(s),
            _ => return Err("ERR invalid argument for 'keys' command".into()),
        };

        Ok(Self { pattern })
    }

    pub fn apply(self, db: &Db) -> Frame {
        let matched = db.keys(&self.pattern);
        Frame::Array(matched.into_iter().map(Frame::Bulk).collect())
    }
}
