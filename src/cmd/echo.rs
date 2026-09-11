use bytes::Bytes;
use crate::frame::Frame;

#[derive(Debug)]
pub struct Echo {
    msg: Bytes,
}

impl Echo {
    pub fn new(msg: Bytes) -> Self {
        Self { msg }
    }

    pub fn parse_frames(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        match args.next() {
            Some(Frame::Bulk(b)) => {
                if args.next().is_some() {
                    return Err("ERR wrong number of arguments for 'echo' command".into());
                }
                Ok(Self { msg: b })
            }
            Some(Frame::Simple(s)) => {
                if args.next().is_some() {
                    return Err("ERR wrong number of arguments for 'echo' command".into());
                }
                Ok(Self { msg: Bytes::from(s) })
            }
            Some(_) => Err("ERR invalid argument for 'echo' command".into()),
            None => Err("ERR wrong number of arguments for 'echo' command".into()),
        }
    }

    pub fn apply(self) -> Frame {
        Frame::Bulk(self.msg)
    }
}

