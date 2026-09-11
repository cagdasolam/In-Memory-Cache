use bytes::Bytes;
use crate::frame::Frame;

#[derive(Debug, Default)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    pub fn new(msg: Option<Bytes>) -> Self {
        Self { msg }
    }

    pub fn parse_frames(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        match args.next() {
            Some(Frame::Bulk(b)) => {
                if args.next().is_some() {
                    return Err("ERR wrong number of arguments for 'ping' command".into());
                }
                Ok(Self { msg: Some(b) })
            }
            Some(Frame::Simple(s)) => {
                if args.next().is_some() {
                    return Err("ERR wrong number of arguments for 'ping' command".into());
                }
                Ok(Self { msg: Some(Bytes::from(s)) })
            }
            Some(_) => Err("ERR invalid argument for 'ping' command".into()),
            None => Ok(Self { msg: None }),
        }
    }

    pub fn apply(self) -> Frame {
        match self.msg {
            Some(msg) => Frame::Bulk(msg),
            None => Frame::Simple("PONG".to_string()),
        }
    }
}

