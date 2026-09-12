use bytes::Bytes;
use crate::frame::Frame;
use crate::pubsub::PubSub;

#[derive(Debug)]
pub enum PubSubCmd {
    Publish { channel: String, message: Bytes },
    Subscribe { channels: Vec<String> },
}

impl PubSubCmd {
    pub fn parse_publish(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let channel = match args.next() {
            Some(Frame::Bulk(b)) => String::from_utf8(b.to_vec()).map_err(|_| "ERR invalid channel name")?,
            Some(Frame::Simple(s)) => s,
            _ => return Err("ERR wrong number of arguments for 'publish' command".into()),
        };

        let message = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'publish' command".into()),
        };

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'publish' command".into());
        }

        Ok(PubSubCmd::Publish { channel, message })
    }

    pub fn parse_subscribe(args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let mut channels = Vec::new();
        for frame in args {
            let ch = match frame {
                Frame::Bulk(b) => String::from_utf8(b.to_vec()).map_err(|_| "ERR invalid channel name")?,
                Frame::Simple(s) => s,
                _ => return Err("ERR syntax error".into()),
            };
            channels.push(ch);
        }

        if channels.is_empty() {
            return Err("ERR wrong number of arguments for 'subscribe' command".into());
        }

        Ok(PubSubCmd::Subscribe { channels })
    }

    pub fn apply_publish(self, pubsub: &PubSub) -> Frame {
        match self {
            PubSubCmd::Publish { channel, message } => {
                let count = pubsub.publish(&channel, message);
                Frame::Integer(count as i64)
            }
            _ => Frame::Error("ERR invalid pubsub command execution".into()),
        }
    }
}

