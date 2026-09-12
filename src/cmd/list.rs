use bytes::Bytes;
use crate::db::Db;
use crate::frame::Frame;

#[derive(Debug)]
pub enum ListCmd {
    Lpush { key: Bytes, elements: Vec<Bytes> },
    Rpush { key: Bytes, elements: Vec<Bytes> },
    Lpop { key: Bytes },
    Rpop { key: Bytes },
    Lrange { key: Bytes, start: i64, stop: i64 },
}

impl ListCmd {
    pub fn parse_lpush(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'lpush' command".into()),
        };

        let mut elements = Vec::new();
        for frame in args {
            let el = match frame {
                Frame::Bulk(b) => b,
                Frame::Simple(s) => Bytes::from(s),
                _ => return Err("ERR syntax error".into()),
            };
            elements.push(el);
        }

        if elements.is_empty() {
            return Err("ERR wrong number of arguments for 'lpush' command".into());
        }

        Ok(ListCmd::Lpush { key, elements })
    }

    pub fn parse_rpush(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'rpush' command".into()),
        };

        let mut elements = Vec::new();
        for frame in args {
            let el = match frame {
                Frame::Bulk(b) => b,
                Frame::Simple(s) => Bytes::from(s),
                _ => return Err("ERR syntax error".into()),
            };
            elements.push(el);
        }

        if elements.is_empty() {
            return Err("ERR wrong number of arguments for 'rpush' command".into());
        }

        Ok(ListCmd::Rpush { key, elements })
    }

    pub fn parse_lpop(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'lpop' command".into()),
        };

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'lpop' command".into());
        }

        Ok(ListCmd::Lpop { key })
    }

    pub fn parse_rpop(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'rpop' command".into()),
        };

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'rpop' command".into());
        }

        Ok(ListCmd::Rpop { key })
    }

    pub fn parse_lrange(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'lrange' command".into()),
        };

        let start_str = match args.next() {
            Some(Frame::Bulk(b)) => String::from_utf8(b.to_vec()).map_err(|_| "ERR value is not an integer")?,
            Some(Frame::Simple(s)) => s,
            Some(Frame::Integer(i)) => i.to_string(),
            _ => return Err("ERR wrong number of arguments for 'lrange' command".into()),
        };
        let start: i64 = start_str.parse().map_err(|_| "ERR value is not an integer or out of range")?;

        let stop_str = match args.next() {
            Some(Frame::Bulk(b)) => String::from_utf8(b.to_vec()).map_err(|_| "ERR value is not an integer")?,
            Some(Frame::Simple(s)) => s,
            Some(Frame::Integer(i)) => i.to_string(),
            _ => return Err("ERR wrong number of arguments for 'lrange' command".into()),
        };
        let stop: i64 = stop_str.parse().map_err(|_| "ERR value is not an integer or out of range")?;

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'lrange' command".into());
        }

        Ok(ListCmd::Lrange { key, start, stop })
    }

    pub fn apply(self, db: &Db) -> Frame {
        match self {
            ListCmd::Lpush { key, elements } => match db.lpush(key, elements) {
                Ok(len) => Frame::Integer(len as i64),
                Err(err) => Frame::Error(err),
            },
            ListCmd::Rpush { key, elements } => match db.rpush(key, elements) {
                Ok(len) => Frame::Integer(len as i64),
                Err(err) => Frame::Error(err),
            },
            ListCmd::Lpop { key } => match db.lpop(&key) {
                Ok(Some(val)) => Frame::Bulk(val),
                Ok(None) => Frame::Null,
                Err(err) => Frame::Error(err),
            },
            ListCmd::Rpop { key } => match db.rpop(&key) {
                Ok(Some(val)) => Frame::Bulk(val),
                Ok(None) => Frame::Null,
                Err(err) => Frame::Error(err),
            },
            ListCmd::Lrange { key, start, stop } => match db.lrange(&key, start, stop) {
                Ok(elements) => Frame::Array(elements.into_iter().map(Frame::Bulk).collect()),
                Err(err) => Frame::Error(err),
            },
        }
    }
}

