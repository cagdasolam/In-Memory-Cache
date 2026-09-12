use bytes::Bytes;
use crate::db::Db;
use crate::frame::Frame;

#[derive(Debug)]
pub enum SetCmd {
    Sadd { key: Bytes, members: Vec<Bytes> },
    Smembers { key: Bytes },
    Srem { key: Bytes, members: Vec<Bytes> },
    Sismember { key: Bytes, member: Bytes },
}

impl SetCmd {
    pub fn parse_sadd(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'sadd' command".into()),
        };

        let mut members = Vec::new();
        for frame in args {
            let m = match frame {
                Frame::Bulk(b) => b,
                Frame::Simple(s) => Bytes::from(s),
                _ => return Err("ERR syntax error".into()),
            };
            members.push(m);
        }

        if members.is_empty() {
            return Err("ERR wrong number of arguments for 'sadd' command".into());
        }

        Ok(SetCmd::Sadd { key, members })
    }

    pub fn parse_smembers(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'smembers' command".into()),
        };

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'smembers' command".into());
        }

        Ok(SetCmd::Smembers { key })
    }

    pub fn parse_srem(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'srem' command".into()),
        };

        let mut members = Vec::new();
        for frame in args {
            let m = match frame {
                Frame::Bulk(b) => b,
                Frame::Simple(s) => Bytes::from(s),
                _ => return Err("ERR syntax error".into()),
            };
            members.push(m);
        }

        if members.is_empty() {
            return Err("ERR wrong number of arguments for 'srem' command".into());
        }

        Ok(SetCmd::Srem { key, members })
    }

    pub fn parse_sismember(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'sismember' command".into()),
        };

        let member = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'sismember' command".into()),
        };

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'sismember' command".into());
        }

        Ok(SetCmd::Sismember { key, member })
    }

    pub fn apply(self, db: &Db) -> Frame {
        match self {
            SetCmd::Sadd { key, members } => match db.sadd(key, members) {
                Ok(count) => Frame::Integer(count as i64),
                Err(err) => Frame::Error(err),
            },
            SetCmd::Smembers { key } => match db.smembers(&key) {
                Ok(members) => Frame::Array(members.into_iter().map(Frame::Bulk).collect()),
                Err(err) => Frame::Error(err),
            },
            SetCmd::Srem { key, members } => match db.srem(&key, &members) {
                Ok(count) => Frame::Integer(count as i64),
                Err(err) => Frame::Error(err),
            },
            SetCmd::Sismember { key, member } => match db.sismember(&key, &member) {
                Ok(exists) => Frame::Integer(if exists { 1 } else { 0 }),
                Err(err) => Frame::Error(err),
            },
        }
    }
}

