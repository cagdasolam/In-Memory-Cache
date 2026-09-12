use bytes::Bytes;
use crate::db::Db;
use crate::frame::Frame;

#[derive(Debug)]
pub enum HashCmd {
    Hset { key: Bytes, fields: Vec<(Bytes, Bytes)> },
    Hget { key: Bytes, field: Bytes },
    Hdel { key: Bytes, fields: Vec<Bytes> },
    Hgetall { key: Bytes },
}

impl HashCmd {
    pub fn parse_hset(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'hset' command".into()),
        };

        let mut fields = Vec::new();
        while let Some(field_frame) = args.next() {
            let field = match field_frame {
                Frame::Bulk(b) => b,
                Frame::Simple(s) => Bytes::from(s),
                _ => return Err("ERR syntax error".into()),
            };

            let val_frame = args.next().ok_or_else(|| "ERR wrong number of arguments for 'hset' command")?;
            let value = match val_frame {
                Frame::Bulk(b) => b,
                Frame::Simple(s) => Bytes::from(s),
                _ => return Err("ERR syntax error".into()),
            };

            fields.push((field, value));
        }

        if fields.is_empty() {
            return Err("ERR wrong number of arguments for 'hset' command".into());
        }

        Ok(HashCmd::Hset { key, fields })
    }

    pub fn parse_hget(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'hget' command".into()),
        };

        let field = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'hget' command".into()),
        };

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'hget' command".into());
        }

        Ok(HashCmd::Hget { key, field })
    }

    pub fn parse_hdel(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'hdel' command".into()),
        };

        let mut fields = Vec::new();
        for frame in args {
            let field = match frame {
                Frame::Bulk(b) => b,
                Frame::Simple(s) => Bytes::from(s),
                _ => return Err("ERR syntax error".into()),
            };
            fields.push(field);
        }

        if fields.is_empty() {
            return Err("ERR wrong number of arguments for 'hdel' command".into());
        }

        Ok(HashCmd::Hdel { key, fields })
    }

    pub fn parse_hgetall(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let key = match args.next() {
            Some(Frame::Bulk(b)) => b,
            Some(Frame::Simple(s)) => Bytes::from(s),
            _ => return Err("ERR wrong number of arguments for 'hgetall' command".into()),
        };

        if args.next().is_some() {
            return Err("ERR wrong number of arguments for 'hgetall' command".into());
        }

        Ok(HashCmd::Hgetall { key })
    }

    pub fn apply(self, db: &Db) -> Frame {
        match self {
            HashCmd::Hset { key, fields } => match db.hset(key, fields) {
                Ok(count) => Frame::Integer(count as i64),
                Err(err) => Frame::Error(err),
            },
            HashCmd::Hget { key, field } => match db.hget(&key, &field) {
                Ok(Some(val)) => Frame::Bulk(val),
                Ok(None) => Frame::Null,
                Err(err) => Frame::Error(err),
            },
            HashCmd::Hdel { key, fields } => match db.hdel(&key, &fields) {
                Ok(count) => Frame::Integer(count as i64),
                Err(err) => Frame::Error(err),
            },
            HashCmd::Hgetall { key } => match db.hgetall(&key) {
                Ok(pairs) => {
                    let mut out = Vec::with_capacity(pairs.len() * 2);
                    for (k, v) in pairs {
                        out.push(Frame::Bulk(k));
                        out.push(Frame::Bulk(v));
                    }
                    Frame::Array(out)
                }
                Err(err) => Frame::Error(err),
            },
        }
    }
}

