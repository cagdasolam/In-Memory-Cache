use crate::db::Db;
use crate::frame::Frame;
use bytes::Bytes;

#[derive(Debug)]
pub struct Scan {
    cursor: u64,
    pattern: Option<Bytes>,
    count: usize,
    type_filter: Option<String>,
}

impl Scan {
    pub fn new(
        cursor: u64,
        pattern: Option<Bytes>,
        count: usize,
        type_filter: Option<String>,
    ) -> Self {
        Self {
            cursor,
            pattern,
            count,
            type_filter,
        }
    }

    pub fn parse_frames(mut args: std::vec::IntoIter<Frame>) -> Result<Self, crate::Error> {
        let cursor_frame = args.next().ok_or_else(|| {
            crate::Error::from("ERR wrong number of arguments for 'scan' command")
        })?;

        let cursor_str = match cursor_frame {
            Frame::Bulk(b) => String::from_utf8(b.to_vec()).map_err(|_| "ERR invalid cursor")?,
            Frame::Simple(s) => s,
            Frame::Integer(i) => {
                if i < 0 {
                    return Err("ERR invalid cursor".into());
                }
                i.to_string()
            }
            _ => return Err("ERR invalid cursor".into()),
        };

        let cursor = cursor_str
            .parse::<u64>()
            .map_err(|_| "ERR invalid cursor")?;

        let mut pattern = None;
        let mut count = 10;
        let mut type_filter = None;

        while let Some(frame) = args.next() {
            let opt = match frame {
                Frame::Bulk(b) => String::from_utf8(b.to_vec()).map_err(|_| "ERR syntax error")?,
                Frame::Simple(s) => s,
                _ => return Err("ERR syntax error".into()),
            };

            match opt.to_uppercase().as_str() {
                "MATCH" => {
                    let pat_frame = args
                        .next()
                        .ok_or_else(|| crate::Error::from("ERR syntax error"))?;
                    let pat_bytes = match pat_frame {
                        Frame::Bulk(b) => b,
                        Frame::Simple(s) => Bytes::from(s),
                        _ => return Err("ERR syntax error".into()),
                    };
                    pattern = Some(pat_bytes);
                }
                "COUNT" => {
                    let count_frame = args
                        .next()
                        .ok_or_else(|| crate::Error::from("ERR syntax error"))?;
                    let count_val = match count_frame {
                        Frame::Bulk(b) => {
                            let s = String::from_utf8(b.to_vec())
                                .map_err(|_| "ERR value is not an integer or out of range")?;
                            s.parse::<usize>()
                                .map_err(|_| "ERR value is not an integer or out of range")?
                        }
                        Frame::Simple(s) => s
                            .parse::<usize>()
                            .map_err(|_| "ERR value is not an integer or out of range")?,
                        Frame::Integer(i) => {
                            if i <= 0 {
                                return Err("ERR value is not an integer or out of range".into());
                            }
                            i as usize
                        }
                        _ => return Err("ERR value is not an integer or out of range".into()),
                    };
                    count = count_val;
                }
                "TYPE" => {
                    let type_frame = args
                        .next()
                        .ok_or_else(|| crate::Error::from("ERR syntax error"))?;
                    let t = match type_frame {
                        Frame::Bulk(b) => {
                            String::from_utf8(b.to_vec()).map_err(|_| "ERR syntax error")?
                        }
                        Frame::Simple(s) => s,
                        _ => return Err("ERR syntax error".into()),
                    };
                    type_filter = Some(t);
                }
                _ => return Err("ERR syntax error".into()),
            }
        }

        Ok(Self {
            cursor,
            pattern,
            count,
            type_filter,
        })
    }

    pub fn apply(self, db: &Db) -> Frame {
        let (next_cursor, matched_keys) = db.scan(
            self.cursor,
            self.pattern.as_deref(),
            self.count,
            self.type_filter.as_deref(),
        );

        let keys_frames = matched_keys.into_iter().map(Frame::Bulk).collect();
        Frame::Array(vec![
            Frame::Bulk(Bytes::from(next_cursor.to_string())),
            Frame::Array(keys_frames),
        ])
    }
}
