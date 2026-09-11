pub mod echo;
pub mod get;
pub mod ping;
pub mod set;

use crate::db::Db;
use crate::frame::Frame;

pub use echo::Echo;
pub use get::Get;
pub use ping::Ping;
pub use set::Set;

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Get(Get),
    Set(Set),
    /// Handle client handshake command
    Command,
}

impl Command {
    /// Parse a command from a Frame::Array received over the wire.
    pub fn from_frame(frame: Frame) -> Result<Command, crate::Error> {
        let frames = match frame {
            Frame::Array(frames) => frames,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };

        let mut iter = frames.into_iter();
        let cmd_frame = iter
            .next()
            .ok_or_else(|| crate::Error::from("protocol error; empty command array"))?;

        let cmd_name = match cmd_frame {
            Frame::Bulk(bytes) => {
                String::from_utf8(bytes.to_vec()).map_err(|_| "protocol error; invalid command name")?
            }
            Frame::Simple(s) => s,
            _ => return Err("protocol error; command name must be a string".into()),
        };

        match cmd_name.to_uppercase().as_str() {
            "PING" => Ok(Command::Ping(Ping::parse_frames(iter)?)),
            "ECHO" => Ok(Command::Echo(Echo::parse_frames(iter)?)),
            "GET" => Ok(Command::Get(Get::parse_frames(iter)?)),
            "SET" => Ok(Command::Set(Set::parse_frames(iter)?)),
            "COMMAND" => Ok(Command::Command),
            other => Err(format!("ERR unknown command '{}'", other).into()),
        }
    }

    /// Execute the command against the database and produce the response Frame.
    pub fn apply(self, db: &Db) -> Frame {
        match self {
            Command::Ping(ping) => ping.apply(),
            Command::Echo(echo) => echo.apply(),
            Command::Get(get) => get.apply(db),
            Command::Set(set) => set.apply(db),
            Command::Command => Frame::Array(vec![]),
        }
    }
}

