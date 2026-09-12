pub mod del;
pub mod echo;
pub mod expire;
pub mod get;
pub mod hash;
pub mod list;
pub mod ping;
pub mod pubsub;
pub mod set;
pub mod set_cmd;
pub mod ttl;

use crate::db::Db;
use crate::frame::Frame;
use crate::pubsub::PubSub;

pub use del::Del;
pub use echo::Echo;
pub use expire::Expire;
pub use get::Get;
pub use hash::HashCmd;
pub use list::ListCmd;
pub use ping::Ping;
pub use pubsub::PubSubCmd;
pub use set::Set;
pub use set_cmd::SetCmd;
pub use ttl::Ttl;

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Get(Get),
    Set(Set),
    Del(Del),
    Expire(Expire),
    Ttl(Ttl),
    List(ListCmd),
    Hash(HashCmd),
    SetCmd(SetCmd),
    PubSub(PubSubCmd),
    /// Handle client handshake commands
    Command,
    Info,
    Client,
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
            "DEL" => Ok(Command::Del(Del::parse_frames(iter)?)),
            "EXPIRE" => Ok(Command::Expire(Expire::parse_expire(iter)?)),
            "PEXPIRE" => Ok(Command::Expire(Expire::parse_pexpire(iter)?)),
            "TTL" => Ok(Command::Ttl(Ttl::parse_frames(iter, false)?)),
            "PTTL" => Ok(Command::Ttl(Ttl::parse_frames(iter, true)?)),

            // List commands
            "LPUSH" => Ok(Command::List(ListCmd::parse_lpush(iter)?)),
            "RPUSH" => Ok(Command::List(ListCmd::parse_rpush(iter)?)),
            "LPOP" => Ok(Command::List(ListCmd::parse_lpop(iter)?)),
            "RPOP" => Ok(Command::List(ListCmd::parse_rpop(iter)?)),
            "LRANGE" => Ok(Command::List(ListCmd::parse_lrange(iter)?)),

            // Hash commands
            "HSET" => Ok(Command::Hash(HashCmd::parse_hset(iter)?)),
            "HGET" => Ok(Command::Hash(HashCmd::parse_hget(iter)?)),
            "HDEL" => Ok(Command::Hash(HashCmd::parse_hdel(iter)?)),
            "HGETALL" => Ok(Command::Hash(HashCmd::parse_hgetall(iter)?)),

            // Set commands
            "SADD" => Ok(Command::SetCmd(SetCmd::parse_sadd(iter)?)),
            "SMEMBERS" => Ok(Command::SetCmd(SetCmd::parse_smembers(iter)?)),
            "SREM" => Ok(Command::SetCmd(SetCmd::parse_srem(iter)?)),
            "SISMEMBER" => Ok(Command::SetCmd(SetCmd::parse_sismember(iter)?)),

            // Pub/Sub commands
            "PUBLISH" => Ok(Command::PubSub(PubSubCmd::parse_publish(iter)?)),
            "SUBSCRIBE" => Ok(Command::PubSub(PubSubCmd::parse_subscribe(iter)?)),

            "COMMAND" => Ok(Command::Command),
            "INFO" => Ok(Command::Info),
            "CLIENT" => Ok(Command::Client),
            other => Err(format!("ERR unknown command '{}'", other).into()),
        }
    }

    /// Check if this command modifies database state (for AOF recording).
    pub fn is_write(&self) -> bool {
        match self {
            Command::Set(_) => true,
            Command::Del(_) => true,
            Command::Expire(_) => true,
            Command::List(ListCmd::Lpush { .. }) => true,
            Command::List(ListCmd::Rpush { .. }) => true,
            Command::List(ListCmd::Lpop { .. }) => true,
            Command::List(ListCmd::Rpop { .. }) => true,
            Command::Hash(HashCmd::Hset { .. }) => true,
            Command::Hash(HashCmd::Hdel { .. }) => true,
            Command::SetCmd(SetCmd::Sadd { .. }) => true,
            Command::SetCmd(SetCmd::Srem { .. }) => true,
            _ => false,
        }
    }

    /// Execute the command against the database (and pubsub if applicable).
    pub fn apply(self, db: &Db) -> Frame {
        match self {
            Command::Ping(ping) => ping.apply(),
            Command::Echo(echo) => echo.apply(),
            Command::Get(get) => get.apply(db),
            Command::Set(set) => set.apply(db),
            Command::Del(del) => del.apply(db),
            Command::Expire(expire) => expire.apply(db),
            Command::Ttl(ttl) => ttl.apply(db),
            Command::List(list) => list.apply(db),
            Command::Hash(hash) => hash.apply(db),
            Command::SetCmd(set_cmd) => set_cmd.apply(db),
            Command::PubSub(_) => Frame::Error("ERR pubsub executed in wrong context".into()),
            Command::Command => Frame::Array(vec![]),
            Command::Info => Frame::Bulk(bytes::Bytes::from_static(
                b"# Server\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\nos:Linux\r\narch_bits:64\r\nrole:master\r\nloading:0\r\n",
            )),
            Command::Client => Frame::Simple("OK".into()),
        }
    }

    /// Execute command with pubsub support.
    pub fn apply_with_pubsub(self, db: &Db, pubsub: &PubSub) -> Frame {
        match self {
            Command::PubSub(pubsub_cmd) => pubsub_cmd.apply_publish(pubsub),
            other => other.apply(db),
        }
    }
}
