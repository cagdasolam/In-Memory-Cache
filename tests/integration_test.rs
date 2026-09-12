use bytes::Bytes;
use in_memory_cache::cmd::PubSubCmd;
use in_memory_cache::{Aof, Command, Connection, Db, Frame, PubSub};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, StreamMap};

async fn spawn_test_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let db = Db::new();
    let pubsub = Arc::new(PubSub::new());

    tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(res) => res,
                Err(_) => break,
            };
            let db = db.clone();
            let pubsub = pubsub.clone();
            tokio::spawn(async move {
                let mut connection = Connection::new(socket);
                while let Ok(Some(frame)) = connection.read_frame().await {
                    let cmd = match Command::from_frame(frame) {
                        Ok(c) => c,
                        Err(e) => {
                            let _ = connection.write_frame(&Frame::Error(e.to_string())).await;
                            continue;
                        }
                    };

                    if let Command::PubSub(PubSubCmd::Subscribe { channels }) = cmd {
                        let mut subs = StreamMap::new();
                        for (i, ch) in channels.into_iter().enumerate() {
                            let rx = pubsub.subscribe(&ch);
                            subs.insert(ch.clone(), BroadcastStream::new(rx));
                            let confirm = Frame::Array(vec![
                                Frame::Bulk(Bytes::from_static(b"subscribe")),
                                Frame::Bulk(Bytes::from(ch)),
                                Frame::Integer((i + 1) as i64),
                            ]);
                            let _ = connection.write_frame(&confirm).await;
                        }

                        loop {
                            tokio::select! {
                                Some((channel, Ok(msg))) = subs.next() => {
                                    let push = Frame::Array(vec![
                                        Frame::Bulk(Bytes::from_static(b"message")),
                                        Frame::Bulk(Bytes::from(channel)),
                                        Frame::Bulk(msg),
                                    ]);
                                    if connection.write_frame(&push).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(Some(f)) = connection.read_frame() => {
                                    if let Ok(Command::Ping(_)) = Command::from_frame(f) {
                                        let pong = Frame::Array(vec![
                                            Frame::Bulk(Bytes::from_static(b"pong")),
                                            Frame::Bulk(Bytes::from_static(b"")),
                                        ]);
                                        let _ = connection.write_frame(&pong).await;
                                    }
                                }
                                else => break,
                            }
                        }
                        return;
                    }

                    let response = cmd.apply_with_pubsub(&db, &pubsub);
                    if connection.write_frame(&response).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    addr
}

#[tokio::test]
async fn test_ping() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    let ping_frame = Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]);
    conn.write_frame(&ping_frame).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Simple("PONG".to_string()));
}

#[tokio::test]
async fn test_echo() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    let echo_frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"ECHO")),
        Frame::Bulk(Bytes::from_static(b"rust-cache")),
    ]);
    conn.write_frame(&echo_frame).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Bulk(Bytes::from_static(b"rust-cache")));
}

#[tokio::test]
async fn test_set_and_get() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    let set_frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"foo")),
        Frame::Bulk(Bytes::from_static(b"bar")),
    ]);
    conn.write_frame(&set_frame).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Simple("OK".to_string()));

    let get_frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"GET")),
        Frame::Bulk(Bytes::from_static(b"foo")),
    ]);
    conn.write_frame(&get_frame).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Bulk(Bytes::from_static(b"bar")));
}

#[tokio::test]
async fn test_del() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    let set1 = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"d1")),
        Frame::Bulk(Bytes::from_static(b"v1")),
    ]);
    conn.write_frame(&set1).await.unwrap();
    let _ = conn.read_frame().await.unwrap();

    let del = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"DEL")),
        Frame::Bulk(Bytes::from_static(b"d1")),
    ]);
    conn.write_frame(&del).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(1));
}

#[tokio::test]
async fn test_list_commands() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    // RPUSH mylist "world"
    let rpush = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"RPUSH")),
        Frame::Bulk(Bytes::from_static(b"mylist")),
        Frame::Bulk(Bytes::from_static(b"world")),
    ]);
    conn.write_frame(&rpush).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(1));

    // LPUSH mylist "hello"
    let lpush = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"LPUSH")),
        Frame::Bulk(Bytes::from_static(b"mylist")),
        Frame::Bulk(Bytes::from_static(b"hello")),
    ]);
    conn.write_frame(&lpush).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(2));

    // LRANGE mylist 0 -1 -> ["hello", "world"]
    let lrange = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"LRANGE")),
        Frame::Bulk(Bytes::from_static(b"mylist")),
        Frame::Bulk(Bytes::from_static(b"0")),
        Frame::Bulk(Bytes::from_static(b"-1")),
    ]);
    conn.write_frame(&lrange).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(
        res,
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"hello")),
            Frame::Bulk(Bytes::from_static(b"world")),
        ])
    );

    // LPOP mylist -> "hello"
    let lpop = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"LPOP")),
        Frame::Bulk(Bytes::from_static(b"mylist")),
    ]);
    conn.write_frame(&lpop).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Bulk(Bytes::from_static(b"hello")));
}

#[tokio::test]
async fn test_hash_commands() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    // HSET user:1 name "cagdas" role "engineer"
    let hset = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"HSET")),
        Frame::Bulk(Bytes::from_static(b"user:1")),
        Frame::Bulk(Bytes::from_static(b"name")),
        Frame::Bulk(Bytes::from_static(b"cagdas")),
        Frame::Bulk(Bytes::from_static(b"role")),
        Frame::Bulk(Bytes::from_static(b"engineer")),
    ]);
    conn.write_frame(&hset).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(2));

    // HGET user:1 name -> "cagdas"
    let hget = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"HGET")),
        Frame::Bulk(Bytes::from_static(b"user:1")),
        Frame::Bulk(Bytes::from_static(b"name")),
    ]);
    conn.write_frame(&hget).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Bulk(Bytes::from_static(b"cagdas")));

    // HDEL user:1 role -> 1
    let hdel = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"HDEL")),
        Frame::Bulk(Bytes::from_static(b"user:1")),
        Frame::Bulk(Bytes::from_static(b"role")),
    ]);
    conn.write_frame(&hdel).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(1));
}

#[tokio::test]
async fn test_set_commands() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    // SADD tags "rust" "cache" "memory"
    let sadd = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SADD")),
        Frame::Bulk(Bytes::from_static(b"tags")),
        Frame::Bulk(Bytes::from_static(b"rust")),
        Frame::Bulk(Bytes::from_static(b"cache")),
        Frame::Bulk(Bytes::from_static(b"memory")),
    ]);
    conn.write_frame(&sadd).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(3));

    // SISMEMBER tags "rust" -> 1
    let sismember = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SISMEMBER")),
        Frame::Bulk(Bytes::from_static(b"tags")),
        Frame::Bulk(Bytes::from_static(b"rust")),
    ]);
    conn.write_frame(&sismember).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(1));

    // SREM tags "memory" -> 1
    let srem = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SREM")),
        Frame::Bulk(Bytes::from_static(b"tags")),
        Frame::Bulk(Bytes::from_static(b"memory")),
    ]);
    conn.write_frame(&srem).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(1));
}

#[tokio::test]
async fn test_pubsub() {
    let addr = spawn_test_server().await;

    // Subscriber connection
    let sub_socket = TcpStream::connect(addr).await.unwrap();
    let mut sub_conn = Connection::new(sub_socket);

    // SUBSCRIBE news
    let sub_cmd = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SUBSCRIBE")),
        Frame::Bulk(Bytes::from_static(b"news")),
    ]);
    sub_conn.write_frame(&sub_cmd).await.unwrap();
    let confirm = sub_conn.read_frame().await.unwrap().unwrap();
    assert_eq!(
        confirm,
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"subscribe")),
            Frame::Bulk(Bytes::from_static(b"news")),
            Frame::Integer(1),
        ])
    );

    // Publisher connection
    let pub_socket = TcpStream::connect(addr).await.unwrap();
    let mut pub_conn = Connection::new(pub_socket);

    // PUBLISH news "Rust 2026 Released"
    let pub_cmd = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"PUBLISH")),
        Frame::Bulk(Bytes::from_static(b"news")),
        Frame::Bulk(Bytes::from_static(b"Rust 2026 Released")),
    ]);
    pub_conn.write_frame(&pub_cmd).await.unwrap();
    let pub_res = pub_conn.read_frame().await.unwrap().unwrap();
    assert_eq!(pub_res, Frame::Integer(1)); // 1 active subscriber received it

    // Subscriber receives the message
    let msg_frame = sub_conn.read_frame().await.unwrap().unwrap();
    assert_eq!(
        msg_frame,
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"message")),
            Frame::Bulk(Bytes::from_static(b"news")),
            Frame::Bulk(Bytes::from_static(b"Rust 2026 Released")),
        ])
    );
}

#[tokio::test]
async fn test_aof_recovery() {
    let tmp_file = std::env::temp_dir().join(format!("test_aof_{}.aof", fastrand::u64(..)));
    let aof = Aof::start(tmp_file.clone());

    // Record commands to AOF
    let cmd1 = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"saved_key")),
        Frame::Bulk(Bytes::from_static(b"persisted_val")),
    ]);
    let cmd2 = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"RPUSH")),
        Frame::Bulk(Bytes::from_static(b"saved_list")),
        Frame::Bulk(Bytes::from_static(b"item1")),
        Frame::Bulk(Bytes::from_static(b"item2")),
    ]);

    aof.record(&cmd1).await;
    aof.record(&cmd2).await;

    // Allow AOF background writer to flush
    sleep(Duration::from_millis(50)).await;

    // Rehydrate into a brand new Db instance
    let fresh_db = Db::new();
    let count = Aof::load(&tmp_file, &fresh_db).await.unwrap();
    assert_eq!(count, 2);

    // Verify reconstructed data
    assert_eq!(
        fresh_db.get(b"saved_key").unwrap(),
        Some(Bytes::from_static(b"persisted_val"))
    );
    assert_eq!(
        fresh_db.lrange(b"saved_list", 0, -1).unwrap(),
        vec![Bytes::from_static(b"item1"), Bytes::from_static(b"item2")]
    );

    let _ = tokio::fs::remove_file(tmp_file).await;
}
