use bytes::Bytes;
use in_memory_cache::{Command, Connection, Db, Frame};
use tokio::net::{TcpListener, TcpStream};

async fn spawn_test_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let db = Db::new();

    tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(res) => res,
                Err(_) => break,
            };
            let db = db.clone();
            tokio::spawn(async move {
                let mut connection = Connection::new(socket);
                while let Ok(Some(frame)) = connection.read_frame().await {
                    let response = match Command::from_frame(frame) {
                        Ok(cmd) => cmd.apply(&db),
                        Err(err) => Frame::Error(err.to_string()),
                    };
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

    // Test PING without args
    let ping_frame = Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]);
    conn.write_frame(&ping_frame).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Simple("PONG".to_string()));

    // Test PING with custom message
    let ping_msg = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"PING")),
        Frame::Bulk(Bytes::from_static(b"hello")),
    ]);
    conn.write_frame(&ping_msg).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Bulk(Bytes::from_static(b"hello")));
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

    // GET nonexistent key -> Null
    let get_none = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"GET")),
        Frame::Bulk(Bytes::from_static(b"missing_key")),
    ]);
    conn.write_frame(&get_none).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Null);

    // SET key value -> OK
    let set_frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"foo")),
        Frame::Bulk(Bytes::from_static(b"bar")),
    ]);
    conn.write_frame(&set_frame).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Simple("OK".to_string()));

    // GET key -> value
    let get_frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"GET")),
        Frame::Bulk(Bytes::from_static(b"foo")),
    ]);
    conn.write_frame(&get_frame).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Bulk(Bytes::from_static(b"bar")));
}

#[tokio::test]
async fn test_pipelining() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = spawn_test_server().await;
    let mut socket = TcpStream::connect(addr).await.unwrap();

    // Send two commands back to back in a single payload
    let raw_payload = b"*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n";
    socket.write_all(raw_payload).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let n = socket.read(&mut buf).await.unwrap();
    let response_str = std::str::from_utf8(&buf[..n]).unwrap();

    // Expected responses: +PONG\r\n+OK\r\n
    assert_eq!(response_str, "+PONG\r\n+OK\r\n");
}

