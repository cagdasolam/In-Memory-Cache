use bytes::Bytes;
use in_memory_cache::{Command, Connection, Db, Frame};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

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
async fn test_del() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    // Set two keys
    let set1 = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"d1")),
        Frame::Bulk(Bytes::from_static(b"v1")),
    ]);
    conn.write_frame(&set1).await.unwrap();
    let _ = conn.read_frame().await.unwrap();

    let set2 = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"d2")),
        Frame::Bulk(Bytes::from_static(b"v2")),
    ]);
    conn.write_frame(&set2).await.unwrap();
    let _ = conn.read_frame().await.unwrap();

    // DEL d1 d2 d3 (d3 doesn't exist) -> returns 2
    let del = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"DEL")),
        Frame::Bulk(Bytes::from_static(b"d1")),
        Frame::Bulk(Bytes::from_static(b"d2")),
        Frame::Bulk(Bytes::from_static(b"d3")),
    ]);
    conn.write_frame(&del).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(2));
}

#[tokio::test]
async fn test_set_ex_and_ttl() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    // SET temp val EX 2
    let set_ex = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"temp")),
        Frame::Bulk(Bytes::from_static(b"val")),
        Frame::Bulk(Bytes::from_static(b"EX")),
        Frame::Bulk(Bytes::from_static(b"2")),
    ]);
    conn.write_frame(&set_ex).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Simple("OK".to_string()));

    // TTL temp -> should be 1 or 2
    let ttl_cmd = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"TTL")),
        Frame::Bulk(Bytes::from_static(b"temp")),
    ]);
    conn.write_frame(&ttl_cmd).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    match res {
        Frame::Integer(secs) => assert!(secs > 0 && secs <= 2),
        _ => panic!("Expected integer response for TTL"),
    }
}

#[tokio::test]
async fn test_expire_and_pexpire() {
    let addr = spawn_test_server().await;
    let socket = TcpStream::connect(addr).await.unwrap();
    let mut conn = Connection::new(socket);

    // SET exp_key exp_val
    let set = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"SET")),
        Frame::Bulk(Bytes::from_static(b"exp_key")),
        Frame::Bulk(Bytes::from_static(b"exp_val")),
    ]);
    conn.write_frame(&set).await.unwrap();
    let _ = conn.read_frame().await.unwrap();

    // EXPIRE exp_key 10 -> returns 1
    let exp = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"EXPIRE")),
        Frame::Bulk(Bytes::from_static(b"exp_key")),
        Frame::Bulk(Bytes::from_static(b"10")),
    ]);
    conn.write_frame(&exp).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(1));

    // PEXPIRE exp_key 500 (500ms)
    let pexp = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"PEXPIRE")),
        Frame::Bulk(Bytes::from_static(b"exp_key")),
        Frame::Bulk(Bytes::from_static(b"50")),
    ]);
    conn.write_frame(&pexp).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(1));

    // Sleep 60ms and check GET exp_key -> should be Null (expired)
    sleep(Duration::from_millis(60)).await;
    let get = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"GET")),
        Frame::Bulk(Bytes::from_static(b"exp_key")),
    ]);
    conn.write_frame(&get).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Null);

    // TTL of expired key -> should be -2
    let ttl = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"TTL")),
        Frame::Bulk(Bytes::from_static(b"exp_key")),
    ]);
    conn.write_frame(&ttl).await.unwrap();
    let res = conn.read_frame().await.unwrap().unwrap();
    assert_eq!(res, Frame::Integer(-2));
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
