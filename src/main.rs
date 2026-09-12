use bytes::Bytes;
use in_memory_cache::cmd::PubSubCmd;
use in_memory_cache::{Aof, Command, Connection, Db, Frame, PubSub, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, StreamMap};
use tracing::{error, info};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "in_memory_cache=info".into()),
        )
        .init();

    let port = std::env::var("PORT").unwrap_or_else(|_| "6379".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);

    let aof_path = std::env::var("AOF_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("appendonly.aof"));

    let db = Db::new();

    // Rehydrate database from AOF if file exists
    if let Err(e) = Aof::load(&aof_path, &db).await {
        error!("AOF rehydration error: {}", e);
    }

    let aof = Arc::new(Aof::start(aof_path));
    let pubsub = Arc::new(PubSub::new());

    let listener = TcpListener::bind(&bind_addr).await?;
    info!("🚀 In-Memory-Cache server listening on {}", bind_addr);

    // Active sweeper background task
    let sweeper_db = db.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            for _ in 0..16 {
                let (sampled, expired) = sweeper_db.purge_expired_step(20);
                if sampled == 0 || expired * 4 <= sampled {
                    break;
                }
            }
        }
    });

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                let db = db.clone();
                let aof = aof.clone();
                let pubsub = pubsub.clone();
                tokio::spawn(async move {
                    if let Err(err) = process_connection(socket, peer_addr, db, aof, pubsub).await {
                        error!("Connection error with {}: {}", peer_addr, err);
                    }
                });
            }
            Err(err) => {
                error!("Failed to accept incoming connection: {}", err);
            }
        }
    }
}

async fn process_connection(
    socket: TcpStream,
    peer_addr: SocketAddr,
    db: Db,
    aof: Arc<Aof>,
    pubsub: Arc<PubSub>,
) -> Result<()> {
    info!("New connection established from {}", peer_addr);
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await? {
        let cmd = match Command::from_frame(frame.clone()) {
            Ok(cmd) => cmd,
            Err(err) => {
                connection.write_frame(&Frame::Error(err.to_string())).await?;
                continue;
            }
        };

        // If client sends SUBSCRIBE, enter pub/sub subscriber mode
        if let Command::PubSub(PubSubCmd::Subscribe { channels }) = cmd {
            return run_subscriber_mode(connection, channels, pubsub, peer_addr).await;
        }

        // Check if command modifies state; if so, persist to AOF
        if cmd.is_write() {
            aof.record(&frame).await;
        }

        let response = cmd.apply_with_pubsub(&db, &pubsub);
        connection.write_frame(&response).await?;
    }

    info!("Connection closed cleanly by {}", peer_addr);
    Ok(())
}

/// Manage connection once it has entered pub/sub mode
async fn run_subscriber_mode(
    mut connection: Connection,
    initial_channels: Vec<String>,
    pubsub: Arc<PubSub>,
    peer_addr: SocketAddr,
) -> Result<()> {
    let mut subscriptions = StreamMap::new();

    for (i, ch) in initial_channels.into_iter().enumerate() {
        let rx = pubsub.subscribe(&ch);
        subscriptions.insert(ch.clone(), BroadcastStream::new(rx));

        let confirm = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"subscribe")),
            Frame::Bulk(Bytes::from(ch)),
            Frame::Integer((i + 1) as i64),
        ]);
        connection.write_frame(&confirm).await?;
    }

    loop {
        tokio::select! {
            // New message arrived on one of the subscribed channels
            Some((channel, res)) = subscriptions.next() => {
                match res {
                    Ok(msg) => {
                        let push_frame = Frame::Array(vec![
                            Frame::Bulk(Bytes::from_static(b"message")),
                            Frame::Bulk(Bytes::from(channel)),
                            Frame::Bulk(msg),
                        ]);
                        connection.write_frame(&push_frame).await?;
                    }
                    Err(_) => {
                        // Lagged receiver
                    }
                }
            }

            // Incoming command from subscriber client (e.g. SUBSCRIBE more, PING, or quit)
            frame_res = connection.read_frame() => {
                match frame_res? {
                    Some(frame) => {
                        match Command::from_frame(frame) {
                            Ok(Command::PubSub(PubSubCmd::Subscribe { channels })) => {
                                for ch in channels {
                                    if !subscriptions.contains_key(&ch) {
                                        let rx = pubsub.subscribe(&ch);
                                        subscriptions.insert(ch.clone(), BroadcastStream::new(rx));
                                    }
                                    let count = subscriptions.len();
                                    let confirm = Frame::Array(vec![
                                        Frame::Bulk(Bytes::from_static(b"subscribe")),
                                        Frame::Bulk(Bytes::from(ch)),
                                        Frame::Integer(count as i64),
                                    ]);
                                    connection.write_frame(&confirm).await?;
                                }
                            }
                            Ok(Command::Ping(_)) => {
                                let pong = Frame::Array(vec![
                                    Frame::Bulk(Bytes::from_static(b"pong")),
                                    Frame::Bulk(Bytes::from_static(b"")),
                                ]);
                                connection.write_frame(&pong).await?;
                            }
                            _ => {
                                connection.write_frame(&Frame::Error("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context".into())).await?;
                            }
                        }
                    }
                    None => {
                        info!("Subscriber disconnected cleanly: {}", peer_addr);
                        return Ok(());
                    }
                }
            }
        }
    }
}
