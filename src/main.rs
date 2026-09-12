use in_memory_cache::{Command, Connection, Db, Frame, Result};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
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

    let listener = TcpListener::bind(&bind_addr).await?;
    info!("🚀 In-Memory-Cache server listening on {}", bind_addr);

    let db = Db::new();

    // Start background active sweeper task
    let sweeper_db = db.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            // Active expiration sampling: sample 20 keys, repeat if >25% are expired
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
                tokio::spawn(async move {
                    if let Err(err) = process_connection(socket, peer_addr, db).await {
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

async fn process_connection(socket: TcpStream, peer_addr: SocketAddr, db: Db) -> Result<()> {
    info!("New connection established from {}", peer_addr);
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await? {
        let response = match Command::from_frame(frame) {
            Ok(cmd) => cmd.apply(&db),
            Err(err) => Frame::Error(err.to_string()),
        };

        connection.write_frame(&response).await?;
    }

    info!("Connection closed cleanly by {}", peer_addr);
    Ok(())
}
