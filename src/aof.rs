use bytes::Buf;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::cmd::Command;
use crate::db::Db;
use crate::frame::{Frame, FrameError};

enum AofMsg {
    Record(Vec<u8>),
    Flush(oneshot::Sender<()>),
}

pub struct Aof {
    tx: mpsc::Sender<AofMsg>,
}

impl Aof {
    /// Start the background AOF writer task.
    pub fn start(file_path: PathBuf) -> Self {
        let (tx, mut rx) = mpsc::channel::<AofMsg>(2048);

        tokio::spawn(async move {
            let file = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    error!("Failed to open AOF file {:?}: {}", file_path, e);
                    return;
                }
            };

            let mut writer = BufWriter::new(file);

            while let Some(msg) = rx.recv().await {
                match msg {
                    AofMsg::Record(bytes) => {
                        if let Err(e) = writer.write_all(&bytes).await {
                            error!("AOF write error: {}", e);
                            continue;
                        }
                    }
                    AofMsg::Flush(reply) => {
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_all().await;
                        let _ = reply.send(());
                    }
                }
            }

            // Drain remaining before shutdown
            let _ = writer.flush().await;
            let _ = writer.get_ref().sync_all().await;
            info!("AOF writer task safely closed: {:?}", file_path);
        });

        Aof { tx }
    }

    /// Record a modifying command frame into AOF.
    pub async fn record(&self, frame: &Frame) {
        let mut buf = Vec::new();
        frame.write_to_buf(&mut buf);
        let _ = self.tx.send(AofMsg::Record(buf)).await;
    }

    /// Force an immediate flush and OS sync of all pending AOF operations.
    pub async fn sync(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(AofMsg::Flush(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }

    /// Load and replay commands from an AOF file to rebuild database state.
    pub async fn load(path: &Path, db: &Db) -> Result<usize, crate::Error> {
        if !path.exists() {
            return Ok(0);
        }

        let mut file = File::open(path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        let mut cursor = Cursor::new(&buf[..]);
        let mut command_count = 0;

        while cursor.has_remaining() {
            let start_pos = cursor.position() as usize;
            match Frame::check(&mut cursor) {
                Ok(_) => {
                    let end_pos = cursor.position() as usize;
                    let mut parse_cursor = Cursor::new(&buf[start_pos..end_pos]);
                    let frame = Frame::parse(&mut parse_cursor)?;

                    if let Ok(cmd) = Command::from_frame(frame) {
                        cmd.apply(db);
                        command_count += 1;
                    }
                }
                Err(FrameError::Incomplete) => break,
                Err(e) => {
                    error!("AOF rehydration parsing error: {}", e);
                    break;
                }
            }
        }

        info!("AOF recovery complete: replayed {} commands from {:?}", command_count, path);
        Ok(command_count)
    }
}
