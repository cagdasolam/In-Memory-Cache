use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use crate::frame::{Frame, FrameError};

/// Network framing abstraction over a TCP stream.
/// Uses `BytesMut` for zero-copy buffer management and pipelining support.
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    /// Create a new `Connection`, backed by a TCP stream.
    pub fn new(socket: TcpStream) -> Connection {
        let _ = socket.set_nodelay(true);
        Connection {
            stream: BufWriter::new(socket),
            // Default 8KB buffer for incoming commands
            buffer: BytesMut::with_capacity(8 * 1024),
        }
    }

    /// Read a single `Frame` from the TCP stream.
    /// Returns `Ok(None)` if the remote closed the connection cleanly.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, crate::Error> {
        loop {
            // Attempt to parse a frame from buffered data
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // Not enough data in buffer, read more from socket
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // Socket reached EOF
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer while reading frame".into());
                }
            }
        }
    }

    /// Try parsing a frame from the internal buffer.
    fn parse_frame(&mut self) -> Result<Option<Frame>, crate::Error> {
        let mut cursor = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut cursor) {
            Ok(_) => {
                let len = cursor.position() as usize;
                cursor.set_position(0);

                let frame = Frame::parse(&mut cursor)?;
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(FrameError::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Check if more bytes are present in the read buffer (useful for pipelining batch flush).
    pub fn has_queued_bytes(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// Write a frame into the internal write buffer without an immediate flush.
    pub async fn write_frame_buffered(&mut self, frame: &Frame) -> io::Result<()> {
        let mut buf = Vec::new();
        frame.write_to_buf(&mut buf);
        self.stream.write_all(&buf).await
    }

    /// Flush the buffered writes directly onto the wire.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }

    /// Write a frame to the connection and immediately flush.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        self.write_frame_buffered(frame).await?;
        self.flush().await
    }
}
