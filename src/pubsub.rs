use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 1024;

/// Pub/Sub channel hub managing multi-channel message dispatch.
#[derive(Clone, Default)]
pub struct PubSub {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<Bytes>>>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Publish a message to a channel. Returns the number of subscribers that received it.
    pub fn publish(&self, channel: &str, msg: Bytes) -> usize {
        let channels = self.channels.read();
        if let Some(sender) = channels.get(channel) {
            let receiver_count = sender.receiver_count();
            let _ = sender.send(msg);
            receiver_count
        } else {
            0
        }
    }

    /// Subscribe to a channel, returning a broadcast Receiver.
    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<Bytes> {
        let mut channels = self.channels.write();
        let sender = channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        sender.subscribe()
    }
}

