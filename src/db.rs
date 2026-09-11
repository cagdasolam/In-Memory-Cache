use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Thread-safe in-memory key-value database.
/// In Phase 1 MVP, entries are stored in a parking_lot::RwLock-protected HashMap.
#[derive(Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

struct Shared {
    entries: RwLock<HashMap<Bytes, Bytes>>,
}

impl Db {
    /// Create a new, empty in-memory database instance.
    pub fn new() -> Db {
        Db {
            shared: Arc::new(Shared {
                entries: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Get the value associated with a key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let entries = self.shared.entries.read();
        entries.get(key).cloned()
    }

    /// Set the value associated with a key.
    pub fn set(&self, key: Bytes, value: Bytes) {
        let mut entries = self.shared.entries.write();
        entries.insert(key, value);
    }

    /// Delete a key from the database. Returns true if the key existed.
    pub fn del(&self, key: &[u8]) -> bool {
        let mut entries = self.shared.entries.write();
        entries.remove(key).is_some()
    }

    /// Get the total number of entries in the database.
    pub fn len(&self) -> usize {
        let entries = self.shared.entries.read();
        entries.len()
    }

    /// Check if the database is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let db = Db::new();
        db.set(Bytes::from("key1"), Bytes::from("val1"));
        assert_eq!(db.get(b"key1"), Some(Bytes::from("val1")));
        assert_eq!(db.get(b"nonexistent"), None);
    }

    #[test]
    fn test_del() {
        let db = Db::new();
        db.set(Bytes::from("key1"), Bytes::from("val1"));
        assert!(db.del(b"key1"));
        assert!(!db.del(b"key1"));
        assert_eq!(db.get(b"key1"), None);
    }
}

