use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_NUM_SHARDS: usize = 64;
const DEFAULT_LRU_SAMPLE_SIZE: usize = 8;
const ESTIMATED_ENTRY_OVERHEAD: usize = 64;

/// A cached item with its value, expiration, and atomic last accessed timestamp.
pub struct CacheEntry {
    pub data: Bytes,
    pub expires_at: Option<Instant>,
    pub last_accessed: AtomicU64,
}

impl CacheEntry {
    pub fn new(data: Bytes, expires_at: Option<Instant>, now_millis: u64) -> Self {
        Self {
            data,
            expires_at,
            last_accessed: AtomicU64::new(now_millis),
        }
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        match self.expires_at {
            Some(exp) => now >= exp,
            None => false,
        }
    }

    pub fn size_bytes(&self, key: &[u8]) -> usize {
        key.len() + self.data.len() + ESTIMATED_ENTRY_OVERHEAD
    }
}

struct Shard {
    entries: RwLock<HashMap<Bytes, CacheEntry>>,
}

struct Shared {
    shards: Vec<Shard>,
    num_shards: usize,
    maxmemory: usize, // 0 = unlimited
    current_memory: AtomicUsize,
    start_time: Instant,
}

/// High-performance sharded in-memory database with TTL, approximated LRU, and memory tracking.
#[derive(Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

impl Db {
    /// Create a new database instance with default settings and optional MAXMEMORY env.
    pub fn new() -> Db {
        let maxmemory = std::env::var("MAXMEMORY")
            .ok()
            .and_then(|v| parse_memory_limit(&v))
            .unwrap_or(0);

        Self::with_options(DEFAULT_NUM_SHARDS, maxmemory)
    }

    /// Create a database instance with specific sharding and memory limit.
    pub fn with_options(num_shards: usize, maxmemory: usize) -> Db {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Shard {
                entries: RwLock::new(HashMap::new()),
            });
        }

        Db {
            shared: Arc::new(Shared {
                shards,
                num_shards,
                maxmemory,
                current_memory: AtomicUsize::new(0),
                start_time: Instant::now(),
            }),
        }
    }

    #[inline]
    fn now_millis(&self) -> u64 {
        Instant::now()
            .duration_since(self.shared.start_time)
            .as_millis() as u64
    }

    #[inline]
    fn get_shard_index(&self, key: &[u8]) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shared.num_shards
    }

    /// Retrieve the value for a key, performing passive eviction if expired.
    /// Updates `last_accessed` atomically on cache hit.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();

        // 1. First attempt read-only lookup
        {
            let entries = shard.entries.read();
            if let Some(entry) = entries.get(key) {
                if entry.is_expired(now) {
                    // Needs eviction; fall through to write-lock cleanup below
                } else {
                    entry
                        .last_accessed
                        .store(self.now_millis(), Ordering::Relaxed);
                    return Some(entry.data.clone());
                }
            } else {
                return None;
            }
        }

        // 2. Passive eviction: acquire write lock to remove expired key
        let mut entries = shard.entries.write();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(key);
                entries.remove(key);
                self.sub_memory(size);
                return None;
            } else {
                entry
                    .last_accessed
                    .store(self.now_millis(), Ordering::Relaxed);
                return Some(entry.data.clone());
            }
        }

        None
    }

    /// Set a key-value pair with an optional expiration timestamp.
    /// Triggers approximated LRU eviction if `maxmemory` limit is exceeded.
    pub fn set(&self, key: Bytes, value: Bytes, expires_at: Option<Instant>) {
        let new_entry = CacheEntry::new(value, expires_at, self.now_millis());
        let new_size = new_entry.size_bytes(&key);

        // Check if eviction is needed
        if self.shared.maxmemory > 0 {
            while self.shared.current_memory.load(Ordering::Relaxed) + new_size
                > self.shared.maxmemory
            {
                if !self.evict_lru_step(DEFAULT_LRU_SAMPLE_SIZE) {
                    // Cannot evict further (database is empty or all candidate shards are empty)
                    break;
                }
            }
        }

        let idx = self.get_shard_index(&key);
        let shard = &self.shared.shards[idx];
        let mut entries = shard.entries.write();

        if let Some(old_entry) = entries.insert(key.clone(), new_entry) {
            let old_size = old_entry.size_bytes(&key);
            if new_size > old_size {
                self.add_memory(new_size - old_size);
            } else {
                self.sub_memory(old_size - new_size);
            }
        } else {
            self.add_memory(new_size);
        }
    }

    /// Delete multiple keys. Returns the number of keys removed.
    pub fn del(&self, keys: &[Bytes]) -> usize {
        let mut removed = 0;
        for key in keys {
            let idx = self.get_shard_index(key);
            let shard = &self.shared.shards[idx];
            let mut entries = shard.entries.write();
            if let Some(entry) = entries.remove(key) {
                self.sub_memory(entry.size_bytes(key));
                removed += 1;
            }
        }
        removed
    }

    /// Set a key's time-to-live. Returns true if TTL was set, false if key does not exist.
    pub fn expire(&self, key: &[u8], duration: Duration) -> bool {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let mut entries = shard.entries.write();
        let now = Instant::now();

        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(key);
                entries.remove(key);
                self.sub_memory(size);
                false
            } else {
                entry.expires_at = Some(now + duration);
                true
            }
        } else {
            false
        }
    }

    /// Returns the remaining TTL in seconds.
    /// -2 if key does not exist (or is expired).
    /// -1 if key exists without TTL.
    /// >= 0 representing remaining seconds.
    pub fn ttl(&self, key: &[u8]) -> i64 {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();

        let entries = shard.entries.read();
        if let Some(entry) = entries.get(key) {
            match entry.expires_at {
                Some(exp) => {
                    if now >= exp {
                        // Expired
                        -2
                    } else {
                        (exp - now).as_secs() as i64
                    }
                }
                None => -1,
            }
        } else {
            -2
        }
    }

    /// Returns the remaining TTL in milliseconds.
    /// -2 if key does not exist (or is expired).
    /// -1 if key exists without TTL.
    /// >= 0 representing remaining milliseconds.
    pub fn pttl(&self, key: &[u8]) -> i64 {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();

        let entries = shard.entries.read();
        if let Some(entry) = entries.get(key) {
            match entry.expires_at {
                Some(exp) => {
                    if now >= exp {
                        -2
                    } else {
                        (exp - now).as_millis() as i64
                    }
                }
                None => -1,
            }
        } else {
            -2
        }
    }

    /// Perform an active sweeper step over random shards.
    /// Samples `sample_size` keys and removes expired ones.
    /// Returns `(sampled_count, expired_count)`.
    pub fn purge_expired_step(&self, sample_size: usize) -> (usize, usize) {
        let random_shard_idx = fastrand::usize(..self.shared.num_shards);
        let shard = &self.shared.shards[random_shard_idx];
        let now = Instant::now();

        let (sampled, expired_keys) = {
            let entries = shard.entries.read();
            if entries.is_empty() {
                return (0, 0);
            }

            // Sample random entries from the shard
            let keys: Vec<&Bytes> = entries.keys().collect();
            let n = keys.len().min(sample_size);
            let mut expired = Vec::new();

            for _ in 0..n {
                let r = fastrand::usize(..keys.len());
                let key = keys[r];
                if let Some(entry) = entries.get(key) {
                    if entry.is_expired(now) {
                        expired.push(key.clone());
                    }
                }
            }
            (n, expired)
        };

        if !expired_keys.is_empty() {
            let mut entries = shard.entries.write();
            for key in &expired_keys {
                if let Some(entry) = entries.get(key) {
                    if entry.is_expired(now) {
                        let size = entry.size_bytes(key);
                        entries.remove(key);
                        self.sub_memory(size);
                    }
                }
            }
        }

        (sampled, expired_keys.len())
    }

    /// Approximated LRU Eviction: Samples `sample_size` keys across shards,
    /// identifies the key with the smallest `last_accessed` timestamp, and removes it.
    /// Returns `true` if a key was evicted.
    pub fn evict_lru_step(&self, sample_size: usize) -> bool {
        let mut oldest_key: Option<(usize, Bytes)> = None;
        let mut oldest_time = u64::MAX;

        for _ in 0..sample_size {
            let shard_idx = fastrand::usize(..self.shared.num_shards);
            let shard = &self.shared.shards[shard_idx];
            let entries = shard.entries.read();

            if entries.is_empty() {
                continue;
            }

            let keys: Vec<&Bytes> = entries.keys().collect();
            let rand_idx = fastrand::usize(..keys.len());
            let candidate_key = keys[rand_idx];

            if let Some(entry) = entries.get(candidate_key) {
                let accessed = entry.last_accessed.load(Ordering::Relaxed);
                if accessed < oldest_time {
                    oldest_time = accessed;
                    oldest_key = Some((shard_idx, candidate_key.clone()));
                }
            }
        }

        if let Some((shard_idx, key_to_evict)) = oldest_key {
            let shard = &self.shared.shards[shard_idx];
            let mut entries = shard.entries.write();
            if let Some(entry) = entries.remove(&key_to_evict) {
                self.sub_memory(entry.size_bytes(&key_to_evict));
                return true;
            }
        }

        false
    }

    /// Total count of keys across all shards.
    pub fn len(&self) -> usize {
        let mut total = 0;
        for shard in &self.shared.shards {
            total += shard.entries.read().len();
        }
        total
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Current tracked memory usage in bytes.
    pub fn current_memory(&self) -> usize {
        self.shared.current_memory.load(Ordering::Relaxed)
    }

    #[inline]
    fn add_memory(&self, bytes: usize) {
        self.shared.current_memory.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    fn sub_memory(&self, bytes: usize) {
        self.shared.current_memory.fetch_sub(bytes, Ordering::Relaxed);
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to parse memory string e.g. "100mb", "1gb", "1048576"
fn parse_memory_limit(s: &str) -> Option<usize> {
    let s = s.trim().to_lowercase();
    if let Some(num_str) = s.strip_suffix("gb") {
        num_str.trim().parse::<usize>().ok().map(|n| n * 1024 * 1024 * 1024)
    } else if let Some(num_str) = s.strip_suffix("mb") {
        num_str.trim().parse::<usize>().ok().map(|n| n * 1024 * 1024)
    } else if let Some(num_str) = s.strip_suffix("kb") {
        num_str.trim().parse::<usize>().ok().map(|n| n * 1024)
    } else {
        s.parse::<usize>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_sharded_set_get() {
        let db = Db::with_options(8, 0);
        for i in 0..100 {
            let k = Bytes::from(format!("key:{}", i));
            let v = Bytes::from(format!("val:{}", i));
            db.set(k.clone(), v.clone(), None);
            assert_eq!(db.get(k.as_ref()), Some(v));
        }
        assert_eq!(db.len(), 100);
    }

    #[test]
    fn test_passive_expiration() {
        let db = Db::with_options(4, 0);
        let key = Bytes::from("expire_me");
        let val = Bytes::from("value");

        // Set with 20ms TTL
        db.set(key.clone(), val.clone(), Some(Instant::now() + Duration::from_millis(20)));
        assert_eq!(db.get(key.as_ref()), Some(val));

        sleep(Duration::from_millis(30));
        // Passive eviction should trigger
        assert_eq!(db.get(key.as_ref()), None);
    }

    #[test]
    fn test_ttl_and_pttl() {
        let db = Db::with_options(4, 0);
        let key = Bytes::from("ttl_test");
        let val = Bytes::from("value");

        // Non-existent key
        assert_eq!(db.ttl(b"non_existent"), -2);

        // Key with no TTL
        db.set(key.clone(), val, None);
        assert_eq!(db.ttl(key.as_ref()), -1);

        // Set TTL of 5 seconds
        assert!(db.expire(key.as_ref(), Duration::from_secs(5)));
        assert!(db.ttl(key.as_ref()) <= 5 && db.ttl(key.as_ref()) > 0);
        assert!(db.pttl(key.as_ref()) <= 5000 && db.pttl(key.as_ref()) > 0);
    }

    #[test]
    fn test_approximated_lru_eviction() {
        // Small maxmemory limit (~300 bytes)
        let db = Db::with_options(2, 300);

        let k1 = Bytes::from("k1");
        let k2 = Bytes::from("k2");
        let k3 = Bytes::from("k3");
        let val = Bytes::from("abcdefghijklmnopqrstuvwxyz"); // 26 bytes

        db.set(k1.clone(), val.clone(), None);
        db.set(k2.clone(), val.clone(), None);

        // Access k1 so k2 becomes older
        sleep(Duration::from_millis(5));
        let _ = db.get(k1.as_ref());

        // Inserting k3 should force LRU eviction
        db.set(k3.clone(), val.clone(), None);

        // At least one key should have been evicted to keep memory under 300
        assert!(db.current_memory() <= 300);
        // k1 was recently accessed, so k1 or newly inserted k3 should still be present
        assert!(db.get(k1.as_ref()).is_some() || db.get(k3.as_ref()).is_some());
    }

    #[test]
    fn test_active_sweeper() {
        let db = Db::with_options(2, 0);
        for i in 0..10 {
            let k = Bytes::from(format!("temp:{}", i));
            let v = Bytes::from("v");
            db.set(k, v, Some(Instant::now() + Duration::from_millis(10)));
        }

        sleep(Duration::from_millis(20));

        // Purge expired keys actively
        let mut total_expired = 0;
        for _ in 0..10 {
            let (_, exp) = db.purge_expired_step(20);
            total_expired += exp;
        }
        assert!(total_expired > 0);
    }
}
