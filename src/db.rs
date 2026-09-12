use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_NUM_SHARDS: usize = 64;
const DEFAULT_LRU_SAMPLE_SIZE: usize = 8;
const ESTIMATED_ENTRY_OVERHEAD: usize = 64;

pub const WRONG_TYPE_ERR: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// Rich data types supported by the cache engine.
#[derive(Clone, Debug)]
pub enum DataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
}

/// A cached item with its data type, expiration, and atomic last accessed timestamp.
pub struct CacheEntry {
    pub data: DataType,
    pub expires_at: Option<Instant>,
    pub last_accessed: AtomicU64,
}

impl CacheEntry {
    pub fn new(data: DataType, expires_at: Option<Instant>, now_millis: u64) -> Self {
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
        let data_size = match &self.data {
            DataType::String(b) => b.len(),
            DataType::List(list) => list.iter().map(|b| b.len() + 16).sum::<usize>(),
            DataType::Set(set) => set.iter().map(|b| b.len() + 16).sum::<usize>(),
            DataType::Hash(hash) => hash
                .iter()
                .map(|(k, v)| k.len() + v.len() + 32)
                .sum::<usize>(),
        };
        key.len() + data_size + ESTIMATED_ENTRY_OVERHEAD
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

/// High-performance sharded in-memory database with multi-types, TTL, and approximated LRU.
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

    /// Ensure memory limit is maintained before adding `needed_bytes`.
    fn ensure_capacity(&self, needed_bytes: usize) {
        if self.shared.maxmemory > 0 {
            while self.shared.current_memory.load(Ordering::Relaxed) + needed_bytes
                > self.shared.maxmemory
            {
                if !self.evict_lru_step(DEFAULT_LRU_SAMPLE_SIZE) {
                    break;
                }
            }
        }
    }

    // ==========================================
    // STRING OPERATIONS
    // ==========================================

    /// Retrieve a String value, performing passive eviction if expired.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();

        // 1. Read lock attempt
        {
            let entries = shard.entries.read();
            if let Some(entry) = entries.get(key) {
                if entry.is_expired(now) {
                    // fall through to passive write eviction
                } else {
                    entry
                        .last_accessed
                        .store(self.now_millis(), Ordering::Relaxed);
                    return match &entry.data {
                        DataType::String(b) => Ok(Some(b.clone())),
                        _ => Err(WRONG_TYPE_ERR.to_string()),
                    };
                }
            } else {
                return Ok(None);
            }
        }

        // 2. Passive eviction with write lock
        let mut entries = shard.entries.write();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(key);
                entries.remove(key);
                self.sub_memory(size);
                return Ok(None);
            } else {
                entry
                    .last_accessed
                    .store(self.now_millis(), Ordering::Relaxed);
                return match &entry.data {
                    DataType::String(b) => Ok(Some(b.clone())),
                    _ => Err(WRONG_TYPE_ERR.to_string()),
                };
            }
        }

        Ok(None)
    }

    /// Set a String key-value pair with an optional expiration.
    pub fn set(&self, key: Bytes, value: Bytes, expires_at: Option<Instant>) {
        let new_entry = CacheEntry::new(DataType::String(value), expires_at, self.now_millis());
        let new_size = new_entry.size_bytes(&key);

        self.ensure_capacity(new_size);

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

    // ==========================================
    // LIST OPERATIONS (LPUSH, RPUSH, LPOP, RPOP, LRANGE)
    // ==========================================

    pub fn lpush(&self, key: Bytes, elements: Vec<Bytes>) -> Result<usize, String> {
        let idx = self.get_shard_index(&key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();
        let mut entries = shard.entries.write();

        // Passive eviction if expired
        if let Some(entry) = entries.get(&key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(&key);
                entries.remove(&key);
                self.sub_memory(size);
            }
        }

        let now_m = self.now_millis();
        let entry = entries.entry(key.clone()).or_insert_with(|| {
            CacheEntry::new(DataType::List(VecDeque::new()), None, now_m)
        });

        match &mut entry.data {
            DataType::List(list) => {
                let mut added_bytes = 0;
                for el in elements {
                    added_bytes += el.len() + 16;
                    list.push_front(el);
                }
                entry.last_accessed.store(now_m, Ordering::Relaxed);
                self.add_memory(added_bytes);
                Ok(list.len())
            }
            _ => Err(WRONG_TYPE_ERR.to_string()),
        }
    }

    pub fn rpush(&self, key: Bytes, elements: Vec<Bytes>) -> Result<usize, String> {
        let idx = self.get_shard_index(&key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();
        let mut entries = shard.entries.write();

        if let Some(entry) = entries.get(&key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(&key);
                entries.remove(&key);
                self.sub_memory(size);
            }
        }

        let now_m = self.now_millis();
        let entry = entries.entry(key.clone()).or_insert_with(|| {
            CacheEntry::new(DataType::List(VecDeque::new()), None, now_m)
        });

        match &mut entry.data {
            DataType::List(list) => {
                let mut added_bytes = 0;
                for el in elements {
                    added_bytes += el.len() + 16;
                    list.push_back(el);
                }
                entry.last_accessed.store(now_m, Ordering::Relaxed);
                self.add_memory(added_bytes);
                Ok(list.len())
            }
            _ => Err(WRONG_TYPE_ERR.to_string()),
        }
    }

    pub fn lpop(&self, key: &[u8]) -> Result<Option<Bytes>, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();
        let mut entries = shard.entries.write();

        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(key);
                entries.remove(key);
                self.sub_memory(size);
                return Ok(None);
            }

            match &mut entry.data {
                DataType::List(list) => {
                    let popped = list.pop_front();
                    if let Some(ref p) = popped {
                        self.sub_memory(p.len() + 16);
                    }
                    if list.is_empty() {
                        let size = entry.size_bytes(key);
                        entries.remove(key);
                        self.sub_memory(size);
                    } else {
                        entry
                            .last_accessed
                            .store(self.now_millis(), Ordering::Relaxed);
                    }
                    Ok(popped)
                }
                _ => Err(WRONG_TYPE_ERR.to_string()),
            }
        } else {
            Ok(None)
        }
    }

    pub fn rpop(&self, key: &[u8]) -> Result<Option<Bytes>, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();
        let mut entries = shard.entries.write();

        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(key);
                entries.remove(key);
                self.sub_memory(size);
                return Ok(None);
            }

            match &mut entry.data {
                DataType::List(list) => {
                    let popped = list.pop_back();
                    if let Some(ref p) = popped {
                        self.sub_memory(p.len() + 16);
                    }
                    if list.is_empty() {
                        let size = entry.size_bytes(key);
                        entries.remove(key);
                        self.sub_memory(size);
                    } else {
                        entry
                            .last_accessed
                            .store(self.now_millis(), Ordering::Relaxed);
                    }
                    Ok(popped)
                }
                _ => Err(WRONG_TYPE_ERR.to_string()),
            }
        } else {
            Ok(None)
        }
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Bytes>, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();

        let entries = shard.entries.read();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired(now) {
                return Ok(Vec::new());
            }

            match &entry.data {
                DataType::List(list) => {
                    entry
                        .last_accessed
                        .store(self.now_millis(), Ordering::Relaxed);
                    let len = list.len() as i64;
                    if len == 0 {
                        return Ok(Vec::new());
                    }

                    // Normalize negative indices
                    let mut s = if start < 0 { len + start } else { start };
                    let mut e = if stop < 0 { len + stop } else { stop };

                    if s < 0 {
                        s = 0;
                    }
                    if e >= len {
                        e = len - 1;
                    }

                    if s > e || s >= len {
                        return Ok(Vec::new());
                    }

                    let result = (s..=e)
                        .filter_map(|i| list.get(i as usize).cloned())
                        .collect();
                    Ok(result)
                }
                _ => Err(WRONG_TYPE_ERR.to_string()),
            }
        } else {
            Ok(Vec::new())
        }
    }

    // ==========================================
    // HASH OPERATIONS (HSET, HGET, HDEL, HGETALL)
    // ==========================================

    pub fn hset(&self, key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Result<usize, String> {
        let idx = self.get_shard_index(&key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();
        let mut entries = shard.entries.write();

        if let Some(entry) = entries.get(&key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(&key);
                entries.remove(&key);
                self.sub_memory(size);
            }
        }

        let now_m = self.now_millis();
        let entry = entries.entry(key.clone()).or_insert_with(|| {
            CacheEntry::new(DataType::Hash(HashMap::new()), None, now_m)
        });

        match &mut entry.data {
            DataType::Hash(map) => {
                let mut added_count = 0;
                for (field, value) in fields {
                    let field_len = field.len();
                    let val_len = value.len();
                    if let Some(old_val) = map.insert(field, value) {
                        if val_len > old_val.len() {
                            self.add_memory(val_len - old_val.len());
                        } else {
                            self.sub_memory(old_val.len() - val_len);
                        }
                    } else {
                        added_count += 1;
                        self.add_memory(field_len + val_len + 32);
                    }
                }
                entry.last_accessed.store(now_m, Ordering::Relaxed);
                Ok(added_count)
            }
            _ => Err(WRONG_TYPE_ERR.to_string()),
        }
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();

        let entries = shard.entries.read();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired(now) {
                return Ok(None);
            }

            match &entry.data {
                DataType::Hash(map) => {
                    entry
                        .last_accessed
                        .store(self.now_millis(), Ordering::Relaxed);
                    Ok(map.get(field).cloned())
                }
                _ => Err(WRONG_TYPE_ERR.to_string()),
            }
        } else {
            Ok(None)
        }
    }

    pub fn hdel(&self, key: &[u8], fields: &[Bytes]) -> Result<usize, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();
        let mut entries = shard.entries.write();

        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(key);
                entries.remove(key);
                self.sub_memory(size);
                return Ok(0);
            }

            match &mut entry.data {
                DataType::Hash(map) => {
                    let mut removed = 0;
                    for f in fields {
                        if let Some(val) = map.remove(f) {
                            removed += 1;
                            self.sub_memory(f.len() + val.len() + 32);
                        }
                    }
                    if map.is_empty() {
                        let size = entry.size_bytes(key);
                        entries.remove(key);
                        self.sub_memory(size);
                    } else {
                        entry
                            .last_accessed
                            .store(self.now_millis(), Ordering::Relaxed);
                    }
                    Ok(removed)
                }
                _ => Err(WRONG_TYPE_ERR.to_string()),
            }
        } else {
            Ok(0)
        }
    }

    pub fn hgetall(&self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();

        let entries = shard.entries.read();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired(now) {
                return Ok(Vec::new());
            }

            match &entry.data {
                DataType::Hash(map) => {
                    entry
                        .last_accessed
                        .store(self.now_millis(), Ordering::Relaxed);
                    let pairs = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    Ok(pairs)
                }
                _ => Err(WRONG_TYPE_ERR.to_string()),
            }
        } else {
            Ok(Vec::new())
        }
    }

    // ==========================================
    // SET OPERATIONS (SADD, SMEMBERS, SREM, SISMEMBER)
    // ==========================================

    pub fn sadd(&self, key: Bytes, members: Vec<Bytes>) -> Result<usize, String> {
        let idx = self.get_shard_index(&key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();
        let mut entries = shard.entries.write();

        if let Some(entry) = entries.get(&key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(&key);
                entries.remove(&key);
                self.sub_memory(size);
            }
        }

        let now_m = self.now_millis();
        let entry = entries.entry(key.clone()).or_insert_with(|| {
            CacheEntry::new(DataType::Set(HashSet::new()), None, now_m)
        });

        match &mut entry.data {
            DataType::Set(set) => {
                let mut added = 0;
                for m in members {
                    let m_len = m.len();
                    if set.insert(m) {
                        added += 1;
                        self.add_memory(m_len + 16);
                    }
                }
                entry.last_accessed.store(now_m, Ordering::Relaxed);
                Ok(added)
            }
            _ => Err(WRONG_TYPE_ERR.to_string()),
        }
    }

    pub fn smembers(&self, key: &[u8]) -> Result<Vec<Bytes>, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();

        let entries = shard.entries.read();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired(now) {
                return Ok(Vec::new());
            }

            match &entry.data {
                DataType::Set(set) => {
                    entry
                        .last_accessed
                        .store(self.now_millis(), Ordering::Relaxed);
                    Ok(set.iter().cloned().collect())
                }
                _ => Err(WRONG_TYPE_ERR.to_string()),
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn srem(&self, key: &[u8], members: &[Bytes]) -> Result<usize, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();
        let mut entries = shard.entries.write();

        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired(now) {
                let size = entry.size_bytes(key);
                entries.remove(key);
                self.sub_memory(size);
                return Ok(0);
            }

            match &mut entry.data {
                DataType::Set(set) => {
                    let mut removed = 0;
                    for m in members {
                        if set.remove(m) {
                            removed += 1;
                            self.sub_memory(m.len() + 16);
                        }
                    }
                    if set.is_empty() {
                        let size = entry.size_bytes(key);
                        entries.remove(key);
                        self.sub_memory(size);
                    } else {
                        entry
                            .last_accessed
                            .store(self.now_millis(), Ordering::Relaxed);
                    }
                    Ok(removed)
                }
                _ => Err(WRONG_TYPE_ERR.to_string()),
            }
        } else {
            Ok(0)
        }
    }

    pub fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool, String> {
        let idx = self.get_shard_index(key);
        let shard = &self.shared.shards[idx];
        let now = Instant::now();

        let entries = shard.entries.read();
        if let Some(entry) = entries.get(key) {
            if entry.is_expired(now) {
                return Ok(false);
            }

            match &entry.data {
                DataType::Set(set) => {
                    entry
                        .last_accessed
                        .store(self.now_millis(), Ordering::Relaxed);
                    Ok(set.contains(member))
                }
                _ => Err(WRONG_TYPE_ERR.to_string()),
            }
        } else {
            Ok(false)
        }
    }

    // ==========================================
    // GENERAL KEY AND MEMORY OPERATIONS
    // ==========================================

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
    pub fn ttl(&self, key: &[u8]) -> i64 {
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

    /// Active sweeper step. Samples `sample_size` keys and removes expired ones.
    pub fn purge_expired_step(&self, sample_size: usize) -> (usize, usize) {
        let random_shard_idx = fastrand::usize(..self.shared.num_shards);
        let shard = &self.shared.shards[random_shard_idx];
        let now = Instant::now();

        let (sampled, expired_keys) = {
            let entries = shard.entries.read();
            if entries.is_empty() {
                return (0, 0);
            }

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

    /// Approximated LRU Eviction: Samples candidate keys across shards and removes the oldest.
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

    #[test]
    fn test_list_operations() {
        let db = Db::new();
        let key = Bytes::from("mylist");

        // RPUSH mylist 1 2 3
        let count = db.rpush(key.clone(), vec![Bytes::from("1"), Bytes::from("2"), Bytes::from("3")]).unwrap();
        assert_eq!(count, 3);

        // LPUSH mylist 0
        let count = db.lpush(key.clone(), vec![Bytes::from("0")]).unwrap();
        assert_eq!(count, 4);

        // LRANGE mylist 0 -1
        let range = db.lrange(key.as_ref(), 0, -1).unwrap();
        assert_eq!(range, vec![Bytes::from("0"), Bytes::from("1"), Bytes::from("2"), Bytes::from("3")]);

        // LPOP
        let popped = db.lpop(key.as_ref()).unwrap();
        assert_eq!(popped, Some(Bytes::from("0")));

        // RPOP
        let popped = db.rpop(key.as_ref()).unwrap();
        assert_eq!(popped, Some(Bytes::from("3")));
    }

    #[test]
    fn test_hash_operations() {
        let db = Db::new();
        let key = Bytes::from("myhash");

        // HSET myhash f1 v1 f2 v2
        let added = db.hset(key.clone(), vec![
            (Bytes::from("f1"), Bytes::from("v1")),
            (Bytes::from("f2"), Bytes::from("v2")),
        ]).unwrap();
        assert_eq!(added, 2);

        // HGET myhash f1
        let val = db.hget(key.as_ref(), b"f1").unwrap();
        assert_eq!(val, Some(Bytes::from("v1")));

        // HDEL myhash f1
        let del_count = db.hdel(key.as_ref(), &[Bytes::from("f1")]).unwrap();
        assert_eq!(del_count, 1);
        assert_eq!(db.hget(key.as_ref(), b"f1").unwrap(), None);

        // HGETALL
        let all = db.hgetall(key.as_ref()).unwrap();
        assert_eq!(all, vec![(Bytes::from("f2"), Bytes::from("v2"))]);
    }

    #[test]
    fn test_set_operations() {
        let db = Db::new();
        let key = Bytes::from("myset");

        // SADD myset a b c a
        let added = db.sadd(key.clone(), vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("a"),
        ]).unwrap();
        assert_eq!(added, 3);

        // SISMEMBER
        assert!(db.sismember(key.as_ref(), b"a").unwrap());
        assert!(!db.sismember(key.as_ref(), b"z").unwrap());

        // SREM
        let rem = db.srem(key.as_ref(), &[Bytes::from("a")]).unwrap();
        assert_eq!(rem, 1);
        assert!(!db.sismember(key.as_ref(), b"a").unwrap());

        // SMEMBERS
        let members = db.smembers(key.as_ref()).unwrap();
        assert_eq!(members.len(), 2);
    }

    #[test]
    fn test_wrong_type_error() {
        let db = Db::new();
        let key = Bytes::from("str_key");
        db.set(key.clone(), Bytes::from("value"), None);

        // Try list op on string
        assert_eq!(db.lpush(key.clone(), vec![Bytes::from("1")]).unwrap_err(), WRONG_TYPE_ERR);
        // Try hash op on string
        assert_eq!(db.hget(key.as_ref(), b"f").unwrap_err(), WRONG_TYPE_ERR);
        // Try set op on string
        assert_eq!(db.smembers(key.as_ref()).unwrap_err(), WRONG_TYPE_ERR);
    }
}
