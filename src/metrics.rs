use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

const LATENCY_BUFFER_SIZE: usize = 2048;
const HISTORY_CAPACITY: usize = 60;

/// Snapshot of historical metrics recorded every second.
#[derive(Clone, Debug)]
pub struct HistoryPoint {
    pub timestamp_sec: u64,
    pub hit_ratio: f64,
    pub miss_ratio: f64,
    pub ops_per_sec: u64,
    pub avg_latency_us: f64,
    pub p95_latency_us: u64,
    pub p99_latency_us: u64,
    pub memory_cache_bytes: usize,
    pub memory_rss_bytes: u64,
    pub cpu_percent: f64,
    pub connected_clients: i64,
    pub evicted_keys: u64,
    pub expired_keys: u64,
}

/// Computed latency percentiles in microseconds.
#[derive(Clone, Copy, Debug, Default)]
pub struct LatencyStats {
    pub min_us: u64,
    pub max_us: u64,
    pub avg_us: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
}

/// Central lock-free metrics engine for the In-Memory Cache.
pub struct Metrics {
    // Cache Hit / Miss counters
    pub hits: AtomicU64,
    pub misses: AtomicU64,

    // Command throughput & latency
    pub total_commands: AtomicU64,
    pub ops_per_sec: AtomicU64,
    pub total_latency_us: AtomicU64,
    pub latency_count: AtomicU64,
    recent_latencies: Mutex<VecDeque<u64>>,

    // Connection tracking
    pub connected_clients: AtomicI64,
    pub peak_clients: AtomicUsize,
    pub total_connections: AtomicU64,
    pub connection_errors: AtomicU64,

    // Keyspace lifecycle
    pub evicted_keys: AtomicU64,
    pub expired_keys: AtomicU64,

    // System resources (CPU % scaled by 100, e.g. 1250 = 12.50%)
    pub cpu_percent_scaled: AtomicU32,
    pub rss_memory_bytes: AtomicU64,

    // Rolling history for charts
    history: RwLock<VecDeque<HistoryPoint>>,

    // Internal state for deltas
    start_time: Instant,
    last_tick_time: Mutex<Instant>,
    last_total_commands: AtomicU64,
    last_cpu_jiffies: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            total_commands: AtomicU64::new(0),
            ops_per_sec: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            recent_latencies: Mutex::new(VecDeque::with_capacity(LATENCY_BUFFER_SIZE)),
            connected_clients: AtomicI64::new(0),
            peak_clients: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            evicted_keys: AtomicU64::new(0),
            expired_keys: AtomicU64::new(0),
            cpu_percent_scaled: AtomicU32::new(0),
            rss_memory_bytes: AtomicU64::new(0),
            history: RwLock::new(VecDeque::with_capacity(HISTORY_CAPACITY)),
            start_time: Instant::now(),
            last_tick_time: Mutex::new(Instant::now()),
            last_total_commands: AtomicU64::new(0),
            last_cpu_jiffies: AtomicU64::new(read_proc_cpu_jiffies().unwrap_or(0)),
        }
    }

    // ==========================================
    // CACHE HIT & MISS TRACKING
    // ==========================================

    #[inline]
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn hit_ratio(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed) as f64;
        let m = self.misses.load(Ordering::Relaxed) as f64;
        let total = h + m;
        if total > 0.0 {
            (h / total) * 100.0
        } else {
            0.0
        }
    }

    pub fn miss_ratio(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed) as f64;
        let m = self.misses.load(Ordering::Relaxed) as f64;
        let total = h + m;
        if total > 0.0 {
            (m / total) * 100.0
        } else {
            0.0
        }
    }

    // ==========================================
    // COMMAND & LATENCY TRACKING
    // ==========================================

    #[inline]
    pub fn record_command(&self, duration_us: u64) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(duration_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);

        let mut buf = self.recent_latencies.lock();
        if buf.len() >= LATENCY_BUFFER_SIZE {
            buf.pop_front();
        }
        buf.push_back(duration_us);
    }

    pub fn latency_stats(&self) -> LatencyStats {
        let buf = self.recent_latencies.lock();
        if buf.is_empty() {
            return LatencyStats::default();
        }

        let mut samples: Vec<u64> = buf.iter().copied().collect();
        drop(buf);

        samples.sort_unstable();
        let count = samples.len();
        let min_us = samples[0];
        let max_us = samples[count - 1];
        let p50_us = samples[(count * 50) / 100];
        let p95_us = samples[(count * 95) / 100];
        let p99_us = samples[(count * 99) / 100];
        let sum: u64 = samples.iter().sum();
        let avg_us = sum as f64 / count as f64;

        LatencyStats {
            min_us,
            max_us,
            avg_us,
            p50_us,
            p95_us,
            p99_us,
        }
    }

    // ==========================================
    // CLIENT CONNECTION TRACKING
    // ==========================================

    pub fn client_connected(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        let curr = self.connected_clients.fetch_add(1, Ordering::SeqCst) + 1;
        if curr > 0 {
            let ucurr = curr as usize;
            let mut peak = self.peak_clients.load(Ordering::Relaxed);
            while ucurr > peak {
                match self.peak_clients.compare_exchange_weak(
                    peak,
                    ucurr,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => peak = actual,
                }
            }
        }
    }

    pub fn client_disconnected(&self) {
        self.connected_clients.fetch_sub(1, Ordering::SeqCst);
    }

    #[inline]
    pub fn record_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    // ==========================================
    // EVICTION & EXPIRATION TRACKING
    // ==========================================

    #[inline]
    pub fn record_eviction(&self, count: u64) {
        self.evicted_keys.fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_expiration(&self, count: u64) {
        self.expired_keys.fetch_add(count, Ordering::Relaxed);
    }

    // ==========================================
    // BACKGROUND SAMPLING (RUNS EVERY 1 SECOND)
    // ==========================================

    pub fn tick_second(&self, current_cache_memory: usize) {
        let now = Instant::now();
        let mut last_tick = self.last_tick_time.lock();
        let elapsed = now.duration_since(*last_tick);
        *last_tick = now;

        let elapsed_sec = elapsed.as_secs_f64().max(0.001);

        // 1. Throughput (Commands / sec)
        let total_cmds = self.total_commands.load(Ordering::Relaxed);
        let last_cmds = self.last_total_commands.swap(total_cmds, Ordering::Relaxed);
        let delta_cmds = total_cmds.saturating_sub(last_cmds);
        let ops_rate = (delta_cmds as f64 / elapsed_sec).round() as u64;
        self.ops_per_sec.store(ops_rate, Ordering::Relaxed);

        // 2. CPU Usage from /proc/self/stat
        if let Some(curr_jiffies) = read_proc_cpu_jiffies() {
            let prev_jiffies = self.last_cpu_jiffies.swap(curr_jiffies, Ordering::Relaxed);
            let delta_jiffies = curr_jiffies.saturating_sub(prev_jiffies);
            // In Linux, CLK_TCK is typically 100 ticks per sec
            let cpu_ratio = (delta_jiffies as f64 / 100.0) / elapsed_sec;
            let cpu_pct = (cpu_ratio * 100.0).clamp(0.0, 1000.0);
            self.cpu_percent_scaled
                .store((cpu_pct * 100.0) as u32, Ordering::Relaxed);
        }

        // 3. Process RSS Memory from /proc/self/statm
        if let Some(rss_b) = read_proc_rss_bytes() {
            self.rss_memory_bytes.store(rss_b, Ordering::Relaxed);
        }

        // 4. Save history snapshot for real-time charts
        let lat = self.latency_stats();
        let point = HistoryPoint {
            timestamp_sec: self.start_time.elapsed().as_secs(),
            hit_ratio: (self.hit_ratio() * 10.0).round() / 10.0,
            miss_ratio: (self.miss_ratio() * 10.0).round() / 10.0,
            ops_per_sec: ops_rate,
            avg_latency_us: (lat.avg_us * 10.0).round() / 10.0,
            p95_latency_us: lat.p95_us,
            p99_latency_us: lat.p99_us,
            memory_cache_bytes: current_cache_memory,
            memory_rss_bytes: self.rss_memory_bytes.load(Ordering::Relaxed),
            cpu_percent: self.cpu_percent(),
            connected_clients: self.connected_clients.load(Ordering::Relaxed).max(0),
            evicted_keys: self.evicted_keys.load(Ordering::Relaxed),
            expired_keys: self.expired_keys.load(Ordering::Relaxed),
        };

        let mut hist = self.history.write();
        if hist.len() >= HISTORY_CAPACITY {
            hist.pop_front();
        }
        hist.push_back(point);
    }

    pub fn cpu_percent(&self) -> f64 {
        self.cpu_percent_scaled.load(Ordering::Relaxed) as f64 / 100.0
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn get_history(&self) -> Vec<HistoryPoint> {
        self.history.read().iter().cloned().collect()
    }

    // ==========================================
    // JSON SERIALIZATION (ZERO DEPENDENCY)
    // ==========================================

    pub fn to_json(
        &self,
        current_cache_memory: usize,
        current_keys: usize,
        maxmemory: usize,
    ) -> String {
        let lat = self.latency_stats();
        let hit_r = (self.hit_ratio() * 100.0).round() / 100.0;
        let miss_r = (self.miss_ratio() * 100.0).round() / 100.0;
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let ops = self.ops_per_sec.load(Ordering::Relaxed);
        let total_cmds = self.total_commands.load(Ordering::Relaxed);
        let clients = self.connected_clients.load(Ordering::Relaxed).max(0);
        let peak_c = self.peak_clients.load(Ordering::Relaxed);
        let total_conn = self.total_connections.load(Ordering::Relaxed);
        let conn_errs = self.connection_errors.load(Ordering::Relaxed);
        let evicted = self.evicted_keys.load(Ordering::Relaxed);
        let expired = self.expired_keys.load(Ordering::Relaxed);
        let cpu = self.cpu_percent();
        let rss = self.rss_memory_bytes.load(Ordering::Relaxed);
        let uptime = self.uptime_seconds();

        let mem_pct = if maxmemory > 0 {
            ((current_cache_memory as f64 / maxmemory as f64) * 100.0).min(100.0)
        } else {
            0.0
        };

        let maxmem_str = if maxmemory > 0 {
            format_human_bytes(maxmemory as u64)
        } else {
            "Unlimited".to_string()
        };

        // History items
        let hist = self.get_history();
        let mut hist_json = String::new();
        for (i, p) in hist.iter().enumerate() {
            if i > 0 {
                hist_json.push(',');
            }
            hist_json.push_str(&format!(
                "{{\"t\":{},\"hr\":{:.1},\"mr\":{:.1},\"ops\":{},\"lat\":{:.1},\"p95\":{},\"mem\":{},\"rss\":{},\"cpu\":{:.1},\"cli\":{}}}",
                p.timestamp_sec,
                p.hit_ratio,
                p.miss_ratio,
                p.ops_per_sec,
                p.avg_latency_us,
                p.p95_latency_us,
                p.memory_cache_bytes,
                p.memory_rss_bytes,
                p.cpu_percent,
                p.connected_clients
            ));
        }

        let mut out = String::with_capacity(2048);
        out.push('{');
        out.push_str(&format!(
            "\"server\":{{\"status\":\"running\",\"uptime_seconds\":{},\"total_keys\":{}}},",
            uptime, current_keys
        ));
        out.push_str("\"metrics\":{");
        out.push_str(&format!("\"cache_hit_ratio\":{:.2},", hit_r));
        out.push_str(&format!("\"cache_miss_ratio\":{:.2},", miss_r));
        out.push_str(&format!("\"hits\":{},", hits));
        out.push_str(&format!("\"misses\":{},", misses));
        out.push_str(&format!("\"memory_cache_bytes\":{},", current_cache_memory));
        out.push_str(&format!(
            "\"memory_cache_human\":\"{}\",",
            format_human_bytes(current_cache_memory as u64)
        ));
        out.push_str(&format!("\"memory_rss_bytes\":{},", rss));
        out.push_str(&format!(
            "\"memory_rss_human\":\"{}\",",
            format_human_bytes(rss)
        ));
        out.push_str(&format!("\"maxmemory_bytes\":{},", maxmemory));
        out.push_str(&format!("\"maxmemory_human\":\"{}\",", maxmem_str));
        out.push_str(&format!("\"memory_usage_percent\":{:.2},", mem_pct));
        out.push_str(&format!("\"cpu_percent\":{:.2},", cpu));
        out.push_str(&format!("\"connected_clients\":{},", clients));
        out.push_str(&format!("\"peak_clients\":{},", peak_c));
        out.push_str(&format!("\"total_connections\":{},", total_conn));
        out.push_str(&format!("\"connection_errors\":{},", conn_errs));
        out.push_str(&format!("\"commands_per_sec\":{},", ops));
        out.push_str(&format!("\"total_commands\":{},", total_cmds));
        out.push_str(&format!("\"evicted_keys\":{},", evicted));
        out.push_str(&format!("\"expired_keys\":{},", expired));
        out.push_str(&format!(
            "\"latency\":{{\"avg_us\":{:.2},\"min_us\":{},\"max_us\":{},\"p50_us\":{},\"p95_us\":{},\"p99_us\":{}}}",
            lat.avg_us, lat.min_us, lat.max_us, lat.p50_us, lat.p95_us, lat.p99_us
        ));
        out.push_str("},");
        out.push_str(&format!("\"history\":[{}]", hist_json));
        out.push('}');
        out
    }

    // ==========================================
    // REDIS INFO OUTPUT FORMATTER
    // ==========================================

    pub fn format_redis_info(
        &self,
        current_cache_memory: usize,
        current_keys: usize,
        maxmemory: usize,
    ) -> String {
        let uptime = self.uptime_seconds();
        let clients = self.connected_clients.load(Ordering::Relaxed).max(0);
        let total_conn = self.total_connections.load(Ordering::Relaxed);
        let total_cmds = self.total_commands.load(Ordering::Relaxed);
        let ops = self.ops_per_sec.load(Ordering::Relaxed);
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let evicted = self.evicted_keys.load(Ordering::Relaxed);
        let expired = self.expired_keys.load(Ordering::Relaxed);
        let rss = self.rss_memory_bytes.load(Ordering::Relaxed);
        let cpu = self.cpu_percent();

        format!(
            "# Server\r\n\
             redis_version:7.0.0\r\n\
             redis_mode:standalone\r\n\
             os:Linux\r\n\
             arch_bits:64\r\n\
             uptime_in_seconds:{uptime}\r\n\
             uptime_in_days:{days}\r\n\
             \r\n\
             # Clients\r\n\
             connected_clients:{clients}\r\n\
             \r\n\
             # Memory\r\n\
             used_memory:{current_cache_memory}\r\n\
             used_memory_human:{used_human}\r\n\
             used_memory_rss:{rss}\r\n\
             used_memory_rss_human:{rss_human}\r\n\
             maxmemory:{maxmemory}\r\n\
             maxmemory_human:{max_human}\r\n\
             \r\n\
             # Stats\r\n\
             total_connections_received:{total_conn}\r\n\
             total_commands_processed:{total_cmds}\r\n\
             instantaneous_ops_per_sec:{ops}\r\n\
             keyspace_hits:{hits}\r\n\
             keyspace_misses:{misses}\r\n\
             evicted_keys:{evicted}\r\n\
             expired_keys:{expired}\r\n\
             \r\n\
             # CPU\r\n\
             used_cpu_percent:{cpu:.2}\r\n\
             \r\n\
             # Keyspace\r\n\
             db0:keys={current_keys},expires=0,avg_ttl=0\r\n",
            uptime = uptime,
            days = uptime / 86400,
            clients = clients,
            current_cache_memory = current_cache_memory,
            used_human = format_human_bytes(current_cache_memory as u64),
            rss = rss,
            rss_human = format_human_bytes(rss),
            maxmemory = maxmemory,
            max_human = if maxmemory > 0 {
                format_human_bytes(maxmemory as u64)
            } else {
                "0B".to_string()
            },
            total_conn = total_conn,
            total_cmds = total_cmds,
            ops = ops,
            hits = hits,
            misses = misses,
            evicted = evicted,
            expired = expired,
            cpu = cpu,
            current_keys = current_keys,
        )
    }
}

pub fn format_human_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Read total CPU jiffies (utime + stime) for current process from /proc/self/stat.
fn read_proc_cpu_jiffies() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let after_paren = stat.rfind(')')?;
    let rest = &stat[after_paren + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() > 12 {
        let utime: u64 = fields[11].parse().ok()?;
        let stime: u64 = fields[12].parse().ok()?;
        Some(utime + stime)
    } else {
        None
    }
}

/// Read resident set size (RSS) in bytes for current process from /proc/self/statm.
fn read_proc_rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let fields: Vec<&str> = statm.split_whitespace().collect();
    if fields.len() > 1 {
        let pages: u64 = fields[1].parse().ok()?;
        // Page size in Linux x86_64 is 4096 bytes
        Some(pages * 4096)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hit_and_miss_ratio() {
        let m = Metrics::new();
        assert_eq!(m.hit_ratio(), 0.0);
        assert_eq!(m.miss_ratio(), 0.0);

        m.record_hit();
        m.record_hit();
        m.record_hit();
        m.record_miss();

        // 3 hits, 1 miss => 75% hit ratio, 25% miss ratio
        assert!((m.hit_ratio() - 75.0).abs() < 0.001);
        assert!((m.miss_ratio() - 25.0).abs() < 0.001);
    }

    #[test]
    fn test_latency_stats() {
        let m = Metrics::new();
        m.record_command(10);
        m.record_command(20);
        m.record_command(30);
        m.record_command(40);
        m.record_command(50);

        let lat = m.latency_stats();
        assert_eq!(lat.min_us, 10);
        assert_eq!(lat.max_us, 50);
        assert_eq!(lat.avg_us, 30.0);
        assert_eq!(lat.p50_us, 30);
    }

    #[test]
    fn test_json_and_redis_info_generation() {
        let m = Metrics::new();
        m.record_hit();
        m.record_miss();
        m.record_command(15);
        m.client_connected();

        let json = m.to_json(1024, 5, 0);
        assert!(json.contains("\"cache_hit_ratio\":50"));
        assert!(json.contains("\"hits\":1"));
        assert!(json.contains("\"misses\":1"));
        assert!(json.contains("\"connected_clients\":1"));

        let info = m.format_redis_info(1024, 5, 0);
        assert!(info.contains("keyspace_hits:1"));
        assert!(info.contains("keyspace_misses:1"));
        assert!(info.contains("connected_clients:1"));
        assert!(info.contains("used_memory:1024"));
    }
}
