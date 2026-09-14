use crate::db::Db;
use crate::metrics::Metrics;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

/// Embedded HTTP server providing real-time metrics API and Web UI Dashboard.
pub async fn start_web_server(
    bind_addr: SocketAddr,
    db: Db,
    metrics: Arc<Metrics>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("📊 Metrics Web Dashboard listening on http://{}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                let db = db.clone();
                let metrics = metrics.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_http_connection(socket, db, metrics).await {
                        // Connection reset or closed prematurely by client is normal
                        tracing::debug!("HTTP connection from {} closed: {}", peer, e);
                    }
                });
            }
            Err(e) => {
                error!("HTTP listener accept error: {}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn handle_http_connection(
    mut stream: TcpStream,
    db: Db,
    metrics: Arc<Metrics>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buf[..n]);
    let mut lines = request.lines();
    let request_line = match lines.next() {
        Some(l) => l,
        None => return Ok(()),
    };

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Ok(());
    }

    let method = parts[0];
    let uri = parts[1];

    let path = uri.split('?').next().unwrap_or(uri);
    let query = if uri.contains('?') {
        uri.split('?').nth(1).unwrap_or("")
    } else {
        ""
    };

    match (method, path) {
        ("GET", "/") => {
            let html = DASHBOARD_HTML;
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: text/html; charset=utf-8\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                html.len(),
                html
            );
            stream.write_all(response.as_bytes()).await?;
        }
        ("GET", "/api/metrics") => {
            let json = metrics.to_json(db.current_memory(), db.len(), db.maxmemory());
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/json; charset=utf-8\r\n\
                 Access-Control-Allow-Origin: *\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                json.len(),
                json
            );
            stream.write_all(response.as_bytes()).await?;
        }
        ("GET", "/api/stream") => {
            // Server-Sent Events (SSE) stream
            let headers = "HTTP/1.1 200 OK\r\n\
                           Content-Type: text/event-stream; charset=utf-8\r\n\
                           Cache-Control: no-cache, no-transform\r\n\
                           Connection: keep-alive\r\n\
                           Access-Control-Allow-Origin: *\r\n\
                           \r\n";
            stream.write_all(headers.as_bytes()).await?;

            let mut ticker = tokio::time::interval(Duration::from_millis(1000));
            loop {
                ticker.tick().await;
                let json = metrics.to_json(db.current_memory(), db.len(), db.maxmemory());
                let msg = format!("data: {}\n\n", json);
                if let Err(_) = stream.write_all(msg.as_bytes()).await {
                    break;
                }
            }
        }
        ("GET", "/api/simulate") | ("POST", "/api/simulate") => {
            let mut count: usize = 1000;
            let mut hit_ratio_target: u32 = 80;

            for pair in query.split('&') {
                let mut kv = pair.split('=');
                if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
                    if k == "count" || k == "ops" {
                        if let Ok(c) = v.parse::<usize>() {
                            count = c.clamp(10, 50000);
                        }
                    } else if k == "hit_ratio" || k == "ratio" {
                        if let Ok(r) = v.parse::<u32>() {
                            hit_ratio_target = r.clamp(0, 100);
                        }
                    }
                }
            }

            // Seed some benchmark keys
            for i in 0..100 {
                let k = bytes::Bytes::from(format!("sim:key:{}", i));
                let v = bytes::Bytes::from(format!("sim:val:{}", i));
                db.set(k, v, None);
            }

            // Run simulated traffic with specified hit ratio
            let hits_to_do = (count * hit_ratio_target as usize) / 100;
            let misses_to_do = count - hits_to_do;

            let start = std::time::Instant::now();
            for i in 0..hits_to_do {
                let key_id = i % 100;
                let k = format!("sim:key:{}", key_id);
                let _ = db.get(k.as_bytes());
                metrics.record_command(fastrand::u64(5..40));
            }

            for i in 0..misses_to_do {
                let k = format!("sim:nonexistent:{}", i);
                let _ = db.get(k.as_bytes());
                metrics.record_command(fastrand::u64(5..40));
            }
            let elapsed_ms = start.elapsed().as_millis();

            let resp_json = format!(
                "{{\"status\":\"ok\",\"simulated_ops\":{},\"target_hit_ratio\":{},\"elapsed_ms\":{}}}",
                count, hit_ratio_target, elapsed_ms
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: application/json; charset=utf-8\r\n\
                 Access-Control-Allow-Origin: *\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                resp_json.len(),
                resp_json
            );
            stream.write_all(response.as_bytes()).await?;
        }
        _ => {
            let not_found = "404 Not Found";
            let response = format!(
                "HTTP/1.1 404 Not Found\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                not_found.len(),
                not_found
            );
            stream.write_all(response.as_bytes()).await?;
        }
    }

    Ok(())
}

const DASHBOARD_HTML: &str = r###"<!DOCTYPE html>
<html lang="tr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>⚡ In-Memory Cache Monitoring Dashboard</title>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: rgba(17, 24, 39, 0.85);
      --card-border: rgba(255, 255, 255, 0.08);
      --text: #f3f4f6;
      --text-muted: #9ca3af;
      --accent: #3b82f6;
      --accent-glow: rgba(59, 130, 246, 0.25);
      --success: #10b981;
      --success-glow: rgba(16, 185, 129, 0.2);
      --warning: #f59e0b;
      --danger: #ef4444;
      --purple: #8b5cf6;
      --cyan: #06b6d4;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }

    body {
      background-color: var(--bg);
      background-image:
        radial-gradient(at 0% 0%, rgba(59, 130, 246, 0.08) 0px, transparent 50%),
        radial-gradient(at 100% 0%, rgba(139, 92, 246, 0.08) 0px, transparent 50%),
        radial-gradient(at 50% 100%, rgba(16, 185, 129, 0.05) 0px, transparent 50%);
      color: var(--text);
      min-height: 100vh;
      padding: 24px;
      overflow-x: hidden;
    }

    .container { max-width: 1360px; margin: 0 auto; }

    /* Header */
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding-bottom: 24px;
      border-bottom: 1px solid var(--card-border);
      margin-bottom: 24px;
      flex-wrap: wrap;
      gap: 16px;
    }

    .brand { display: flex; align-items: center; gap: 12px; }
    .brand h1 { font-size: 1.45rem; font-weight: 700; letter-spacing: -0.02em; display: flex; align-items: center; gap: 8px; }
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      background: var(--success-glow);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: var(--success);
      font-size: 0.75rem;
      font-weight: 600;
      border-radius: 9999px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .pulse-dot {
      width: 8px; height: 8px; border-radius: 50%; background: var(--success);
      box-shadow: 0 0 10px var(--success);
      animation: pulse 2s infinite;
    }
    @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(0.85); } }

    .header-actions { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
    .btn {
      background: #1f2937;
      color: var(--text);
      border: 1px solid var(--card-border);
      padding: 8px 14px;
      border-radius: 8px;
      font-size: 0.85rem;
      font-weight: 500;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.2s;
    }
    .btn:hover { background: #374151; border-color: rgba(255,255,255,0.2); }
    .btn-primary {
      background: linear-gradient(135deg, #2563eb, #1d4ed8);
      border-color: #3b82f6;
      box-shadow: 0 4px 12px var(--accent-glow);
    }
    .btn-primary:hover { background: linear-gradient(135deg, #1d4ed8, #1e40af); }
    .btn-accent {
      background: linear-gradient(135deg, #059669, #047857);
      border-color: #10b981;
    }
    .btn-accent:hover { background: linear-gradient(135deg, #047857, #065f46); }

    /* Hero Section: CACHE HIT RATIO */
    .hero-card {
      background: linear-gradient(135deg, rgba(17, 24, 39, 0.95), rgba(30, 41, 59, 0.75));
      border: 1px solid rgba(59, 130, 246, 0.3);
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.4), 0 0 30px rgba(59, 130, 246, 0.1);
      border-radius: 16px;
      padding: 28px;
      margin-bottom: 24px;
      display: grid;
      grid-template-columns: auto 1fr auto;
      gap: 32px;
      align-items: center;
    }
    @media (max-width: 860px) {
      .hero-card { grid-template-columns: 1fr; text-align: center; }
    }

    .hit-gauge-wrapper {
      position: relative;
      width: 140px;
      height: 140px;
      display: flex;
      align-items: center;
      justify-content: center;
      margin: 0 auto;
    }
    .hit-gauge-svg {
      transform: rotate(-90deg);
      width: 140px;
      height: 140px;
    }
    .hit-gauge-bg { fill: none; stroke: #1f2937; stroke-width: 12; }
    .hit-gauge-fill {
      fill: none;
      stroke: url(#gaugeGradient);
      stroke-width: 12;
      stroke-linecap: round;
      stroke-dasharray: 377;
      stroke-dashoffset: 377;
      transition: stroke-dashoffset 0.8s ease;
    }
    .hit-gauge-text {
      position: absolute;
      display: flex;
      flex-direction: column;
      align-items: center;
    }
    .hit-gauge-val { font-size: 1.85rem; font-weight: 800; letter-spacing: -0.03em; color: #fff; }
    .hit-gauge-sub { font-size: 0.72rem; text-transform: uppercase; color: var(--text-muted); font-weight: 600; letter-spacing: 0.05em; }

    .hero-details { display: flex; flex-direction: column; gap: 8px; }
    .hero-title-row { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
    .hero-title { font-size: 1.5rem; font-weight: 700; color: #fff; letter-spacing: -0.02em; }
    .hero-badge {
      padding: 4px 10px; border-radius: 6px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase;
    }
    .badge-excellent { background: var(--success-glow); color: var(--success); border: 1px solid rgba(16, 185, 129, 0.4); }
    .badge-moderate { background: rgba(245, 158, 11, 0.2); color: var(--warning); border: 1px solid rgba(245, 158, 11, 0.4); }
    .badge-poor { background: rgba(239, 68, 68, 0.2); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.4); }

    .hero-desc { color: var(--text-muted); font-size: 0.92rem; max-width: 650px; line-height: 1.5; }

    .hero-stats-row { display: flex; gap: 24px; margin-top: 8px; flex-wrap: wrap; }
    .hero-stat-item { display: flex; flex-direction: column; }
    .hero-stat-label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; font-weight: 600; }
    .hero-stat-value { font-size: 1.25rem; font-weight: 700; color: #fff; }

    .hero-actions { display: flex; flex-direction: column; gap: 8px; align-items: flex-end; }
    @media (max-width: 860px) {
      .hero-actions { align-items: center; }
    }

    /* Grid KPI Cards */
    .grid-kpi {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 18px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      backdrop-filter: blur(8px);
      transition: transform 0.2s, border-color 0.2s;
    }
    .card:hover {
      transform: translateY(-2px);
      border-color: rgba(255, 255, 255, 0.15);
    }

    .card-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
    }
    .card-title {
      font-size: 0.82rem;
      color: var(--text-muted);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .card-icon { font-size: 1.1rem; }
    .card-value {
      font-size: 1.75rem;
      font-weight: 800;
      color: #fff;
      letter-spacing: -0.02em;
      margin-bottom: 4px;
    }
    .card-sub {
      font-size: 0.82rem;
      color: var(--text-muted);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .progress-bar-bg {
      width: 100%;
      height: 6px;
      background: #1f2937;
      border-radius: 9999px;
      overflow: hidden;
      margin-top: 10px;
    }
    .progress-bar-fill {
      height: 100%;
      border-radius: 9999px;
      transition: width 0.4s ease;
    }

    /* Charts Section */
    .charts-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
      gap: 20px;
      margin-bottom: 24px;
    }
    @media (max-width: 680px) {
      .charts-grid { grid-template-columns: 1fr; }
    }

    .chart-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 18px;
    }
    .chart-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
    }
    .chart-title { font-size: 0.95rem; font-weight: 600; color: #fff; display: flex; align-items: center; gap: 8px; }
    .chart-legend { display: flex; gap: 12px; font-size: 0.78rem; color: var(--text-muted); }
    .legend-item { display: flex; align-items: center; gap: 4px; }
    .legend-color { width: 10px; height: 10px; border-radius: 2px; }

    canvas { width: 100%; height: 180px; display: block; }

    /* Footer */
    footer {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-top: 16px;
      border-top: 1px solid var(--card-border);
      color: var(--text-muted);
      font-size: 0.8rem;
      flex-wrap: wrap;
      gap: 12px;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="brand">
        <h1>⚡ In-Memory Cache</h1>
        <div class="status-badge">
          <span class="pulse-dot"></span>
          <span id="server-status">ONLINE</span>
        </div>
      </div>
      <div class="header-actions">
        <span style="font-size: 0.82rem; color: var(--text-muted);" id="server-uptime">Uptime: 0s</span>
        <button class="btn btn-accent" onclick="triggerSimulate(1000, 85)">
          🚀 Simulate 1k Ops (85% Hit)
        </button>
        <button class="btn" onclick="triggerSimulate(2000, 50)">
          ⚡ 50% Hit Mix
        </button>
        <button class="btn btn-primary" onclick="fetchMetricsManual()">
          🔄 Refresh
        </button>
      </div>
    </header>

    <!-- HERO METRIC: CACHE HIT RATIO -->
    <div class="hero-card">
      <div class="hit-gauge-wrapper">
        <svg class="hit-gauge-svg" viewBox="0 0 140 140">
          <defs>
            <linearGradient id="gaugeGradient" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stop-color="#10b981" />
              <stop offset="100%" stop-color="#3b82f6" />
            </linearGradient>
          </defs>
          <circle class="hit-gauge-bg" cx="70" cy="70" r="60" />
          <circle class="hit-gauge-fill" id="gauge-fill" cx="70" cy="70" r="60" />
        </svg>
        <div class="hit-gauge-text">
          <span class="hit-gauge-val" id="hero-hit-ratio">0.0%</span>
          <span class="hit-gauge-sub">HIT RATIO</span>
        </div>
      </div>

      <div class="hero-details">
        <div class="hero-title-row">
          <span class="hero-title">Cache Hit Ratio</span>
          <span class="hero-badge badge-excellent" id="hero-status-badge">OPTIMAL</span>
        </div>
        <p class="hero-desc">
          Önbellek verimliliğini gösteren en kritik performans göstergesidir. Yüksek oran, isteklerin doğrudan bellekten karşılandığını ve veritabanı/disk yükünün asgariye indiğini belirtir.
        </p>
        <div class="hero-stats-row">
          <div class="hero-stat-item">
            <span class="hero-stat-label">Cache Hits</span>
            <span class="hero-stat-value" id="hero-hits" style="color: var(--success)">0</span>
          </div>
          <div class="hero-stat-item">
            <span class="hero-stat-label">Cache Misses</span>
            <span class="hero-stat-value" id="hero-misses" style="color: var(--warning)">0</span>
          </div>
          <div class="hero-stat-item">
            <span class="hero-stat-label">Miss Ratio</span>
            <span class="hero-stat-value" id="hero-miss-ratio" style="color: var(--text-muted)">0.0%</span>
          </div>
          <div class="hero-stat-item">
            <span class="hero-stat-label">Total Lookups</span>
            <span class="hero-stat-value" id="hero-total-lookups">0</span>
          </div>
        </div>
      </div>

      <div class="hero-actions">
        <span style="font-size: 0.75rem; color: var(--text-muted);" id="sim-status">Hazır</span>
      </div>
    </div>

    <!-- KPI CARDS GRID -->
    <div class="grid-kpi">
      <!-- 1. Memory Usage -->
      <div class="card">
        <div class="card-head">
          <span class="card-title"><span class="card-icon">💾</span> Memory Usage</span>
          <span style="font-size: 0.75rem; color: var(--cyan);" id="kpi-mem-rss">RSS: 0 MB</span>
        </div>
        <div class="card-value" id="kpi-mem-used">0 B</div>
        <div class="card-sub">
          <span>Max: <strong id="kpi-mem-max">Unlimited</strong></span>
          <span id="kpi-mem-pct">0%</span>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="bar-mem" style="width: 0%; background: linear-gradient(90deg, var(--cyan), var(--accent));"></div>
        </div>
      </div>

      <!-- 2. CPU Usage -->
      <div class="card">
        <div class="card-head">
          <span class="card-title"><span class="card-icon">⚡</span> CPU Usage</span>
          <span style="font-size: 0.75rem; color: var(--purple);">Process Stat</span>
        </div>
        <div class="card-value" id="kpi-cpu">0.0%</div>
        <div class="card-sub">
          <span>Linux procfs jiffies</span>
          <span id="kpi-cpu-state">Normal</span>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="bar-cpu" style="width: 0%; background: linear-gradient(90deg, var(--purple), #ec4899);"></div>
        </div>
      </div>

      <!-- 3. Connected Clients -->
      <div class="card">
        <div class="card-head">
          <span class="card-title"><span class="card-icon">👥</span> Connected Clients</span>
          <span style="font-size: 0.75rem; color: var(--success);">TCP :6379</span>
        </div>
        <div class="card-value" id="kpi-clients">0</div>
        <div class="card-sub">
          <span>Peak: <strong id="kpi-peak-clients">0</strong></span>
          <span>Total: <strong id="kpi-total-conn">0</strong></span>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="bar-clients" style="width: 10%; background: var(--success);"></div>
        </div>
      </div>

      <!-- 4. Commands/sec (Throughput) -->
      <div class="card">
        <div class="card-head">
          <span class="card-title"><span class="card-icon">🚀</span> Commands / Sec</span>
          <span style="font-size: 0.75rem; color: var(--accent);">Throughput</span>
        </div>
        <div class="card-value" id="kpi-ops">0 ops/s</div>
        <div class="card-sub">
          <span>Total Processed:</span>
          <strong id="kpi-total-cmds">0</strong>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="bar-ops" style="width: 0%; background: linear-gradient(90deg, var(--accent), var(--cyan));"></div>
        </div>
      </div>

      <!-- 5. Cache Miss Ratio -->
      <div class="card">
        <div class="card-head">
          <span class="card-title"><span class="card-icon">🎯</span> Cache Miss Ratio</span>
          <span style="font-size: 0.75rem; color: var(--warning);">Keyspace</span>
        </div>
        <div class="card-value" id="kpi-miss-ratio">0.0%</div>
        <div class="card-sub">
          <span>Miss Count:</span>
          <strong id="kpi-miss-count">0</strong>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="bar-miss" style="width: 0%; background: var(--warning);"></div>
        </div>
      </div>

      <!-- 6. Evicted Keys -->
      <div class="card">
        <div class="card-head">
          <span class="card-title"><span class="card-icon">🧹</span> Evicted Keys</span>
          <span style="font-size: 0.75rem; color: #f97316;">LRU Sampler</span>
        </div>
        <div class="card-value" id="kpi-evicted">0</div>
        <div class="card-sub">
          <span>Policy: Approximated LRU</span>
          <span>Sample: 8</span>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="bar-evicted" style="width: 0%; background: #f97316;"></div>
        </div>
      </div>

      <!-- 7. Expired Keys -->
      <div class="card">
        <div class="card-head">
          <span class="card-title"><span class="card-icon">⏳</span> Expired Keys</span>
          <span style="font-size: 0.75rem; color: var(--purple);">TTL Engine</span>
        </div>
        <div class="card-value" id="kpi-expired">0</div>
        <div class="card-sub">
          <span>Sweeper (10Hz) & Passive</span>
          <span id="kpi-keys-count">Keys: 0</span>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="bar-expired" style="width: 0%; background: var(--purple);"></div>
        </div>
      </div>

      <!-- 8. Latency (P50/P95/P99) -->
      <div class="card">
        <div class="card-head">
          <span class="card-title"><span class="card-icon">⏱️</span> Latency</span>
          <span style="font-size: 0.75rem; color: var(--success);">Microseconds</span>
        </div>
        <div class="card-value" id="kpi-lat-avg">0 μs</div>
        <div class="card-sub">
          <span>P50: <strong id="kpi-lat-p50">0μs</strong></span>
          <span>P95: <strong id="kpi-lat-p95">0μs</strong></span>
          <span>P99: <strong id="kpi-lat-p99">0μs</strong></span>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="bar-lat" style="width: 15%; background: var(--success);"></div>
        </div>
      </div>

      <!-- 9. Connection Errors -->
      <div class="card">
        <div class="card-head">
          <span class="card-title"><span class="card-icon">⚠️</span> Connection Errors</span>
          <span style="font-size: 0.75rem; color: var(--danger);">Health</span>
        </div>
        <div class="card-value" id="kpi-conn-errs" style="color: var(--text);">0</div>
        <div class="card-sub">
          <span>Rejections & Drops:</span>
          <span id="kpi-err-rate">Clean</span>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" id="bar-errs" style="width: 0%; background: var(--danger);"></div>
        </div>
      </div>
    </div>

    <!-- CHARTS GRID -->
    <div class="charts-grid">
      <!-- Chart 1: Hit Ratio & Miss Ratio Timeline -->
      <div class="chart-card">
        <div class="chart-header">
          <span class="chart-title">📈 Cache Hit vs Miss Ratio Timeline</span>
          <div class="chart-legend">
            <div class="legend-item"><div class="legend-color" style="background: var(--success);"></div> Hit Ratio %</div>
            <div class="legend-item"><div class="legend-color" style="background: var(--warning);"></div> Miss Ratio %</div>
          </div>
        </div>
        <canvas id="chart-hit-ratio"></canvas>
      </div>

      <!-- Chart 2: Throughput (ops/sec) Timeline -->
      <div class="chart-card">
        <div class="chart-header">
          <span class="chart-title">🚀 Throughput (Commands / sec)</span>
          <div class="chart-legend">
            <div class="legend-item"><div class="legend-color" style="background: var(--accent);"></div> Ops/s</div>
          </div>
        </div>
        <canvas id="chart-ops"></canvas>
      </div>

      <!-- Chart 3: Latency Distribution Timeline -->
      <div class="chart-card">
        <div class="chart-header">
          <span class="chart-title">⏱️ Latency Distribution (μs)</span>
          <div class="chart-legend">
            <div class="legend-item"><div class="legend-color" style="background: var(--cyan);"></div> Avg (μs)</div>
            <div class="legend-item"><div class="legend-color" style="background: #f43f5e;"></div> P95 (μs)</div>
          </div>
        </div>
        <canvas id="chart-latency"></canvas>
      </div>

      <!-- Chart 4: Memory & CPU Timeline -->
      <div class="chart-card">
        <div class="chart-header">
          <span class="chart-title">💻 System Resources (CPU & RSS)</span>
          <div class="chart-legend">
            <div class="legend-item"><div class="legend-color" style="background: var(--purple);"></div> CPU %</div>
            <div class="legend-item"><div class="legend-color" style="background: var(--cyan);"></div> RSS (MB)</div>
          </div>
        </div>
        <canvas id="chart-resources"></canvas>
      </div>
    </div>

    <footer>
      <span>⚡ In-Memory Cache v0.1.0 • High-Performance Multi-Threaded Redis Compatible Engine</span>
      <span>REST API: <code>/api/metrics</code> • SSE Stream: <code>/api/stream</code></span>
    </footer>
  </div>

  <script>
    // State
    let historyData = [];

    // Format helpers
    function formatNumber(num) {
      if (num === undefined || num === null) return "0";
      return num.toLocaleString();
    }

    function formatUptime(seconds) {
      const d = Math.floor(seconds / 86400);
      const h = Math.floor((seconds % 86400) / 3600);
      const m = Math.floor((seconds % 3600) / 60);
      const s = seconds % 60;
      if (d > 0) return `${d}d ${h}h ${m}m`;
      if (h > 0) return `${h}h ${m}m ${s}s`;
      if (m > 0) return `${m}m ${s}s`;
      return `${s}s`;
    }

    // Update UI from metrics object
    function updateDashboard(data) {
      if (!data || !data.metrics) return;
      const m = data.metrics;
      const s = data.server || {};

      // Server info
      document.getElementById('server-uptime').innerText = `Uptime: ${formatUptime(s.uptime_seconds || 0)}`;

      // 1. HERO METRIC: CACHE HIT RATIO
      const hitRatio = m.cache_hit_ratio || 0;
      const missRatio = m.cache_miss_ratio || 0;
      document.getElementById('hero-hit-ratio').innerText = `${hitRatio.toFixed(1)}%`;
      document.getElementById('hero-hits').innerText = formatNumber(m.hits);
      document.getElementById('hero-misses').innerText = formatNumber(m.misses);
      document.getElementById('hero-miss-ratio').innerText = `${missRatio.toFixed(1)}%`;
      document.getElementById('hero-total-lookups').innerText = formatNumber((m.hits || 0) + (m.misses || 0));

      // Circular gauge animation (circumference = 2 * PI * 60 ~= 377)
      const circumference = 377;
      const offset = circumference - (hitRatio / 100) * circumference;
      const fill = document.getElementById('gauge-fill');
      fill.style.strokeDashoffset = offset;

      // Hero Badge
      const badge = document.getElementById('hero-status-badge');
      if (hitRatio >= 80) {
        badge.className = 'hero-badge badge-excellent';
        badge.innerText = 'OPTIMAL';
      } else if (hitRatio >= 50) {
        badge.className = 'hero-badge badge-moderate';
        badge.innerText = 'MODERATE';
      } else if ((m.hits + m.misses) > 0) {
        badge.className = 'hero-badge badge-poor';
        badge.innerText = 'LOW';
      } else {
        badge.className = 'hero-badge badge-moderate';
        badge.innerText = 'NO DATA';
      }

      // 2. Memory Usage
      document.getElementById('kpi-mem-used').innerText = m.memory_cache_human || "0 B";
      document.getElementById('kpi-mem-rss').innerText = `RSS: ${m.memory_rss_human || "0 B"}`;
      document.getElementById('kpi-mem-max').innerText = m.maxmemory_human || "Unlimited";
      document.getElementById('kpi-mem-pct').innerText = `${m.memory_usage_percent || 0}%`;
      document.getElementById('bar-mem').style.width = `${Math.min(100, Math.max(2, m.memory_usage_percent || 5))}%`;

      // 3. CPU Usage
      const cpu = m.cpu_percent || 0;
      document.getElementById('kpi-cpu').innerText = `${cpu.toFixed(1)}%`;
      document.getElementById('bar-cpu').style.width = `${Math.min(100, cpu * 2)}%`;
      document.getElementById('kpi-cpu-state').innerText = cpu > 80 ? 'Heavy Load' : (cpu > 40 ? 'Moderate' : 'Optimal');

      // 4. Clients
      document.getElementById('kpi-clients').innerText = formatNumber(m.connected_clients);
      document.getElementById('kpi-peak-clients').innerText = formatNumber(m.peak_clients);
      document.getElementById('kpi-total-conn').innerText = formatNumber(m.total_connections);

      // 5. Commands / Sec
      document.getElementById('kpi-ops').innerText = `${formatNumber(m.commands_per_sec)} ops/s`;
      document.getElementById('kpi-total-cmds').innerText = formatNumber(m.total_commands);
      const opsPct = Math.min(100, (m.commands_per_sec / 10000) * 100);
      document.getElementById('bar-ops').style.width = `${Math.max(3, opsPct)}%`;

      // 6. Cache Miss Ratio
      document.getElementById('kpi-miss-ratio').innerText = `${missRatio.toFixed(1)}%`;
      document.getElementById('kpi-miss-count').innerText = formatNumber(m.misses);
      document.getElementById('bar-miss').style.width = `${Math.min(100, missRatio)}%`;

      // 7. Evicted Keys
      document.getElementById('kpi-evicted').innerText = formatNumber(m.evicted_keys);

      // 8. Expired Keys
      document.getElementById('kpi-expired').innerText = formatNumber(m.expired_keys);
      document.getElementById('kpi-keys-count').innerText = `Total Keys: ${formatNumber(s.total_keys || 0)}`;

      // 9. Latency
      const lat = m.latency || {};
      document.getElementById('kpi-lat-avg').innerText = `${(lat.avg_us || 0).toFixed(1)} μs`;
      document.getElementById('kpi-lat-p50').innerText = `${lat.p50_us || 0}μs`;
      document.getElementById('kpi-lat-p95').innerText = `${lat.p95_us || 0}μs`;
      document.getElementById('kpi-lat-p99').innerText = `${lat.p99_us || 0}μs`;

      // 10. Connection Errors
      const errs = m.connection_errors || 0;
      const errEl = document.getElementById('kpi-conn-errs');
      errEl.innerText = formatNumber(errs);
      errEl.style.color = errs > 0 ? 'var(--danger)' : '#fff';
      document.getElementById('kpi-err-rate').innerText = errs > 0 ? 'Errors Detected' : 'All Healthy';
      document.getElementById('bar-errs').style.width = errs > 0 ? '100%' : '0%';

      // Update charts
      if (data.history && data.history.length > 0) {
        historyData = data.history;
      } else {
        historyData.push({
          t: Date.now() / 1000,
          hr: hitRatio,
          mr: missRatio,
          ops: m.commands_per_sec || 0,
          lat: lat.avg_us || 0,
          p95: lat.p95_us || 0,
          mem: m.memory_cache_bytes || 0,
          rss: m.memory_rss_bytes || 0,
          cpu: cpu,
          cli: m.connected_clients || 0,
        });
        if (historyData.length > 60) historyData.shift();
      }
      renderAllCharts();
    }

    // Canvas Chart Renderer
    function drawLineChart(canvasId, seriesList, options = {}) {
      const canvas = document.getElementById(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.scale(dpr, dpr);

      const w = rect.width;
      const h = rect.height;
      const padLeft = 40;
      const padRight = 10;
      const padTop = 15;
      const padBottom = 25;
      const chartW = w - padLeft - padRight;
      const chartH = h - padTop - padBottom;

      ctx.clearRect(0, 0, w, h);

      if (historyData.length < 2) {
        ctx.fillStyle = "#6b7280";
        ctx.font = "12px sans-serif";
        ctx.textAlign = "center";
        ctx.fillText("Toplanan veri bekleniyor...", w / 2, h / 2);
        return;
      }

      // Determine yMax
      let maxY = options.fixedMax || 0;
      if (!options.fixedMax) {
        for (const s of seriesList) {
          for (const d of historyData) {
            const val = s.getValue(d);
            if (val > maxY) maxY = val;
          }
        }
        maxY = maxY > 0 ? maxY * 1.15 : 10;
      }

      // Draw Grid Lines
      ctx.strokeStyle = "rgba(255, 255, 255, 0.05)";
      ctx.lineWidth = 1;
      ctx.fillStyle = "#6b7280";
      ctx.font = "10px sans-serif";
      ctx.textAlign = "right";

      for (let i = 0; i <= 4; i++) {
        const y = padTop + chartH - (i / 4) * chartH;
        ctx.beginPath();
        ctx.moveTo(padLeft, y);
        ctx.lineTo(w - padRight, y);
        ctx.stroke();

        const labelVal = (maxY * (i / 4));
        const labelStr = options.isPercent ? `${labelVal.toFixed(0)}%` : (labelVal >= 1000 ? `${(labelVal/1000).toFixed(1)}k` : labelVal.toFixed(0));
        ctx.fillText(labelStr, padLeft - 6, y + 3);
      }

      // Draw series
      seriesList.forEach(series => {
        ctx.beginPath();
        ctx.strokeStyle = series.color;
        ctx.lineWidth = series.width || 2;

        const points = [];
        for (let i = 0; i < historyData.length; i++) {
          const x = padLeft + (i / (historyData.length - 1)) * chartW;
          const val = series.getValue(historyData[i]);
          const normY = Math.min(1, Math.max(0, val / maxY));
          const y = padTop + chartH - normY * chartH;
          points.push({ x, y });
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }
        ctx.stroke();

        if (series.fill) {
          ctx.lineTo(points[points.length - 1].x, padTop + chartH);
          ctx.lineTo(points[0].x, padTop + chartH);
          ctx.closePath();
          ctx.fillStyle = series.fill;
          ctx.fill();
        }
      });
    }

    function renderAllCharts() {
      // 1. Hit Ratio & Miss Ratio Chart
      drawLineChart('chart-hit-ratio', [
        { getValue: d => d.hr, color: '#10b981', fill: 'rgba(16, 185, 129, 0.12)', width: 2.5 },
        { getValue: d => d.mr, color: '#f59e0b', width: 1.5 }
      ], { fixedMax: 100, isPercent: true });

      // 2. Throughput Chart
      drawLineChart('chart-ops', [
        { getValue: d => d.ops, color: '#3b82f6', fill: 'rgba(59, 130, 246, 0.15)', width: 2.5 }
      ]);

      // 3. Latency Chart
      drawLineChart('chart-latency', [
        { getValue: d => d.lat, color: '#06b6d4', width: 2 },
        { getValue: d => d.p95, color: '#f43f5e', width: 1.5 }
      ]);

      // 4. Resources Chart (CPU % & RSS in MB)
      drawLineChart('chart-resources', [
        { getValue: d => d.cpu, color: '#8b5cf6', width: 2 },
        { getValue: d => (d.rss || 0) / (1024 * 1024), color: '#06b6d4', width: 1.5 }
      ]);
    }

    // Manual Refresh API Call
    async function fetchMetricsManual() {
      try {
        const res = await fetch('/api/metrics');
        if (res.ok) {
          const data = await res.json();
          updateDashboard(data);
        }
      } catch (err) {
        console.error("Fetch metrics error:", err);
      }
    }

    // Simulate Traffic Helper
    async function triggerSimulate(count, hitRatio) {
      const statusEl = document.getElementById('sim-status');
      statusEl.innerText = `Simüle ediliyor: ${count} istek (%${hitRatio} Hit)...`;
      try {
        const res = await fetch(`/api/simulate?count=${count}&hit_ratio=${hitRatio}`);
        if (res.ok) {
          const r = await res.json();
          statusEl.innerText = `✅ ${r.simulated_ops} istek ${r.elapsed_ms}ms içinde tamamlandı!`;
          fetchMetricsManual();
        }
      } catch (e) {
        statusEl.innerText = '❌ Simülasyon hatası';
      }
      setTimeout(() => { statusEl.innerText = 'Hazır'; }, 4000);
    }

    // Connect via Server-Sent Events (SSE) for zero-latency live updates
    function connectSSE() {
      if (window.EventSource) {
        const sse = new EventSource('/api/stream');
        sse.onmessage = function(event) {
          try {
            const data = JSON.parse(event.data);
            updateDashboard(data);
          } catch (e) {
            console.error("SSE parse error:", e);
          }
        };
        sse.onerror = function() {
          console.warn("SSE stream disconnected, falling back to 1s polling...");
          sse.close();
          setInterval(fetchMetricsManual, 1000);
        };
      } else {
        setInterval(fetchMetricsManual, 1000);
      }
    }

    // Window resize handler for canvas redraw
    window.addEventListener('resize', renderAllCharts);

    // Initial load
    fetchMetricsManual();
    connectSSE();
  </script>
</body>
</html>"###;
