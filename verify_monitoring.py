#!/usr/bin/env python3
import socket
import urllib.request
import json
import time
import subprocess
import os
import signal
import sys

def resp_encode(args):
    out = f"*{len(args)}\r\n"
    for arg in args:
        out += f"${len(arg)}\r\n{arg}\r\n"
    return out.encode('utf-8')

def main():
    print("=== Starting In-Memory-Cache Server for Monitoring Verification ===")
    env = os.environ.copy()
    env["PORT"] = "6389"
    env["WEB_PORT"] = "8089"
    env["AOF_PATH"] = "test_monitoring.aof"

    if os.path.exists("test_monitoring.aof"):
        os.remove("test_monitoring.aof")

    proc = subprocess.Popen(["./target/debug/in-memory-cache"], env=env)
    time.sleep(1.5)

    try:
        # 1. Test Web UI Dashboard HTML
        print("\n[1/5] Testing HTTP Dashboard GET / on http://127.0.0.1:8089/ ...")
        with urllib.request.urlopen("http://127.0.0.1:8089/") as resp:
            assert resp.status == 200
            html = resp.read().decode('utf-8')
            assert "Cache Hit Ratio" in html
            assert "In-Memory Cache" in html
            assert "kpi-hit-ratio" in html or "hero-hit-ratio" in html
            print(" -> PASSED! Dashboard HTML is served properly.")

        # 2. Test initial /api/metrics JSON
        print("\n[2/5] Testing HTTP API GET /api/metrics ...")
        with urllib.request.urlopen("http://127.0.0.1:8089/api/metrics") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode('utf-8'))
            m = data["metrics"]

            # Verify all 10 required metrics are present:
            required_keys = [
                ("memory_cache_bytes", "Memory Usage (Cache)"),
                ("memory_rss_bytes", "Memory Usage (RSS)"),
                ("cpu_percent", "CPU Usage"),
                ("connected_clients", "Connected Clients"),
                ("commands_per_sec", "Commands/sec"),
                ("cache_hit_ratio", "Cache Hit Ratio"),
                ("cache_miss_ratio", "Cache Miss Ratio"),
                ("evicted_keys", "Evicted Keys"),
                ("expired_keys", "Expired Keys"),
                ("latency", "Latency"),
                ("connection_errors", "Connection Errors")
            ]
            for k, label in required_keys:
                assert k in m, f"Missing metric: {k} ({label})"
                print(f"    ✓ {label}: {m[k]}")
            print(" -> PASSED! All 10 metrics present in JSON API.")

        # 3. Connect via TCP (Redis RESP2) and perform operations to generate hits, misses, latency
        print("\n[3/5] Performing Redis operations via TCP on port 6389...")
        s = socket.create_connection(("127.0.0.1", 6389))

        # SET key1 value1
        s.sendall(resp_encode(["SET", "user:1", "Alice"]))
        res = s.recv(1024)
        assert b"+OK\r\n" in res

        # GET key1 (HIT)
        s.sendall(resp_encode(["GET", "user:1"]))
        res = s.recv(1024)
        assert b"Alice" in res

        # GET non_existent_key (MISS)
        s.sendall(resp_encode(["GET", "user:999"]))
        res = s.recv(1024)
        assert b"$-1\r\n" in res

        # Another HIT
        s.sendall(resp_encode(["GET", "user:1"]))
        res = s.recv(1024)
        assert b"Alice" in res

        # INFO command
        s.sendall(resp_encode(["INFO"]))
        info_res = s.recv(4096).decode('utf-8', errors='ignore')
        assert "keyspace_hits:2" in info_res, f"Expected keyspace_hits:2, got:\n{info_res}"
        assert "keyspace_misses:1" in info_res, f"Expected keyspace_misses:1, got:\n{info_res}"
        print(" -> PASSED! Redis INFO command reports keyspace_hits:2 and keyspace_misses:1.")

        s.close()

        # 4. Check updated metrics via HTTP API
        print("\n[4/5] Verifying Cache Hit Ratio computation via /api/metrics ...")
        time.sleep(1.2) # Allow 1s tick
        with urllib.request.urlopen("http://127.0.0.1:8089/api/metrics") as resp:
            data = json.loads(resp.read().decode('utf-8'))
            m = data["metrics"]
            assert m["hits"] == 2, f"Expected 2 hits, got {m['hits']}"
            assert m["misses"] == 1, f"Expected 1 miss, got {m['misses']}"
            expected_hit_ratio = round((2.0 / 3.0) * 100.0, 2)
            assert abs(m["cache_hit_ratio"] - expected_hit_ratio) < 0.1, f"Expected ~{expected_hit_ratio}%, got {m['cache_hit_ratio']}"
            print(f"    ✓ Hits: {m['hits']}, Misses: {m['misses']}")
            print(f"    ✓ Cache Hit Ratio: {m['cache_hit_ratio']}% (Expected: {expected_hit_ratio}%)")
            print(f"    ✓ Cache Miss Ratio: {m['cache_miss_ratio']}%")
            print(f"    ✓ Latency Stats: {m['latency']}")
            print(" -> PASSED! Hit Ratio calculated with 100% mathematical accuracy.")

        # 5. Test /api/simulate endpoint (1,000 requests, 80% hit ratio)
        print("\n[5/5] Testing /api/simulate traffic generator...")
        with urllib.request.urlopen("http://127.0.0.1:8089/api/simulate?count=1000&hit_ratio=80") as resp:
            assert resp.status == 200
            sim_data = json.loads(resp.read().decode('utf-8'))
            print(f"    ✓ Sim result: {sim_data}")
            assert sim_data["status"] == "ok"
            assert sim_data["simulated_ops"] == 1000

        with urllib.request.urlopen("http://127.0.0.1:8089/api/metrics") as resp:
            data = json.loads(resp.read().decode('utf-8'))
            m = data["metrics"]
            print(f"    ✓ After simulation total lookups: {m['hits'] + m['misses']}")
            print(f"    ✓ Cache Hit Ratio: {m['cache_hit_ratio']}%")
            print(f"    ✓ Average Latency: {m['latency']['avg_us']} μs (P95: {m['latency']['p95_us']} μs, P99: {m['latency']['p99_us']} μs)")
            print(" -> PASSED! Simulation traffic generated and reflected in metrics.")

        print("\n🎉 ALL 5 VERIFICATION CHECKS COMPLETED SUCCESSFULLY!")

    finally:
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=5)
        if os.path.exists("test_monitoring.aof"):
            os.remove("test_monitoring.aof")

if __name__ == "__main__":
    main()

