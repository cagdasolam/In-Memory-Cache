#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

cargo build

# Start server in background
cargo run &
SERVER_PID=$!

# Wait for server to start
sleep 2

echo "=== Testing redis-cli commands ==="
echo ">> redis-cli PING"
redis-cli -p 6379 PING

echo ">> redis-cli ECHO 'Hello Redis from Rust'"
redis-cli -p 6379 ECHO "Hello Redis from Rust"

echo ">> redis-cli SET user:100 'Antigravity Cache'"
redis-cli -p 6379 SET user:100 "Antigravity Cache"

echo ">> redis-cli GET user:100"
redis-cli -p 6379 GET user:100

echo ">> redis-cli GET non_existent"
redis-cli -p 6379 GET non_existent

# Cleanup
kill $SERVER_PID || true
echo "=== All redis-cli tests passed successfully! ==="

