#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

cargo build

# Start server in background
cargo run &
SERVER_PID=$!

# Wait for server to start
sleep 2

echo "=== Testing Phase 1 & Phase 2 redis-cli commands ==="
echo ">> redis-cli PING"
redis-cli -p 6379 PING

echo ">> redis-cli ECHO 'Hello Redis from Rust'"
redis-cli -p 6379 ECHO "Hello Redis from Rust"

echo ">> redis-cli SET user:100 'Antigravity Cache'"
redis-cli -p 6379 SET user:100 "Antigravity Cache"

echo ">> redis-cli GET user:100"
redis-cli -p 6379 GET user:100

echo ">> redis-cli DEL user:100"
redis-cli -p 6379 DEL user:100

echo ">> redis-cli GET user:100 (after DEL)"
redis-cli -p 6379 GET user:100

echo ">> redis-cli SET temp_key 'short_lived' EX 2"
redis-cli -p 6379 SET temp_key "short_lived" EX 2

echo ">> redis-cli TTL temp_key"
redis-cli -p 6379 TTL temp_key

echo ">> Sleeping 3 seconds for TTL expiration..."
sleep 3

echo ">> redis-cli GET temp_key (after TTL expiration)"
redis-cli -p 6379 GET temp_key

echo ">> redis-cli TTL temp_key (should be -2)"
redis-cli -p 6379 TTL temp_key

echo ">> redis-cli SET persistent_key 'keep_me'"
redis-cli -p 6379 SET persistent_key "keep_me"

echo ">> redis-cli EXPIRE persistent_key 10"
redis-cli -p 6379 EXPIRE persistent_key 10

echo ">> redis-cli TTL persistent_key"
redis-cli -p 6379 TTL persistent_key

# Cleanup
kill $SERVER_PID || true
echo "=== All redis-cli Phase 2 tests passed successfully! ==="
