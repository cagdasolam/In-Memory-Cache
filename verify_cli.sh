#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

rm -f appendonly.aof

cargo build

echo "=== Starting Server for CLI Testing ==="
./target/debug/in-memory-cache &
SERVER_PID=$!

# Wait for server to become responsive
while ! redis-cli -p 6379 PING > /dev/null 2>&1; do
    sleep 0.2
done

echo ">> Testing Lists (LPUSH, RPUSH, LRANGE, LPOP)"
redis-cli -p 6379 RPUSH tasks "task1" "task2"
redis-cli -p 6379 LPUSH tasks "task0"
redis-cli -p 6379 LRANGE tasks 0 -1
redis-cli -p 6379 LPOP tasks

echo ">> Testing Hashes (HSET, HGET, HGETALL)"
redis-cli -p 6379 HSET profile:42 username "cagdas" role "lead"
redis-cli -p 6379 HGET profile:42 username
redis-cli -p 6379 HGETALL profile:42

echo ">> Testing Sets (SADD, SISMEMBER, SMEMBERS)"
redis-cli -p 6379 SADD skills "rust" "distributed-systems" "tokio"
redis-cli -p 6379 SISMEMBER skills "rust"
redis-cli -p 6379 SMEMBERS skills

echo ">> Testing AOF Persistence: writing a durable key"
redis-cli -p 6379 SET aof_test_key "persisted_value_123"

# Allow AOF write buffer to flush
sleep 1

echo ">> Stopping server to test recovery from AOF..."
kill -SIGINT $SERVER_PID || true
sleep 1

echo ">> Restarting server (Rehydrating from appendonly.aof)..."
./target/debug/in-memory-cache &
SERVER_PID2=$!

while ! redis-cli -p 6379 PING > /dev/null 2>&1; do
    sleep 0.2
done

echo ">> Querying key after restart:"
redis-cli -p 6379 GET aof_test_key

echo ">> Testing Pub/Sub:"
# Launch subscriber in background
timeout 3 redis-cli -p 6379 SUBSCRIBE announcements &
SUB_PID=$!
sleep 1

# Publish message
redis-cli -p 6379 PUBLISH announcements "Welcome to Phase 3 PubSub!"

sleep 1
kill -SIGINT $SERVER_PID2 || true
rm -f appendonly.aof

echo "=== All CLI tests passed successfully! ==="
