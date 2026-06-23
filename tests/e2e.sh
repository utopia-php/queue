#!/bin/sh
# Run the e2e suite against local workers. Expects the compose services
# (redis, redis-cluster) to be up — see docker-compose.yml.
set -e
cd "$(dirname "$0")/.."

php tests/Queue/servers/Swoole/worker.php & SWOOLE=$!
php tests/Queue/servers/SwooleRedisCluster/worker.php & CLUSTER=$!
php tests/Queue/servers/Workerman/worker.php start & WORKERMAN=$!

cleanup() {
    kill -INT "$SWOOLE" "$CLUSTER" "$WORKERMAN" 2> /dev/null || true
    wait 2> /dev/null || true
}
trap cleanup EXIT INT TERM

sleep 3

phpunit --testsuite e2e
