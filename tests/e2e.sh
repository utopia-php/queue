#!/bin/sh
# Run the e2e suite against local workers plus a KEDA-driven kind cluster.
# Expects the compose services (redis, redis-cluster) to be up — see
# docker-compose.yml — and provisions kind + KEDA + the ScaledJob for KedaTest.
set -e
cd "$(dirname "$0")/.."

. tests/keda-lib.sh

php tests/Queue/servers/Swoole/worker.php & SWOOLE=$!
php tests/Queue/servers/SwooleRedisCluster/worker.php & CLUSTER=$!
php tests/Queue/servers/Workerman/worker.php start & WORKERMAN=$!

cleanup() {
    kill -INT "$SWOOLE" "$CLUSTER" "$WORKERMAN" 2> /dev/null || true
    wait 2> /dev/null || true
    keda_down
}
trap cleanup EXIT INT TERM

keda_up

sleep 3

phpunit --testsuite e2e
