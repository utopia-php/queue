<?php

namespace Queue\E2E\Adapter;

use Tests\E2E\Adapter\Base;
use Utopia\Queue\Client;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Connection\RedisCluster;

class SwooleRedisClusterTest extends Base
{
    protected function getClient(): Client
    {
        $connection = new RedisCluster(['redis-cluster-0:6379', 'redis-cluster-1:6379', 'redis-cluster-2:6379']);
        $client = new Client('swoole-redis-cluster', $connection);

        return $client;
    }
}
