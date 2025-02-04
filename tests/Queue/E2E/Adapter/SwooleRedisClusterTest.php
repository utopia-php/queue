<?php

namespace Queue\E2E\Adapter;

use Tests\E2E\Adapter\Base;
use Utopia\Queue\Broker\Redis;
use Utopia\Queue\Connection\RedisCluster;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class SwooleRedisClusterTest extends Base
{
    protected function getPublisher(): Publisher
    {
        $connection = new RedisCluster(['redis-cluster-0:6379', 'redis-cluster-1:6379', 'redis-cluster-2:6379']);
        return new Redis($connection);
    }

    protected function getQueue(): Queue
    {
        return new Queue('swoole-redis-cluster');
    }
}
