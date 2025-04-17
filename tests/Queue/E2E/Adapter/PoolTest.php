<?php

namespace Tests\E2E\Adapter;

use Tests\E2E\Adapter\Base;
use Utopia\Queue\Broker\Redis as RedisBroker;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class PoolTest extends Base
{
    protected function getPublisher(): Publisher
    {
        $connection = new Redis('redis', 6379);
        return new RedisBroker($connection);
    }

    protected function getQueue(): Queue
    {
        return new Queue('pool');
    }
}
