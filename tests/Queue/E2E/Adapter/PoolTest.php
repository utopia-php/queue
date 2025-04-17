<?php

namespace Tests\E2E\Adapter;

use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Queue\Broker\Pool;
use Utopia\Queue\Broker\Redis as RedisBroker;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class PoolTest extends Base
{
    protected function getPublisher(): Publisher
    {
        $pool = new UtopiaPool('redis', 1, function () {
            return new RedisBroker(new Redis('redis', 6379));
        });

        return new Pool($pool, $pool);
    }

    protected function getQueue(): Queue
    {
        return new Queue('pool');
    }
}
