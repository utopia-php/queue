<?php

declare(strict_types=1);

namespace Tests\E2E\Adapter;

use Utopia\Pools\Adapter\Stack as Stack;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Queue\Broker\Pool;
use Utopia\Queue\Broker\Redis as RedisBroker;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

final class PoolTest extends Base
{
    protected function getPublisher(): Publisher
    {
        $pool = new UtopiaPool(new Stack(), 'redis', 1, fn(): \Utopia\Queue\Broker\Redis => new RedisBroker(new Redis('127.0.0.1', 16379), new Redis('127.0.0.1', 16379)));

        return new Pool($pool, $pool);
    }

    protected function getQueue(): Queue
    {
        return new Queue('pool');
    }
}
