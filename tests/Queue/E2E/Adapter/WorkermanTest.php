<?php

namespace Tests\E2E\Adapter;

use Utopia\Queue\Broker\Redis as RedisPublisher;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class WorkermanTest extends Base
{
    protected function getPublisher(): Publisher
    {
        return new RedisPublisher(new Redis('redis', 6379), new Redis('redis', 6379));
    }

    protected function getQueue(): Queue
    {
        return new Queue('workerman');
    }
}
