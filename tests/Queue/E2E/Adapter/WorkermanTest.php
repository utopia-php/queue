<?php

namespace Tests\E2E\Adapter;

use Utopia\Queue\Client;
use Utopia\Queue\Connection\Redis;

class WorkermanTest extends Base
{
    protected function getClient(): Client
    {
        $connection = new Redis('redis', 6379);
        $client = new Client('workerman', $connection);

        return $client;
    }
}
