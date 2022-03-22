<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Client;
use Utopia\Queue\Connection\Redis;

use function Swoole\Coroutine\run;

class SwooleTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function testClient(): void
    {
        $connection = new Redis('localhost', 6378);

        run(function () use ($connection) {
            $client = new Client('test', $connection);

            $this->assertTrue($client->enqueue([
                'value' => 123
            ]));
            $this->assertTrue($client->enqueue([
                'value' => 'haha'
            ]));
            $this->assertTrue($client->enqueue([
                'stop' => true
            ]));
        });
    }
}
