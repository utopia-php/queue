<?php
namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Client;
use Utopia\Queue\Connection\RedisConnection;

use function Swoole\Coroutine\run;

class SwooleTest extends TestCase
{
    public function setUp(): void
    {

    }

    public function testClient(): void
    {
        run(function(){
            $connection = new RedisConnection('localhost', 6378);
            $client = new Client('test', $connection);
            $this->assertTrue($client->queue([
                'test' => 123
            ]));
        });
    }
}
