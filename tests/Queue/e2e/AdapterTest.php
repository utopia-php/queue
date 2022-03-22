<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Swoole\Table;
use Utopia\Queue\Adapter\SwooleAdapter;
use Utopia\Queue\Client;
use Utopia\Queue\Connection\RedisConnection;
use Utopia\Queue\Job;
use Utopia\Queue\Server;

use function Swoole\Coroutine\run;

class SwooleTest extends TestCase
{
    protected array $jobs;
    public function setUp(): void
    {
        $this->jobs = [];
    }

    public function testClient(): void
    {
        $connection = new RedisConnection('localhost', 6378);

        run(function () use ($connection) {
            $client = new Client('test', $connection);
            $this->assertTrue($client->enqueue([
                'value' => 123
            ]));
            $this->assertTrue($client->enqueue([
                'value' => 'haha'
            ]));
            \sleep(1);
            $this->assertTrue($client->enqueue([
                'stop' => true
            ]));
        });

        $adapter = new SwooleAdapter(1, $connection);
        $server = new Server($adapter, 'test');
        $server
            ->onJob(function (Job $job) use ($server) {
                $this->jobs = ['asd'];
                if (\array_key_exists('stop', $job->getPayload())) {
                    $server->shutdown();
                }
            });
        $server->start();
    }
}
