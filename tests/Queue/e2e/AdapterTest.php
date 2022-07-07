<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Client;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Connection\RedisSwoole;

use function Swoole\Coroutine\go;
use function Swoole\Coroutine\run;

class SwooleTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function testWorkerman(): void
    {
        $connection = new Redis('redis', 6379);

        $client = new Client('workerman', $connection);

        $this->assertTrue($client->enqueue([
            'value' => 'lorem ipsum'
        ]));

        $this->assertTrue($client->enqueue([
            'value' => 123
        ]));

        $this->assertTrue($client->enqueue([
            'value' => true
        ]));

        $this->assertTrue($client->enqueue([
            'value' => [
                '1',
                '2',
                '3'
            ]
        ]));

        $this->assertTrue($client->enqueue([
            'value' => [
                'string' => 'ipsum',
                'number' => 123,
                'bool' => true,
                'null' => null
            ]
        ]));

        $this->assertTrue($client->enqueue([
            'stop' => true
        ]));

        sleep(1);

        $this->assertEquals(6, $client->sumTotalJobs());
        $this->assertEquals(0, $client->getQueueSize());
        $this->assertEquals(0, $client->sumProcessingJobs());
        $this->assertEquals(1, $client->sumFailedJobs());
        $this->assertEquals(5, $client->sumSuccessfulJobs());
    }

    public function testSwoole(): void
    {
        $connection = new RedisSwoole('redis', 6379);

        run(function () use ($connection) {
            $client = new Client('swoole', $connection);

            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 'lorem ipsum'
                ]));
            });

            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 123
                ]));
            });

            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => true
                ]));
            });

            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => [
                        '1',
                        '2',
                        '3'
                    ]
                ]));
            });

            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => [
                        'string' => 'ipsum',
                        'number' => 123,
                        'bool' => true,
                        'null' => null
                    ]
                ]));
            });

            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'stop' => true
                ]));
            });

            sleep(1);

            $this->assertEquals(6, $client->sumTotalJobs());
            $this->assertEquals(0, $client->getQueueSize());
            $this->assertEquals(0, $client->sumProcessingJobs());
            $this->assertEquals(1, $client->sumFailedJobs());
            $this->assertEquals(5, $client->sumSuccessfulJobs());
        });
    }
}
