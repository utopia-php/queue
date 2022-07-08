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

    public function testEvents(): void
    {
        $connection = new Redis('redis', 6379);

        $client = new Client('workerman', $connection);
        $client->resetStats();

        $this->assertTrue($client->enqueue([
            'type' => 'test_string',
            'value' => 'lorem ipsum'
        ]));

        $this->assertTrue($client->enqueue([
            'type' => 'test_number',
            'value' => 123
        ]));

        $this->assertTrue($client->enqueue([
            'type' => 'test_number',
            'value' => 123.456
        ]));

        $this->assertTrue($client->enqueue([
            'type' => 'test_bool',
            'value' => true
        ]));

        $this->assertTrue($client->enqueue([
            'type' => 'test_null',
            'value' => null
        ]));

        $this->assertTrue($client->enqueue([
            'type' => 'test_array',
            'value' => [
                1,
                2,
                3
            ]
        ]));

        $this->assertTrue($client->enqueue([
            'type' => 'test_assoc',
            'value' => [
                'string' => 'ipsum',
                'number' => 123,
                'bool' => true,
                'null' => null
            ]
        ]));

        $this->assertTrue($client->enqueue([
            'type' => 'test_exception'
        ]));

        sleep(1);

        $this->assertEquals(8, $client->sumTotalJobs());
        $this->assertEquals(0, $client->getQueueSize());
        $this->assertEquals(0, $client->sumProcessingJobs());
        $this->assertEquals(1, $client->sumFailedJobs());
        $this->assertEquals(7, $client->sumSuccessfulJobs());
    }

    public function testSwoole(): void
    {
        $connection = new RedisSwoole('redis', 6379);

        run(function () use ($connection) {
            $client = new Client('swoole', $connection);
            go(function () use ($client) {
                $client->resetStats();
                $this->assertTrue($client->enqueue([
                    'type' => 'test_string',
                    'value' => 'lorem ipsum'
                ]));
                $this->assertTrue($client->enqueue([
                    'type' => 'test_number',
                    'value' => 123
                ]));
                $this->assertTrue($client->enqueue([
                    'type' => 'test_number',
                    'value' => 123.456
                ]));

                $this->assertTrue($client->enqueue([
                    'type' => 'test_bool',
                    'value' => true
                ]));

                $this->assertTrue($client->enqueue([
                    'type' => 'test_null',
                    'value' => null
                ]));

                $this->assertTrue($client->enqueue([
                    'type' => 'test_array',
                    'value' => [
                        1,
                        2,
                        3
                    ]
                ]));

                $this->assertTrue($client->enqueue([
                    'type' => 'test_assoc',
                    'value' => [
                        'string' => 'ipsum',
                        'number' => 123,
                        'bool' => true,
                        'null' => null
                    ]
                ]));

                $this->assertTrue($client->enqueue([
                    'type' => 'test_exception'
                ]));

                sleep(1);

                $this->assertEquals(8, $client->sumTotalJobs());
                $this->assertEquals(0, $client->sumProcessingJobs());
                $this->assertEquals(1, $client->sumFailedJobs());
                $this->assertEquals(7, $client->sumSuccessfulJobs());
            });
        });
    }
}
