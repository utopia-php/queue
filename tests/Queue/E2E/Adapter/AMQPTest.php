<?php

namespace Tests\E2E\Adapter;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Connection\AMQPSwooleConnection;
use Utopia\Queue\Broker\AMQP;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

use function Co\run;

class AMQPTest extends Base
{
    private string $connectionClass;

    public function connectionsProvider(): array
    {
        return [
            'AMQPStreamConnection' => [AMQPStreamConnection::class],
            'AMQPSwooleConnection' => [AMQPSwooleConnection::class],
        ];
    }

    /**
     * @dataProvider connectionsProvider
     */
    protected function getPublisher(): Publisher
    {
        if ($this->connectionClass === AMQPSwooleConnection::class && !extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is not available');
        }

        return new AMQP(host: 'amqp', port: 5672, user: 'amqp', password: 'amqp', connectionClass: $this->connectionClass);
    }

    protected function getQueue(): Queue
    {
        return new Queue('amqp');
    }

    /**
     * @dataProvider connectionsProvider
     */
    public function testEventsWithConnection(string $connectionClass): void
    {
        $this->connectionClass = $connectionClass;
        parent::testEvents();
    }

    /**
     * @dataProvider connectionsProvider
     */
    public function testConcurrencyWithConnection(string $connectionClass): void
    {
        $this->connectionClass = $connectionClass;

        if ($this->connectionClass !== AMQPSwooleConnection::class) {
            $this->markTestSkipped('Concurrency test can only be run with Swoole');
        }

        parent::testConcurrency();
    }

    /**
     * @dataProvider connectionsProvider
     */
    public function testRetryWithConnection(string $connectionClass): void
    {
        $this->connectionClass = $connectionClass;
        // The "depends" annotation will not work with a dataprovider.
        // We need to manually call the dependency.
        parent::testEvents();
        parent::testRetry();
    }
}
