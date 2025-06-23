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
    protected function getPublisher(string $connectionClass): Publisher
    {
        if ($connectionClass === AMQPSwooleConnection::class && !extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is not available');
        }

        return new AMQP(host: 'amqp', port: 5672, user: 'amqp', password: 'amqp', connectionClass: $connectionClass);
    }

    protected function getQueue(): Queue
    {
        return new Queue('amqp');
    }

    /**
     * @dataProvider connectionsProvider
     */
    public function testEvents(string $connectionClass): void
    {
        $publisher = $this->getPublisher($connectionClass);

        foreach ($this->payloads as $payload) {
            $this->assertTrue($publisher->enqueue($this->getQueue(), $payload));
        }

        sleep(1);
    }

    /**
     * @dataProvider connectionsProvider
     */
    public function testConcurrency(string $connectionClass): void
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension is not available');
        }

        run(function () use ($connectionClass) {
            $publisher = $this->getPublisher($connectionClass);
            go(function () use ($publisher) {
                foreach ($this->payloads as $payload) {
                    $this->assertTrue($publisher->enqueue($this->getQueue(), $payload));
                }

                sleep(1);
            });
        });
    }

    /**
     * @dataProvider connectionsProvider
     * @depends testEvents
     */
    public function testRetry(string $connectionClass): void
    {
        $publisher = $this->getPublisher($connectionClass);

        $published = $publisher->enqueue($this->getQueue(), [
            'type' => 'test_exception',
            'id' => 1
        ]);

        $this->assertTrue($published);

        $published = $publisher->enqueue($this->getQueue(), [
            'type' => 'test_exception',
            'id' => 2
        ]);

        $this->assertTrue($published);

        $published = $publisher->enqueue($this->getQueue(), [
            'type' => 'test_exception',
            'id' => 3
        ]);

        $this->assertTrue($published);

        $published = $publisher->enqueue($this->getQueue(), [
            'type' => 'test_exception',
            'id' => 4
        ]);

        $this->assertTrue($published);

        sleep(1);
        $publisher->retry($this->getQueue());
        sleep(1);
        $publisher->retry($this->getQueue(), 2);
        sleep(1);
    }
}
