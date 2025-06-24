<?php

namespace Tests\E2E\Adapter;

use Utopia\Queue\Broker\AMQPSwoole;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

use function Co\run;

class AMQPSwooleTest extends Base
{
    protected function getPublisher(): Publisher
    {
        return new AMQPSwoole(host: 'amqp', port: 5672, user: 'amqp', password: 'amqp');
    }

    protected function getQueue(): Queue
    {
        return new Queue('amqp-swoole');
    }

    /**
     * Override testEvents to run within Swoole coroutines
     */
    public function testEvents(): void
    {
        run(function () {
            $publisher = $this->getPublisher();
            go(function () use ($publisher) {
                foreach ($this->payloads as $payload) {
                    $this->assertTrue($publisher->enqueue($this->getQueue(), $payload));
                }

                sleep(1);
                /** @var \Utopia\Queue\Broker\AMQPSwoole $publisher */
                $publisher->close();
            });
        });
    }

    public function testConcurrency(): void
    {
        run(function () {
            $publisher = $this->getPublisher();
            go(function () use ($publisher) {
                foreach ($this->payloads as $payload) {
                    $this->assertTrue($publisher->enqueue($this->getQueue(), $payload));
                }

                sleep(1);
                /** @var \Utopia\Queue\Broker\AMQPSwoole $publisher */
                $publisher->close();
            });
        });
    }

    /**
     * Override testRetry to run within Swoole coroutines
     * @depends testEvents
     */
    public function testRetry(): void
    {
        run(function () {
            $publisher = $this->getPublisher();
            go(function () use ($publisher) {
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
                /** @var \Utopia\Queue\Broker\AMQPSwoole $publisher */
                $publisher->close();
            });
        });
    }
}
