<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Adapter\Swoole;
use Utopia\Queue\Consumer;
use Utopia\Queue\Error\ConsumerFailures;
use Utopia\Queue\Queue;

class SwooleConcurrencyTest extends TestCase
{
    public function testMaxCoroutinesConsumeInParallel(): void
    {
        $consumer = new ConcurrentConsumer();

        \Swoole\Coroutine\run(function () use ($consumer) {
            $adapter = new Swoole($consumer, 1, 'swoole-concurrency', maxCoroutines: 3);
            $adapter->consume(fn () => null, fn () => null, fn () => null);
        });

        $this->assertSame(3, $consumer->consumeCalls);
        $this->assertSame(3, $consumer->maxActive);
    }

    public function testPreservesAllCoroutineConsumerErrors(): void
    {
        $consumer = new FailingConcurrentConsumer();
        $failure = null;

        \Swoole\Coroutine\run(function () use ($consumer, &$failure) {
            $adapter = new Swoole($consumer, 1, 'swoole-concurrency', maxCoroutines: 3);

            try {
                $adapter->consume(fn () => null, fn () => null, fn () => null);
            } catch (ConsumerFailures $error) {
                $failure = $error;
            }
        });

        $this->assertInstanceOf(ConsumerFailures::class, $failure);
        $this->assertSame(3, $consumer->consumeCalls);
        $this->assertSame(3, $consumer->closed);
        $this->assertCount(3, $failure->getErrors());
        $messages = \array_map(fn (\Throwable $error) => $error->getMessage(), $failure->getErrors());
        \sort($messages);

        $this->assertSame(['consumer 1 failed', 'consumer 2 failed', 'consumer 3 failed'], $messages);
        $this->assertSame($failure->getErrors()[0], $failure->getPrevious());
    }
}

final class ConcurrentConsumer implements Consumer
{
    public int $active = 0;
    public int $consumeCalls = 0;
    public int $maxActive = 0;

    public function consume(Queue $queue, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $this->consumeCalls++;
        $this->active++;
        $this->maxActive = \max($this->maxActive, $this->active);

        \Swoole\Coroutine::sleep(0.05);

        $this->active--;
    }

    public function close(): void
    {
    }
}

final class FailingConcurrentConsumer implements Consumer
{
    public int $closed = 0;
    public int $consumeCalls = 0;

    public function consume(Queue $queue, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $this->consumeCalls++;
        $id = $this->consumeCalls;

        \Swoole\Coroutine::sleep(0.05);

        throw new \RuntimeException("consumer {$id} failed");
    }

    public function close(): void
    {
        $this->closed++;
    }
}
