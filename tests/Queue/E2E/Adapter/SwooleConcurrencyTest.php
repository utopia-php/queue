<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Concurrency\Coroutine;
use Utopia\Queue\Concurrency\Inline;
use Utopia\Queue\Broker\Redis;
use Utopia\Queue\Queue;

class SwooleConcurrencyTest extends TestCase
{
    private const string QUEUE = 'concurrency';
    private const string NAMESPACE = 'tests';

    /**
     * With a concurrent executor the broker receives messages on its receive
     * connection and processes them on overlapping coroutines, bounded by the
     * executor's limit — one receiver fanning out to many handlers.
     */
    public function testRedisFansProcessingOutAcrossCoroutines(): void
    {
        $connection = new InMemoryConnection();
        $queue = new Queue(self::QUEUE, self::NAMESPACE);
        $this->enqueue($connection, $queue, 9);

        $active = 0;
        $maxActive = 0;
        $processed = 0;

        \Swoole\Coroutine\run(function () use ($connection, $queue, &$active, &$maxActive, &$processed) {
            $broker = new Redis(
                receive: $connection,
                work: $connection,
                executor: new Coroutine(maxCoroutines: 3),
            );

            $broker->consume(
                $queue,
                function () use ($broker, &$active, &$maxActive, &$processed) {
                    $active++;
                    $maxActive = \max($maxActive, $active);
                    \Swoole\Coroutine::sleep(0.02);
                    $active--;

                    if (++$processed === 9) {
                        $broker->close();
                    }
                },
                fn () => null,
                fn () => null,
            );
        });

        $this->assertSame(9, $processed, 'every enqueued message should be processed');
        $this->assertSame(3, $maxActive, 'concurrency is bounded by the executor limit');
    }

    /**
     * The default (Inline) executor processes one message fully before the next
     * is received, so handlers never overlap.
     */
    public function testInlineProcessesOneMessageAtATime(): void
    {
        $connection = new InMemoryConnection();
        $queue = new Queue(self::QUEUE, self::NAMESPACE);
        $this->enqueue($connection, $queue, 5);

        $active = 0;
        $maxActive = 0;
        $processed = 0;

        \Swoole\Coroutine\run(function () use ($connection, $queue, &$active, &$maxActive, &$processed) {
            $broker = new Redis(receive: $connection, executor: new Inline());

            $broker->consume(
                $queue,
                function () use ($broker, &$active, &$maxActive, &$processed) {
                    $active++;
                    $maxActive = \max($maxActive, $active);
                    $active--;

                    if (++$processed === 5) {
                        $broker->close();
                    }
                },
                fn () => null,
                fn () => null,
            );
        });

        $this->assertSame(5, $processed);
        $this->assertSame(1, $maxActive, 'inline processing never overlaps');
    }

    private function enqueue(InMemoryConnection $connection, Queue $queue, int $count): void
    {
        for ($i = 0; $i < $count; $i++) {
            $connection->leftPushArray("{$queue->namespace}.queue.{$queue->name}", [
                'pid' => "pid-{$i}",
                'queue' => $queue->name,
                'timestamp' => 0,
                'payload' => ['n' => $i],
            ]);
        }
    }
}
