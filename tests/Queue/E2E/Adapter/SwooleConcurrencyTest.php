<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Adapter\Swoole;
use Utopia\Queue\Broker\Redis;
use Utopia\Queue\Queue;

class SwooleConcurrencyTest extends TestCase
{
    private const string QUEUE = 'concurrency';
    private const string NAMESPACE = 'tests';

    public function testProcessesUpToMaxCoroutinesAtOnce(): void
    {
        [$processed, $maxActive] = $this->runWorker(messages: 9, maxCoroutines: 3);

        $this->assertSame(9, $processed);
        $this->assertSame(3, $maxActive, 'concurrency is bounded by maxCoroutines');
    }

    public function testOneCoroutineNeverOverlaps(): void
    {
        [$processed, $maxActive] = $this->runWorker(messages: 5, maxCoroutines: 1);

        $this->assertSame(5, $processed);
        $this->assertSame(1, $maxActive);
    }

    /**
     * Run the consume loop until $messages are processed; return the count and
     * the peak concurrency observed.
     *
     * @return array{0: int, 1: int} [processed, maxActive]
     */
    private function runWorker(int $messages, int $maxCoroutines): array
    {
        $connection = new InMemoryConnection();
        $broker = new Redis($connection, $connection);
        $queue = new Queue(self::QUEUE, self::NAMESPACE);

        $active = 0;
        $maxActive = 0;
        $processed = 0;

        \Swoole\Coroutine\run(function () use ($broker, $queue, $messages, $maxCoroutines, &$active, &$maxActive, &$processed) {
            for ($i = 0; $i < $messages; $i++) {
                $broker->enqueue($queue, ['n' => $i]);
            }

            $adapter = new Swoole($broker, 1, self::QUEUE, self::NAMESPACE, maxCoroutines: $maxCoroutines);

            $adapter->consume(
                function () use ($adapter, $messages, &$active, &$maxActive, &$processed) {
                    $active++;
                    $maxActive = max($maxActive, $active);
                    \Swoole\Coroutine::sleep(0.02);
                    $active--;

                    if (++$processed === $messages) {
                        $adapter->stop();
                    }
                },
                fn() => null,
                fn() => null,
            );
        });

        return [$processed, $maxActive];
    }
}
