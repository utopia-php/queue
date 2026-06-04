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

    /**
     * The Swoole adapter owns concurrency: it receives on a single loop and
     * fans message processing out across coroutines, with at most
     * maxCoroutines handlers running at once.
     */
    public function testAdapterFansProcessingOutUpToMaxCoroutines(): void
    {
        [$processed, $maxActive] = $this->runWorker(messages: 9, maxCoroutines: 3);

        $this->assertSame(9, $processed, 'every enqueued message should be processed');
        $this->assertSame(3, $maxActive, 'concurrency is bounded by maxCoroutines');
    }

    /**
     * With maxCoroutines of 1 the adapter degrades to sequential processing —
     * handlers never overlap.
     */
    public function testMaxCoroutinesOfOneProcessesSequentially(): void
    {
        [$processed, $maxActive] = $this->runWorker(messages: 5, maxCoroutines: 1);

        $this->assertSame(5, $processed);
        $this->assertSame(1, $maxActive, 'a single coroutine never overlaps');
    }

    /**
     * Enqueue $messages jobs, run the adapter's consume loop until they are all
     * processed, and report how many ran and the peak concurrency observed.
     *
     * @return array{0: int, 1: int} [processed, maxActive]
     */
    private function runWorker(int $messages, int $maxCoroutines): array
    {
        $connection = new InMemoryConnection();
        $broker = new Redis($connection);
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
                    $maxActive = \max($maxActive, $active);
                    \Swoole\Coroutine::sleep(0.02);
                    $active--;

                    if (++$processed === $messages) {
                        $adapter->stop();
                    }
                },
                fn () => null,
                fn () => null,
            );
        });

        return [$processed, $maxActive];
    }
}
