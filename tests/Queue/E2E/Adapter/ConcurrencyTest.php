<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Concurrency\Coroutine;
use Utopia\Queue\Concurrency\Inline;

class ConcurrencyTest extends TestCase
{
    /**
     * Inline runs each task immediately, in submit order, on the calling stack.
     */
    public function testInlineRunsTasksImmediatelyInOrder(): void
    {
        $order = [];
        $executor = new Inline();

        $executor->submit(function () use (&$order) {
            $order[] = 1;
        });
        $executor->submit(function () use (&$order) {
            $order[] = 2;
        });
        $executor->drain();

        $this->assertSame([1, 2], $order);
    }

    /**
     * The coroutine executor runs every submitted task, but never lets more
     * than $maxCoroutines of them run at the same time.
     */
    public function testCoroutineBoundsConcurrencyToLimit(): void
    {
        $active = 0;
        $maxActive = 0;
        $completed = 0;

        \Swoole\Coroutine\run(function () use (&$active, &$maxActive, &$completed) {
            $executor = new Coroutine(maxCoroutines: 3);

            for ($i = 0; $i < 9; $i++) {
                $executor->submit(function () use (&$active, &$maxActive, &$completed) {
                    $active++;
                    $maxActive = \max($maxActive, $active);
                    \Swoole\Coroutine::sleep(0.02);
                    $active--;
                    $completed++;
                });
            }

            $executor->drain();
        });

        $this->assertSame(9, $completed, 'every task should run');
        $this->assertSame(3, $maxActive, 'no more than maxCoroutines run concurrently');
    }

    /**
     * drain() must block until in-flight tasks have finished, so a graceful
     * shutdown does not abandon work.
     */
    public function testCoroutineDrainWaitsForInFlightTasks(): void
    {
        $finished = false;

        \Swoole\Coroutine\run(function () use (&$finished) {
            $executor = new Coroutine(maxCoroutines: 2);

            $executor->submit(function () use (&$finished) {
                \Swoole\Coroutine::sleep(0.05);
                $finished = true;
            });

            $executor->drain();
        });

        $this->assertTrue($finished, 'drain() should not return before submitted work completes');
    }
}
