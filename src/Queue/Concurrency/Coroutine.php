<?php

namespace Utopia\Queue\Concurrency;

use Swoole\Coroutine as SwooleCoroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\WaitGroup;

/**
 * Fans each unit of work out onto its own Swoole coroutine, with at most
 * $maxCoroutines running concurrently.
 *
 * A single receiver can submit() faster than handlers complete, so the
 * semaphore channel applies backpressure: submit() blocks once the pool is
 * full, which in turn stops the receiver from pulling the next message until
 * there is capacity to process it. This decouples the concurrency level from
 * the connection count — one receiver fans out to many coroutines.
 */
final class Coroutine implements Executor
{
    private readonly int $maxCoroutines;

    /**
     * Semaphore bounding the number of in-flight coroutines. Lazily created so
     * the channel is allocated inside the worker's coroutine runtime (after any
     * process fork), never in the parent process that constructs the executor.
     */
    private ?Channel $slots = null;

    private ?WaitGroup $waitGroup = null;

    public function __construct(int $maxCoroutines = 1)
    {
        $this->maxCoroutines = \max(1, $maxCoroutines);
    }

    public function submit(callable $task): void
    {
        $slots = $this->slots ??= new Channel($this->maxCoroutines);
        $waitGroup = $this->waitGroup ??= new WaitGroup();

        // Acquire a slot; blocks the caller once $maxCoroutines are in flight.
        $slots->push(true);
        $waitGroup->add();

        SwooleCoroutine::create(function () use ($task, $slots, $waitGroup) {
            try {
                $task();
            } finally {
                $waitGroup->done();
                $slots->pop();
            }
        });
    }

    public function drain(): void
    {
        $this->waitGroup?->wait();
    }
}
