<?php

namespace Utopia\Queue\Concurrency;

/**
 * Runs each unit of work synchronously, in the calling coroutine/process.
 *
 * This is the default strategy and preserves the original sequential behaviour:
 * a message is fully processed before the next one is received.
 */
final class Inline implements Executor
{
    public function submit(callable $task): void
    {
        $task();
    }

    public function drain(): void
    {
    }
}
