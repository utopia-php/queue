<?php

namespace Utopia\Queue\Concurrency;

/**
 * Strategy for running the work a broker schedules per received message.
 *
 * A broker receives messages sequentially on a single connection and hands the
 * processing of each one to an {@see Executor}. The executor decides whether
 * that work runs inline (synchronously, one message at a time) or fans out onto
 * a bounded pool of coroutines, so the concurrency model is decoupled from both
 * the broker and the number of connections it holds.
 */
interface Executor
{
    /**
     * Run a unit of work. Implementations may execute it inline in the calling
     * coroutine or schedule it on a coroutine from a bounded pool.
     *
     * @param callable(): void $task
     */
    public function submit(callable $task): void;

    /**
     * Block until all work submitted so far has finished. Called during a
     * graceful shutdown so in-flight messages are processed rather than dropped.
     */
    public function drain(): void;
}
