<?php

namespace Utopia\Queue;

interface Consumer
{
    /**
     * Block for up to $timeout seconds for the next message and claim it as
     * in-flight. Returns null when nothing arrived in that window (or the
     * consumer is closing), so the caller's loop can re-check its state.
     *
     * The loop that calls this is single-threaded; concurrency, if any, comes
     * from the adapter fanning {@see commit()}/{@see reject()} out across
     * coroutines.
     */
    public function receive(Queue $queue, int $timeout): ?Message;

    /**
     * Acknowledge a message as successfully processed.
     */
    public function commit(Queue $queue, Message $message): void;

    /**
     * Mark a message as failed.
     */
    public function reject(Queue $queue, Message $message): void;

    /**
     * Closes the consumer and frees any underlying resources.
     */
    public function close(): void;
}
