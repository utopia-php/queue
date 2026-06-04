<?php

namespace Utopia\Queue;

interface Consumer
{
    /**
     * Block up to $timeout seconds for the next message and claim it. Returns
     * null on timeout so the caller can re-check its state.
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
     * Close the consumer and free any underlying resources.
     */
    public function close(): void;
}
