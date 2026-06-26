<?php

declare(strict_types=1);

namespace Utopia\Queue;

interface Consumer
{
    /** Block up to $timeout seconds for the next message and claim it, or null on timeout. */
    public function receive(Queue $queue, int $timeout): ?Message;

    /** Acknowledge a processed message. */
    public function commit(Queue $queue, Message $message): void;

    /** Mark a message as failed. */
    public function reject(Queue $queue, Message $message): void;

    /** Close the consumer and free resources. */
    public function close(): void;
}
