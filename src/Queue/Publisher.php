<?php

declare(strict_types=1);

namespace Utopia\Queue;

interface Publisher
{
    /**
     * Publishes a new message onto the queue.
     */
    public function enqueue(Queue $queue, array $payload, bool $priority = false): bool;

    /**
     * Retries failed jobs.
     */
    public function retry(Queue $queue, ?int $limit = null): void;

    /**
     * Returns the amount of pending messages in the queue.
     */
    public function getQueueSize(Queue $queue, bool $failedJobs = false): int;
}
