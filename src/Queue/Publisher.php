<?php

namespace Utopia\Queue;

interface Publisher
{
    /**
     * Checks if the publisher can reach the queue.
     *
     * @return bool
     */
    public function ping(): bool;

    /**
     * Publishes a new message onto the queue.
     *
     * @param Queue $queue
     * @param array $payload
     * @return bool
     */
    public function enqueue(Queue $queue, array $payload): bool;

    /**
     * Retries failed jobs.
     *
     * @param Queue $queue
     * @param int|null $limit
     * @return void
     */
    public function retry(Queue $queue, int $limit = null): void;

    /**
     * Returns the amount of pending messages in the queue.
     *
     * @param Queue $queue
     * @param bool $failedJobs
     * @return int
     */
    public function getQueueSize(Queue $queue, bool $failedJobs = false): int;
}
