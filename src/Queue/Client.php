<?php

namespace Utopia\Queue;

class Client
{
    protected string $queue;
    protected string $namespace;
    protected Connection $connection;
    public function __construct(string $queue, Connection $connection, string $namespace = 'utopia-queue')
    {
        $this->queue = $queue;
        $this->namespace = $namespace;
        $this->connection = $connection;
    }

    public function enqueue(array $payload): bool
    {
        if (!isset($payload['concurrencyKey'])) {
            $payload['concurrencyKey'] = \uniqid();
        }

        $payload = [
            'pid' => \uniqid(more_entropy: true),
            'queue' => $this->queue,
            'timestamp' => time(),
            'payload' => $payload
        ];

        return $this->connection->leftPushArray("{$this->namespace}.queue.{$this->queue}", $payload);
    }

    /**
     * Take all jobs from the failed queue and re-enqueue them.
     * @param int|null $limit The amount of jobs to retry
     */
    public function retry(int $limit = null): void
    {
        $start = \time();
        $processed = 0;

        while (true) {
            $pid = $this->connection->rightPop("{$this->namespace}.failed.{$this->queue}", 5);

            // No more jobs to retry
            if ($pid === false) {
                break;
            }

            $job = $this->getJob($pid);

            // Job doesn't exist
            if ($job === false) {
                break;
            }

            // Job was already retried
            if ($job->getTimestamp() >= $start) {
                break;
            }

            // We're reached the max amount of jobs to retry
            if ($limit !== null && $processed >= $limit) {
                break;
            }

            $this->enqueue($job->getPayload());
            $processed++;
        }
    }

    public function getJob(string $pid): Message|false
    {
        $value = $this->connection->get("{$this->namespace}.jobs.{$this->queue}.{$pid}");

        if ($value === false) {
            return false;
        }

        $job = json_decode($value, true);

        return new Message($job);
    }

    public function listJobs(int $total = 50, int $offset = 0): array
    {
        return $this->connection->listRange("{$this->namespace}.queue.{$this->queue}", $total, $offset);
    }

    public function getQueueSize(): int
    {
        return $this->connection->listSize("{$this->namespace}.queue.{$this->queue}");
    }

    public function countTotalJobs(): int
    {
        return (int)($this->connection->get("{$this->namespace}.stats.{$this->queue}.total") ?? 0);
    }

    public function countSuccessfulJobs(): int
    {
        return (int)($this->connection->get("{$this->namespace}.stats.{$this->queue}.success") ?? 0);
    }

    public function countFailedJobs(): int
    {
        return (int)($this->connection->get("{$this->namespace}.stats.{$this->queue}.failed") ?? 0);
    }

    public function countProcessingJobs(): int
    {
        return (int)($this->connection->get("{$this->namespace}.stats.{$this->queue}.processing") ?? 0);
    }

    public function resetStats(): void
    {
        $this->connection->set("{$this->namespace}.stats.{$this->queue}.total", 0);
        $this->connection->set("{$this->namespace}.stats.{$this->queue}.success", 0);
        $this->connection->set("{$this->namespace}.stats.{$this->queue}.failed", 0);
        $this->connection->set("{$this->namespace}.stats.{$this->queue}.processing", 0);
    }
}
