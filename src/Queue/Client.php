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
        $payload = [
            'pid' => \uniqid(more_entropy: true),
            'queue' => $this->queue,
            'timestamp' => time(),
            'payload' => $payload
        ];

        return $this->connection->leftPushArray("{$this->namespace}.queue.{$this->queue}", $payload);
    }

    public function getJob(string $pid): Message|false
    {
        $job = $this->connection->get("{$this->namespace}.jobs.{$this->queue}.{$pid}");

        if ($job === false) {
            return false;
        }

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

    public function sumTotalJobs(): int
    {
        return \intval($this->connection->get("{$this->namespace}.stats.{$this->queue}.total") ?? 0);
    }

    public function sumSuccessfulJobs(): int
    {
        return \intval($this->connection->get("{$this->namespace}.stats.{$this->queue}.success") ?? 0);
    }

    public function sumFailedJobs(): int
    {
        return \intval($this->connection->get("{$this->namespace}.stats.{$this->queue}.failed") ?? 0);
    }

    public function sumProcessingJobs(): int
    {
        return \intval($this->connection->get("{$this->namespace}.stats.{$this->queue}.processing") ?? 0);
    }

    public function resetStats(): void
    {
        $this->connection->set("{$this->namespace}.stats.{$this->queue}.total", 0);
        $this->connection->set("{$this->namespace}.stats.{$this->queue}.success", 0);
        $this->connection->set("{$this->namespace}.stats.{$this->queue}.failed", 0);
        $this->connection->set("{$this->namespace}.stats.{$this->queue}.processing", 0);
    }
}
