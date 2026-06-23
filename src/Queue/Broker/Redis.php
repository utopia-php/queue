<?php

namespace Utopia\Queue\Broker;

use Utopia\Queue\Connection;
use Utopia\Queue\Consumer;
use Utopia\Queue\Message;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class Redis implements Publisher, Consumer
{
    private const int POP_TIMEOUT = 2;
    private const int RECONNECT_BACKOFF_MS = 100;
    private const int RECONNECT_MAX_BACKOFF_MS = 5_000;

    private bool $closed = false;
    private int $reconnectAttempt = 0;
    private int $reconnectBackoffMs = self::RECONNECT_BACKOFF_MS;
    /**
     * @var (callable(Queue, \Throwable, int, int): void)|null
     */
    private $reconnectCallback = null;
    /**
     * @var (callable(Queue, int): void)|null
     */
    private $reconnectSuccessCallback = null;

    public function __construct(
        // Blocking receive loop + claim writes (single caller).
        private readonly Connection $receive,
        // Acks and publishing; wrap in Locking when shared by coroutines.
        private readonly Connection $commands,
    ) {}

    public function setReconnectCallback(?callable $callback): self
    {
        $this->reconnectCallback = $callback;

        return $this;
    }

    public function setReconnectSuccessCallback(?callable $callback): self
    {
        $this->reconnectSuccessCallback = $callback;

        return $this;
    }

    public function receive(Queue $queue, int $timeout): ?Message
    {
        if ($this->isClosed()) {
            return null;
        }

        try {
            $nextMessage = $this->receive->rightPopArray("{$queue->namespace}.queue.{$queue->name}", $timeout);
            if ($this->reconnectAttempt > 0) {
                $this->triggerReconnectSuccessCallback($queue, $this->reconnectAttempt);
            }

            $this->reconnectBackoffMs = self::RECONNECT_BACKOFF_MS;
            $this->reconnectAttempt = 0;
        } catch (\RedisException|\RedisClusterException $e) {
            if ($this->isClosed()) {
                return null;
            }

            $this->reconnectAttempt++;

            try {
                $this->receive->close();
            } catch (\Throwable) {
            }

            $sleepMs = mt_rand(0, $this->reconnectBackoffMs);
            $this->triggerReconnectCallback($queue, $e, $this->reconnectAttempt, $sleepMs);

            usleep($sleepMs * 1000);
            $this->reconnectBackoffMs = min(self::RECONNECT_MAX_BACKOFF_MS, $this->reconnectBackoffMs * 2);

            return null;
        }

        if (!$nextMessage) {
            return null;
        }

        $nextMessage['timestamp'] = (int) $nextMessage['timestamp'];

        $message = new Message($nextMessage);
        $pid = $message->getPid();

        // Claim: store the job, mark it processing, bump received stats.
        $this->receive->setArray("{$queue->namespace}.jobs.{$queue->name}.{$pid}", $nextMessage, $queue->jobTtl);
        $this->receive->leftPush("{$queue->namespace}.processing.{$queue->name}", $pid);
        $this->receive->increment("{$queue->namespace}.stats.{$queue->name}.total");
        $this->receive->increment("{$queue->namespace}.stats.{$queue->name}.processing");

        return $message;
    }

    public function commit(Queue $queue, Message $message): void
    {
        $pid = $message->getPid();

        $this->commands->remove("{$queue->namespace}.jobs.{$queue->name}.{$pid}");
        $this->commands->increment("{$queue->namespace}.stats.{$queue->name}.success");
        $this->commands->listRemove("{$queue->namespace}.processing.{$queue->name}", $pid);
        $this->commands->decrement("{$queue->namespace}.stats.{$queue->name}.processing");
    }

    public function reject(Queue $queue, Message $message): void
    {
        $pid = $message->getPid();

        $this->commands->leftPush("{$queue->namespace}.failed.{$queue->name}", $pid);
        $this->commands->increment("{$queue->namespace}.stats.{$queue->name}.failed");
        $this->commands->listRemove("{$queue->namespace}.processing.{$queue->name}", $pid);
        $this->commands->decrement("{$queue->namespace}.stats.{$queue->name}.processing");
    }

    public function close(): void
    {
        $this->closed = true;
    }

    /** @phpstan-impure close() flips this from another coroutine mid-receive(). */
    private function isClosed(): bool
    {
        return $this->closed;
    }

    private function triggerReconnectCallback(Queue $queue, \Throwable $error, int $attempt, int $sleepMs): void
    {
        if (!\is_callable($this->reconnectCallback)) {
            return;
        }

        try {
            ($this->reconnectCallback)($queue, $error, $attempt, $sleepMs);
        } catch (\Throwable) {
        }
    }

    private function triggerReconnectSuccessCallback(Queue $queue, int $attempts): void
    {
        if (!\is_callable($this->reconnectSuccessCallback)) {
            return;
        }

        try {
            ($this->reconnectSuccessCallback)($queue, $attempts);
        } catch (\Throwable) {
        }
    }

    public function enqueue(Queue $queue, array $payload, bool $priority = false): bool
    {
        $payload = [
            'pid' => uniqid(more_entropy: true),
            'queue' => $queue->name,
            'timestamp' => time(),
            'payload' => $payload,
        ];
        if ($priority) {
            return $this->commands->rightPushArray("{$queue->namespace}.queue.{$queue->name}", $payload);
        }
        return $this->commands->leftPushArray("{$queue->namespace}.queue.{$queue->name}", $payload);
    }

    /**
     * Take all jobs from the failed queue and re-enqueue them.
     * @param int|null $limit The amount of jobs to retry
     */
    public function retry(Queue $queue, ?int $limit = null): void
    {
        $start = time();
        $processed = 0;

        while (true) {
            $pid = $this->commands->rightPop("{$queue->namespace}.failed.{$queue->name}", self::POP_TIMEOUT);

            // No more jobs to retry
            if ($pid === false) {
                break;
            }

            $job = $this->getJob($queue, $pid);

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

            $this->enqueue($queue, $job->getPayload());
            $processed++;
        }
    }

    private function getJob(Queue $queue, string $pid): Message|false
    {
        $value = $this->commands->get("{$queue->namespace}.jobs.{$queue->name}.{$pid}");

        // Missing/expired jobs return false or null depending on the driver.
        if (!\is_string($value)) {
            return false;
        }

        $job = json_decode($value, true);

        return \is_array($job) ? new Message($job) : false;
    }

    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        $queueName = "{$queue->namespace}.queue.{$queue->name}";
        if ($failedJobs) {
            $queueName = "{$queue->namespace}.failed.{$queue->name}";
        }
        return $this->commands->listSize($queueName);
    }
}
