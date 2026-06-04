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

    /**
     * Dedicated to the blocking receive loop and its claim writes. A single
     * caller drives it, so it never needs locking.
     */
    private readonly Connection $receive;

    /**
     * Carries acknowledgements and publishing. When the adapter processes
     * messages concurrently, several coroutines share this connection, so it
     * should be wrapped in {@see \Utopia\Queue\Connection\Locking}. Defaults to
     * $receive, which is safe while processing inline.
     */
    private readonly Connection $work;

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

    /**
     * @param Connection      $receive Connection used for the blocking receive loop.
     * @param Connection|null $work    Connection used for acknowledgements and publishing;
     *                                 defaults to $receive. Pass a separate, locked
     *                                 connection when an adapter processes concurrently.
     */
    public function __construct(
        Connection $receive,
        ?Connection $work = null,
    ) {
        $this->receive = $receive;
        $this->work = $work ?? $receive;
    }

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
        if ($this->closed) {
            return null;
        }

        /**
         * Wait for the next Job. The receive loop is single-threaded, so the
         * blocking pop and the claim writes below have exclusive use of this
         * connection and need no locking.
         */
        try {
            $nextMessage = $this->receive->rightPopArray("{$queue->namespace}.queue.{$queue->name}", $timeout);
            if ($this->reconnectAttempt > 0) {
                $this->triggerReconnectSuccessCallback($queue, $this->reconnectAttempt);
            }

            $this->reconnectBackoffMs = self::RECONNECT_BACKOFF_MS;
            $this->reconnectAttempt = 0;
        } catch (\RedisException|\RedisClusterException $e) {
            if ($this->closed) {
                return null;
            }

            $this->reconnectAttempt++;

            try {
                $this->receive->close();
            } catch (\Throwable) {
            }

            $sleepMs = \mt_rand(0, $this->reconnectBackoffMs);
            $this->triggerReconnectCallback($queue, $e, $this->reconnectAttempt, $sleepMs);

            \usleep($sleepMs * 1000);
            $this->reconnectBackoffMs = \min(self::RECONNECT_MAX_BACKOFF_MS, $this->reconnectBackoffMs * 2);

            return null;
        }

        if (!$nextMessage) {
            return null;
        }

        $nextMessage['timestamp'] = (int)$nextMessage['timestamp'];

        $message = new Message($nextMessage);
        $pid = $message->getPid();

        /**
         * Claim the Job: record it under Jobs, add its PID to the processing
         * list and bump the received/processing stats.
         */
        $this->receive->setArray("{$queue->namespace}.jobs.{$queue->name}.{$pid}", $nextMessage, $queue->jobTtl);
        $this->receive->leftPush("{$queue->namespace}.processing.{$queue->name}", $pid);
        $this->receive->increment("{$queue->namespace}.stats.{$queue->name}.total");
        $this->receive->increment("{$queue->namespace}.stats.{$queue->name}.processing");

        return $message;
    }

    public function commit(Queue $queue, Message $message): void
    {
        $pid = $message->getPid();

        /**
         * Remove the Job, bump the success stat, then clear it from processing.
         */
        $this->work->remove("{$queue->namespace}.jobs.{$queue->name}.{$pid}");
        $this->work->increment("{$queue->namespace}.stats.{$queue->name}.success");
        $this->work->listRemove("{$queue->namespace}.processing.{$queue->name}", $pid);
        $this->work->decrement("{$queue->namespace}.stats.{$queue->name}.processing");
    }

    public function reject(Queue $queue, Message $message): void
    {
        $pid = $message->getPid();

        /**
         * Move the Job to the failed list, bump the failed stat, then clear it
         * from processing.
         */
        $this->work->leftPush("{$queue->namespace}.failed.{$queue->name}", $pid);
        $this->work->increment("{$queue->namespace}.stats.{$queue->name}.failed");
        $this->work->listRemove("{$queue->namespace}.processing.{$queue->name}", $pid);
        $this->work->decrement("{$queue->namespace}.stats.{$queue->name}.processing");
    }

    public function close(): void
    {
        $this->closed = true;
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
            'pid' => \uniqid(more_entropy: true),
            'queue' => $queue->name,
            'timestamp' => time(),
            'payload' => $payload
        ];
        if ($priority) {
            return $this->work->rightPushArray("{$queue->namespace}.queue.{$queue->name}", $payload);
        }
        return $this->work->leftPushArray("{$queue->namespace}.queue.{$queue->name}", $payload);
    }

    /**
     * Take all jobs from the failed queue and re-enqueue them.
     * @param int|null $limit The amount of jobs to retry
     */
    public function retry(Queue $queue, ?int $limit = null): void
    {
        $start = \time();
        $processed = 0;

        while (true) {
            $pid = $this->work->rightPop("{$queue->namespace}.failed.{$queue->name}", self::POP_TIMEOUT);

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
        $value = $this->work->get("{$queue->namespace}.jobs.{$queue->name}.{$pid}");

        if ($value === false) {
            return false;
        }

        $job = json_decode($value, true);
        return new Message($job);
    }

    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        $queueName = "{$queue->namespace}.queue.{$queue->name}";
        if ($failedJobs) {
            $queueName = "{$queue->namespace}.failed.{$queue->name}";
        }
        return $this->work->listSize($queueName);
    }
}
