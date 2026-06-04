<?php

namespace Utopia\Queue\Broker;

use Utopia\Queue\Concurrency\Executor;
use Utopia\Queue\Concurrency\Inline;
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
     * Dedicated to the blocking receive loop, which is driven by a single
     * coroutine. Because it has exactly one user it never needs locking.
     */
    private readonly Connection $receive;

    /**
     * Carries every other command — the per-message bookkeeping, acknowledging
     * and publishing. When messages are processed concurrently (see the
     * {@see Executor}), several coroutines share this connection, so it should
     * be wrapped in {@see \Utopia\Queue\Connection\Locking}.
     */
    private readonly Connection $work;

    private readonly Executor $executor;

    private bool $closed = false;
    /**
     * @var (callable(Queue, \Throwable, int, int): void)|null
     */
    private $reconnectCallback = null;
    /**
     * @var (callable(Queue, int): void)|null
     */
    private $reconnectSuccessCallback = null;

    /**
     * @param Connection      $receive  Connection used for the blocking receive loop.
     * @param Connection|null $work     Connection used for every other command; defaults
     *                                  to $receive, which is safe while processing inline.
     *                                  Pass a separate, locked connection when using a
     *                                  concurrent executor.
     * @param Executor        $executor Strategy for processing each received message.
     */
    public function __construct(
        Connection $receive,
        ?Connection $work = null,
        Executor $executor = new Inline(),
    ) {
        $this->receive = $receive;
        $this->work = $work ?? $receive;
        $this->executor = $executor;
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

    public function consume(Queue $queue, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $reconnectBackoffMs = self::RECONNECT_BACKOFF_MS;
        $reconnectAttempt = 0;

        while (!$this->closed) {
            /**
             * Waiting for next Job. The receive loop runs in a single coroutine,
             * so the blocking pop has exclusive use of its connection.
             */
            try {
                $nextMessage = $this->receive->rightPopArray("{$queue->namespace}.queue.{$queue->name}", self::POP_TIMEOUT);
                if ($reconnectAttempt > 0) {
                    $this->triggerReconnectSuccessCallback($queue, $reconnectAttempt);
                }

                $reconnectBackoffMs = self::RECONNECT_BACKOFF_MS;
                $reconnectAttempt = 0;
            } catch (\RedisException|\RedisClusterException $e) {
                if ($this->closed) {
                    break;
                }

                $reconnectAttempt++;

                try {
                    $this->receive->close();
                } catch (\Throwable) {
                }

                $sleepMs = \mt_rand(0, $reconnectBackoffMs);
                $this->triggerReconnectCallback($queue, $e, $reconnectAttempt, $sleepMs);

                \usleep($sleepMs * 1000);
                $reconnectBackoffMs = \min(self::RECONNECT_MAX_BACKOFF_MS, $reconnectBackoffMs * 2);

                continue;
            }

            if (!$nextMessage) {
                continue;
            }

            $nextMessage['timestamp'] = (int)$nextMessage['timestamp'];

            $message = new Message($nextMessage);

            /**
             * Hand the message off to the executor. Inline runs it here and now;
             * a concurrent executor fans it out onto a coroutine while the loop
             * goes back to receiving (bounded by the executor's backpressure).
             */
            $this->executor->submit(function () use ($queue, $message, $nextMessage, $messageCallback, $successCallback, $errorCallback) {
                $this->process($queue, $message, $nextMessage, $messageCallback, $successCallback, $errorCallback);
            });
        }

        /**
         * Wait for in-flight messages to finish before returning, so a graceful
         * shutdown does not abandon work that was already received.
         */
        $this->executor->drain();
    }

    /**
     * Process a single received message: record it as in-flight, run the
     * handler, then acknowledge or fail it. Every command here runs on the
     * $work connection so it is safe to invoke from many coroutines at once.
     *
     * @param array<string, mixed> $raw The decoded message payload.
     */
    private function process(
        Queue $queue,
        Message $message,
        array $raw,
        callable $messageCallback,
        callable $successCallback,
        callable $errorCallback,
    ): void {
        $pid = $message->getPid();

        /**
         * Move Job to Jobs and it's PID to the processing list.
         */
        $this->work->setArray("{$queue->namespace}.jobs.{$queue->name}.{$pid}", $raw, $queue->jobTtl);
        $this->work->leftPush("{$queue->namespace}.processing.{$queue->name}", $pid);

        /**
         * Increment Total Jobs Received from Stats.
         */
        $this->work->increment("{$queue->namespace}.stats.{$queue->name}.total");

        try {
            /**
             * Increment Processing Jobs from Stats.
             */
            $this->work->increment("{$queue->namespace}.stats.{$queue->name}.processing");

            $messageCallback($message);

            /**
             * Remove Jobs if successful.
             */
            $this->work->remove("{$queue->namespace}.jobs.{$queue->name}.{$pid}");

            /**
             * Increment Successful Jobs from Stats.
             */
            $this->work->increment("{$queue->namespace}.stats.{$queue->name}.success");

            $successCallback($message);
        } catch (\Throwable $th) {
            /**
             * Move failed Job to Failed list.
             */
            $this->work->leftPush("{$queue->namespace}.failed.{$queue->name}", $pid);

            /**
             * Increment Failed Jobs from Stats.
             */
            $this->work->increment("{$queue->namespace}.stats.{$queue->name}.failed");

            $errorCallback($message, $th);
        } finally {
            /**
             * Remove Job from Processing.
             */
            $this->work->listRemove("{$queue->namespace}.processing.{$queue->name}", $pid);

            /**
             * Decrease Processing Jobs from Stats.
             */
            $this->work->decrement("{$queue->namespace}.stats.{$queue->name}.processing");
        }
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
