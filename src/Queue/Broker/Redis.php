<?php

namespace Utopia\Queue\Broker;

use Utopia\Queue\Connection;
use Utopia\Queue\Consumer;
use Utopia\Queue\Message;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class Redis implements Publisher, Consumer
{
    private bool $closed = false;

    public function __construct(private readonly Connection $connection)
    {
    }

    public function consume(Queue $queue, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        while (!$this->closed) {
            /**
             * Waiting for next Job.
             */
            $nextMessage = $this->connection->rightPopArray("{$queue->namespace}.queue.{$queue->name}", 5);

            if (!$nextMessage) {
                continue;
            }

            $nextMessage['timestamp'] = (int)$nextMessage['timestamp'];

            $message = new Message($nextMessage);

            /**
             * Move Job to Jobs and it's PID to the processing list.
             */
            $this->connection->setArray("{$queue->namespace}.jobs.{$queue->name}.{$message->getPid()}", $nextMessage);
            $this->connection->leftPush("{$queue->namespace}.processing.{$queue->name}", $message->getPid());

            /**
             * Increment Total Jobs Received from Stats.
             */
            $this->connection->increment("{$queue->namespace}.stats.{$queue->name}.total");

            try {
                /**
                 * Increment Processing Jobs from Stats.
                 */
                $this->connection->increment("{$queue->namespace}.stats.{$queue->name}.processing");

                $messageCallback($message);

                /**
                 * Remove Jobs if successful.
                 */
                $this->connection->remove("{$queue->namespace}.jobs.{$queue->name}.{$message->getPid()}");

                /**
                 * Increment Successful Jobs from Stats.
                 */
                $this->connection->increment("{$queue->namespace}.stats.{$queue->name}.success");


                $successCallback($message);
            } catch (\Throwable $th) {
                /**
                 * Move failed Job to Failed list.
                 */
                $this->connection->leftPush("{$queue->namespace}.failed.{$queue->name}", $message->getPid());

                /**
                 * Increment Failed Jobs from Stats.
                 */
                $this->connection->increment("{$queue->namespace}.stats.{$queue->name}.failed");

                $errorCallback($message, $th);
            } finally {
                /**
                 * Remove Job from Processing.
                 */
                $this->connection->listRemove("{$queue->namespace}.processing.{$queue->name}", $message->getPid());

                /**
                 * Decrease Processing Jobs from Stats.
                 */
                $this->connection->decrement("{$queue->namespace}.stats.{$queue->name}.processing");
            }
        }
    }

    public function close(): void
    {
        $this->closed = true;
    }

    public function ping(): bool
    {
        return $this->connection->ping();
    }

    public function enqueue(Queue $queue, array $payload): bool
    {
        $payload = [
            'pid' => \uniqid(more_entropy: true),
            'queue' => $queue->name,
            'timestamp' => time(),
            'payload' => $payload
        ];
        return $this->connection->leftPushArray("{$queue->namespace}.queue.{$queue->name}", $payload);
    }

    /**
     * Take all jobs from the failed queue and re-enqueue them.
     * @param int|null $limit The amount of jobs to retry
     */
    public function retry(Queue $queue, int $limit = null): void
    {
        $start = \time();
        $processed = 0;

        while (true) {
            $pid = $this->connection->rightPop("{$queue->namespace}.failed.{$queue->name}", 5);

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
        $value = $this->connection->get("{$queue->namespace}.jobs.{$queue->name}.{$pid}");

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
        return $this->connection->listSize($queueName);
    }
}
