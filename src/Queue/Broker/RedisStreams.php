<?php

namespace Utopia\Queue\Broker;

use Utopia\Queue\Consumer;
use Utopia\Queue\Message;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;
use Utopia\Queue\Schedule;
use Utopia\Queue\StreamConnection;

class RedisStreams implements Publisher, Consumer
{
    private const int BLOCK_TIMEOUT_MS = 2000;
    private const int DEFAULT_CLAIM_IDLE_TIME_MS = 30000;
    private const int DEFAULT_MAX_RETRIES = 3;
    private const int DELAYED_CHECK_INTERVAL_MS = 1000;
    private const int SCHEDULE_CHECK_INTERVAL_MS = 1000;

    private bool $closed = false;
    private string $consumerId;
    private int $lastDelayedCheck = 0;
    private int $lastScheduleCheck = 0;

    /**
     * @param StreamConnection $connection Redis stream connection
     * @param int|null $maxStreamLength Max stream length for trimming (null = no trim)
     * @param int $maxRetries Max retries before moving to DLQ
     * @param int $claimIdleTimeMs Min idle time before claiming abandoned messages
     */
    public function __construct(
        private readonly StreamConnection $connection,
        private readonly ?int $maxStreamLength = 10000,
        private readonly int $maxRetries = self::DEFAULT_MAX_RETRIES,
        private readonly int $claimIdleTimeMs = self::DEFAULT_CLAIM_IDLE_TIME_MS,
    ) {
        if ($maxStreamLength !== null && $maxStreamLength < 1) {
            throw new \InvalidArgumentException('Max stream length must be positive or null');
        }
        if ($maxRetries < 0) {
            throw new \InvalidArgumentException('Max retries must be non-negative');
        }
        if ($claimIdleTimeMs < 0) {
            throw new \InvalidArgumentException('Claim idle time must be non-negative');
        }

        $this->consumerId = 'worker-' . \uniqid();
    }

    /**
     * Set the consumer ID for this broker instance.
     *
     * @param string $consumerId
     * @return void
     */
    public function setConsumerId(string $consumerId): void
    {
        $this->consumerId = $consumerId;
    }

    /**
     * Get the consumer ID for this broker instance.
     *
     * @return string
     */
    public function getConsumerId(): string
    {
        return $this->consumerId;
    }


    /**
     * @inheritDoc
     */
    public function enqueue(Queue $queue, array $payload): bool
    {
        $streamKey = $this->getStreamKey($queue);
        $groupName = $this->getGroupName($queue);

        // Ensure consumer group exists
        $this->ensureConsumerGroup($streamKey, $groupName);

        $messageData = [
            'pid' => \uniqid(more_entropy: true),
            'queue' => $queue->name,
            'timestamp' => \time(),
            'payload' => $payload,
        ];

        $encodedData = \json_encode($messageData);
        if ($encodedData === false) {
            throw new \RuntimeException('Failed to encode message data: ' . \json_last_error_msg());
        }

        $fields = [
            'data' => $encodedData,
            'retry_count' => '0',
        ];

        $result = $this->connection->streamAdd($streamKey, $fields, '*', $this->maxStreamLength);

        return $result !== false;
    }

    /**
     * Enqueue a job to be processed after a delay.
     *
     * @param Queue $queue
     * @param array $payload
     * @param int $delaySeconds Seconds to delay before processing
     * @return bool
     */
    public function enqueueDelayed(Queue $queue, array $payload, int $delaySeconds): bool
    {
        if ($delaySeconds < 0) {
            throw new \InvalidArgumentException('Delay seconds must be non-negative');
        }

        $delayedKey = $this->getDelayedKey($queue);

        $messageData = [
            'pid' => \uniqid(more_entropy: true),
            'queue' => $queue->name,
            'timestamp' => \time(),
            'payload' => $payload,
        ];

        $encodedData = \json_encode($messageData);
        if ($encodedData === false) {
            throw new \RuntimeException('Failed to encode message data: ' . \json_last_error_msg());
        }

        $fields = [
            'data' => $encodedData,
            'retry_count' => '0',
        ];

        // Score is the timestamp when the job should be processed (in milliseconds)
        $executeAt = (int)(\microtime(true) * 1000) + ($delaySeconds * 1000);

        $encodedFields = \json_encode($fields);
        if ($encodedFields === false) {
            throw new \RuntimeException('Failed to encode field data: ' . \json_last_error_msg());
        }

        $result = $this->connection->sortedSetAdd($delayedKey, $executeAt, $encodedFields);

        return $result >= 0;
    }

    /**
     * Enqueue a job to be processed at a specific time.
     *
     * @param Queue $queue
     * @param array $payload
     * @param int $timestamp Unix timestamp when the job should be processed
     * @return bool
     */
    public function enqueueAt(Queue $queue, array $payload, int $timestamp): bool
    {
        $delaySeconds = \max(0, $timestamp - \time());
        return $this->enqueueDelayed($queue, $payload, $delaySeconds);
    }

    /**
     * @inheritDoc
     */
    public function retry(Queue $queue, ?int $limit = null): void
    {
        $dlqKey = $this->getDlqKey($queue);
        $streamKey = $this->getStreamKey($queue);
        $groupName = $this->getGroupName($queue);

        // Ensure group exists
        $this->ensureConsumerGroup($streamKey, $groupName);

        // Read from DLQ stream
        $entries = $this->connection->streamRange($dlqKey, '-', '+', $limit ?? 100);

        $processed = 0;
        $idsToDelete = [];

        foreach ($entries as $entryId => $fields) {
            if ($limit !== null && $processed >= $limit) {
                break;
            }

            // Reset retry count and re-add to main stream
            $fields['retry_count'] = '0';
            unset($fields['error'], $fields['failed_at']);

            $this->connection->streamAdd($streamKey, $fields, '*', $this->maxStreamLength);
            $idsToDelete[] = $entryId;
            $processed++;
        }

        // Delete retried entries from DLQ
        if (!empty($idsToDelete)) {
            $this->connection->streamDel($dlqKey, $idsToDelete);
        }
    }

    /**
     * @inheritDoc
     */
    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        if ($failedJobs) {
            return $this->connection->streamLen($this->getDlqKey($queue));
        }

        $streamSize = $this->connection->streamLen($this->getStreamKey($queue));
        $delayedSize = $this->connection->sortedSetSize($this->getDelayedKey($queue));

        return $streamSize + $delayedSize;
    }


    /**
     * @inheritDoc
     */
    public function consume(Queue $queue, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $streamKey = $this->getStreamKey($queue);
        $groupName = $this->getGroupName($queue);
        $dlqKey = $this->getDlqKey($queue);
        $delayedKey = $this->getDelayedKey($queue);

        // Ensure consumer groups exist
        $this->ensureConsumerGroup($streamKey, $groupName);
        $this->ensureConsumerGroup($dlqKey, $groupName);

        while (!$this->closed) {
            try {
                // 1. Process due scheduled jobs
                $this->processScheduledJobs($queue);

                // 2. Process due delayed jobs
                $this->processDelayedJobs($queue, $delayedKey, $streamKey);

                // 3. Claim abandoned messages from crashed consumers
                $this->claimAbandonedMessages($streamKey, $groupName, $dlqKey, $queue, $messageCallback, $successCallback, $errorCallback);

                // 4. Read new messages from stream
                $entries = $this->connection->streamReadGroup(
                    $groupName,
                    $this->consumerId,
                    [$streamKey],
                    1,
                    self::BLOCK_TIMEOUT_MS
                );

                if ($entries === false || empty($entries)) {
                    continue;
                }

                foreach ($entries[$streamKey] ?? [] as $entryId => $fields) {
                    $this->processEntry($entryId, $fields, $streamKey, $groupName, $dlqKey, $queue, $messageCallback, $successCallback, $errorCallback);
                }
            } catch (\RedisException $e) {
                if ($this->closed) {
                    break;
                }
                throw $e;
            }
        }
    }

    /**
     * Consume from multiple queues simultaneously.
     *
     * @param Queue[] $queues Array of queues to consume from
     * @param callable $messageCallback Receives (Message $message, Queue $queue)
     * @param callable $successCallback Receives (Message $message, Queue $queue)
     * @param callable $errorCallback Receives (Message $message, Queue $queue, \Throwable $error)
     * @return void
     */
    public function consumeMultiple(array $queues, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $streamKeys = [];
        $queueMap = [];

        foreach ($queues as $queue) {
            $streamKey = $this->getStreamKey($queue);
            $groupName = $this->getGroupName($queue);

            // Ensure consumer groups exist
            $this->ensureConsumerGroup($streamKey, $groupName);
            $this->ensureConsumerGroup($this->getDlqKey($queue), $groupName);

            $streamKeys[] = $streamKey;
            $queueMap[$streamKey] = $queue;
        }

        while (!$this->closed) {
            try {
                // Process scheduled and delayed jobs for all queues
                foreach ($queues as $queue) {
                    $this->processScheduledJobs($queue);
                    $this->processDelayedJobs($queue, $this->getDelayedKey($queue), $this->getStreamKey($queue));

                    // Claim abandoned messages
                    $streamKey = $this->getStreamKey($queue);
                    $groupName = $this->getGroupName($queue);
                    $dlqKey = $this->getDlqKey($queue);
                    $this->claimAbandonedMessages($streamKey, $groupName, $dlqKey, $queue, $messageCallback, $successCallback, $errorCallback);
                }

                // Read from each queue individually with its own consumer group
                // Note: Redis XREADGROUP requires a single consumer group, so we can't
                // read from multiple streams with different groups in one call
                foreach ($queues as $queue) {
                    $streamKey = $this->getStreamKey($queue);
                    $groupName = $this->getGroupName($queue);
                    $dlqKey = $this->getDlqKey($queue);

                    $entries = $this->connection->streamReadGroup(
                        $groupName,
                        $this->consumerId,
                        [$streamKey],
                        1,
                        0  // Non-blocking to check all queues quickly
                    );

                    if ($entries === false || empty($entries)) {
                        continue;
                    }

                    foreach ($entries[$streamKey] ?? [] as $entryId => $fields) {
                        $this->processEntry($entryId, $fields, $streamKey, $groupName, $dlqKey, $queue, $messageCallback, $successCallback, $errorCallback);
                    }
                }

                // Brief sleep to prevent tight loop when all queues are empty
                \usleep(10000); // 10ms
            } catch (\RedisException $e) {
                if ($this->closed) {
                    break;
                }
                throw $e;
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function close(): void
    {
        $this->closed = true;
    }


    /**
     * Register a recurring schedule.
     *
     * @param Queue $queue
     * @param Schedule $schedule
     * @return bool
     */
    public function schedule(Queue $queue, Schedule $schedule): bool
    {
        $schedulesKey = $this->getSchedulesKey($queue);
        $nextKey = $this->getScheduleNextKey($queue);

        // Store schedule definition
        $encodedSchedule = \json_encode($schedule->toArray());
        if ($encodedSchedule === false) {
            throw new \RuntimeException('Failed to encode schedule data: ' . \json_last_error_msg());
        }

        $stored = $this->connection->hashSet($schedulesKey, $schedule->id, $encodedSchedule);

        if (!$stored) {
            return false;
        }

        // Calculate and store next run time
        $nextRun = $schedule->getNextRunTime();
        if ($nextRun !== null) {
            $this->connection->sortedSetAdd($nextKey, (float)($nextRun * 1000), $schedule->id);
        }

        return true;
    }

    /**
     * Remove a recurring schedule.
     *
     * @param Queue $queue
     * @param string $scheduleId
     * @return bool
     */
    public function unschedule(Queue $queue, string $scheduleId): bool
    {
        $schedulesKey = $this->getSchedulesKey($queue);
        $nextKey = $this->getScheduleNextKey($queue);

        $this->connection->hashDel($schedulesKey, $scheduleId);
        $this->connection->sortedSetRemove($nextKey, $scheduleId);

        return true;
    }

    /**
     * Get a schedule by ID.
     *
     * @param Queue $queue
     * @param string $scheduleId
     * @return Schedule|null
     */
    public function getSchedule(Queue $queue, string $scheduleId): ?Schedule
    {
        $schedulesKey = $this->getSchedulesKey($queue);
        $data = $this->connection->hashGet($schedulesKey, $scheduleId);

        if ($data === false) {
            return null;
        }

        $decodedData = \json_decode($data, true);
        if ($decodedData === null && \json_last_error() !== JSON_ERROR_NONE) {
            throw new \RuntimeException('Failed to decode schedule data: ' . \json_last_error_msg());
        }

        return Schedule::fromArray($decodedData);
    }

    /**
     * Get all schedules for a queue.
     *
     * @param Queue $queue
     * @return Schedule[]
     */
    public function getSchedules(Queue $queue): array
    {
        $schedulesKey = $this->getSchedulesKey($queue);
        $all = $this->connection->hashGetAll($schedulesKey);

        $schedules = [];
        foreach ($all as $id => $data) {
            $decodedData = \json_decode($data, true);
            if ($decodedData === null && \json_last_error() !== JSON_ERROR_NONE) {
                // Skip corrupted schedule data rather than failing completely
                continue;
            }
            $schedules[$id] = Schedule::fromArray($decodedData);
        }

        return $schedules;
    }

    /**
     * Pause a schedule.
     *
     * @param Queue $queue
     * @param string $scheduleId
     * @return bool
     */
    public function pauseSchedule(Queue $queue, string $scheduleId): bool
    {
        $schedule = $this->getSchedule($queue, $scheduleId);
        if ($schedule === null) {
            return false;
        }

        $paused = $schedule->pause();
        $schedulesKey = $this->getSchedulesKey($queue);
        $nextKey = $this->getScheduleNextKey($queue);

        $encodedSchedule = \json_encode($paused->toArray());
        if ($encodedSchedule === false) {
            throw new \RuntimeException('Failed to encode schedule data: ' . \json_last_error_msg());
        }

        // Update schedule and remove from next execution queue
        $this->connection->hashSet($schedulesKey, $scheduleId, $encodedSchedule);
        $this->connection->sortedSetRemove($nextKey, $scheduleId);

        return true;
    }

    /**
     * Resume a paused schedule.
     *
     * @param Queue $queue
     * @param string $scheduleId
     * @return bool
     */
    public function resumeSchedule(Queue $queue, string $scheduleId): bool
    {
        $schedule = $this->getSchedule($queue, $scheduleId);
        if ($schedule === null) {
            return false;
        }

        $resumed = $schedule->resume();
        $schedulesKey = $this->getSchedulesKey($queue);
        $nextKey = $this->getScheduleNextKey($queue);

        $encodedSchedule = \json_encode($resumed->toArray());
        if ($encodedSchedule === false) {
            throw new \RuntimeException('Failed to encode schedule data: ' . \json_last_error_msg());
        }

        // Update schedule and add next execution time
        $this->connection->hashSet($schedulesKey, $scheduleId, $encodedSchedule);

        $nextRun = $resumed->getNextRunTime();
        if ($nextRun !== null) {
            $this->connection->sortedSetAdd($nextKey, (float)($nextRun * 1000), $scheduleId);
        }

        return true;
    }


    /**
     * Get stream information.
     *
     * @param Queue $queue
     * @return array
     */
    public function getStreamInfo(Queue $queue): array
    {
        return $this->connection->streamInfo($this->getStreamKey($queue));
    }

    /**
     * Get consumer group information.
     *
     * @param Queue $queue
     * @return array
     */
    public function getGroupInfo(Queue $queue): array
    {
        $groups = $this->connection->streamGroupInfo($this->getStreamKey($queue));
        $groupName = $this->getGroupName($queue);

        foreach ($groups as $group) {
            if (($group['name'] ?? '') === $groupName) {
                return $group;
            }
        }

        return [];
    }

    /**
     * Get consumers information for the queue's consumer group.
     *
     * @param Queue $queue
     * @return array
     */
    public function getConsumersInfo(Queue $queue): array
    {
        return $this->connection->streamConsumersInfo(
            $this->getStreamKey($queue),
            $this->getGroupName($queue)
        );
    }

    /**
     * Get the consumer lag (messages waiting to be delivered).
     *
     * @param Queue $queue
     * @return int
     */
    public function getLag(Queue $queue): int
    {
        $groupInfo = $this->getGroupInfo($queue);
        return $groupInfo['lag'] ?? 0;
    }

    /**
     * Get count of delayed jobs.
     *
     * @param Queue $queue
     * @return int
     */
    public function getDelayedCount(Queue $queue): int
    {
        return $this->connection->sortedSetSize($this->getDelayedKey($queue));
    }

    /**
     * Get pending message count (messages being processed).
     *
     * @param Queue $queue
     * @return int
     */
    public function getPendingCount(Queue $queue): int
    {
        $pending = $this->connection->streamPendingSummary(
            $this->getStreamKey($queue),
            $this->getGroupName($queue)
        );

        return $pending[0] ?? 0;
    }

    /**
     * Get messages from stream (for replay/history).
     *
     * @param Queue $queue
     * @param string $start Start ID ('-' for minimum)
     * @param string $end End ID ('+' for maximum)
     * @param int|null $count Max messages
     * @return Message[]
     */
    public function getMessages(Queue $queue, string $start = '-', string $end = '+', ?int $count = null): array
    {
        $entries = $this->connection->streamRange($this->getStreamKey($queue), $start, $end, $count);

        $messages = [];
        foreach ($entries as $id => $fields) {
            $data = \json_decode($fields['data'] ?? '{}', true);
            if ($data === null && \json_last_error() !== JSON_ERROR_NONE) {
                // Skip corrupted message data
                continue;
            }
            $data['streamId'] = $id;
            $messages[] = new Message($data);
        }

        return $messages;
    }

    /**
     * Get a specific message by ID.
     *
     * @param Queue $queue
     * @param string $id Stream entry ID
     * @return Message|null
     */
    public function getMessage(Queue $queue, string $id): ?Message
    {
        $entries = $this->connection->streamRange($this->getStreamKey($queue), $id, $id, 1);

        if (empty($entries)) {
            return null;
        }

        $fields = \reset($entries);
        $data = \json_decode($fields['data'] ?? '{}', true);
        if ($data === null && \json_last_error() !== JSON_ERROR_NONE) {
            throw new \RuntimeException('Failed to decode message data: ' . \json_last_error_msg());
        }
        $data['streamId'] = $id;

        return new Message($data);
    }

    /**
     * Manually trim the stream.
     *
     * @param Queue $queue
     * @param int $maxLen Maximum length to keep
     * @return int Number of entries trimmed
     */
    public function trimStream(Queue $queue, int $maxLen): int
    {
        // Use exact trimming (not approximate) for manual trim operations
        return $this->connection->streamTrim($this->getStreamKey($queue), $maxLen, false);
    }

    /**
     * Delete a consumer from the consumer group.
     *
     * @param Queue $queue
     * @param string $consumerId
     * @return int Number of pending messages that were owned by the consumer
     */
    public function deleteConsumer(Queue $queue, string $consumerId): int
    {
        return $this->connection->streamDeleteConsumer(
            $this->getStreamKey($queue),
            $this->getGroupName($queue),
            $consumerId
        );
    }


    /**
     * Process a single stream entry.
     */
    private function processEntry(
        string $entryId,
        array $fields,
        string $streamKey,
        string $groupName,
        string $dlqKey,
        Queue $queue,
        callable $messageCallback,
        callable $successCallback,
        callable $errorCallback
    ): void {
        $messageData = \json_decode($fields['data'] ?? '{}', true);
        $messageData['timestamp'] = (int)($messageData['timestamp'] ?? \time());
        $messageData['streamId'] = $entryId;
        $retryCount = (int)($fields['retry_count'] ?? 0);

        $message = new Message($messageData);

        // Update stats
        $this->connection->increment("{$queue->namespace}.stats.{$queue->name}.total");
        $this->connection->increment("{$queue->namespace}.stats.{$queue->name}.processing");

        try {
            $messageCallback($message);

            // Acknowledge the message
            $this->connection->streamAck($streamKey, $groupName, $entryId);

            $this->connection->increment("{$queue->namespace}.stats.{$queue->name}.success");
            $successCallback($message);
        } catch (\Throwable $th) {
            // Acknowledge the failed message to remove from pending
            $this->connection->streamAck($streamKey, $groupName, $entryId);

            if ($retryCount < $this->maxRetries) {
                // Re-add to stream with incremented retry count
                $fields['retry_count'] = (string)($retryCount + 1);
                $this->connection->streamAdd($streamKey, $fields, '*', $this->maxStreamLength);
            } else {
                // Move to DLQ
                $fields['error'] = $th->getMessage();
                $fields['failed_at'] = (string)\time();
                $this->connection->streamAdd($dlqKey, $fields, '*', $this->maxStreamLength);
                $this->connection->increment("{$queue->namespace}.stats.{$queue->name}.failed");
            }

            $errorCallback($message, $th);
        } finally {
            $this->connection->decrement("{$queue->namespace}.stats.{$queue->name}.processing");
        }
    }

    /**
     * Ensure consumer group exists.
     */
    private function ensureConsumerGroup(string $streamKey, string $groupName): void
    {
        $this->connection->streamCreateGroup($streamKey, $groupName, '0', true);
    }

    /**
     * Claim abandoned messages from crashed consumers.
     */
    private function claimAbandonedMessages(
        string $streamKey,
        string $groupName,
        string $dlqKey,
        Queue $queue,
        callable $messageCallback,
        callable $successCallback,
        callable $errorCallback
    ): void {
        $result = $this->connection->streamAutoClaim(
            $streamKey,
            $groupName,
            $this->consumerId,
            $this->claimIdleTimeMs,
            '0-0',
            10
        );

        if (empty($result) || empty($result[1])) {
            return;
        }

        // Process claimed messages
        foreach ($result[1] as $entryId => $fields) {
            if (\is_array($fields)) {
                $this->processEntry($entryId, $fields, $streamKey, $groupName, $dlqKey, $queue, $messageCallback, $successCallback, $errorCallback);
            }
        }
    }

    /**
     * Process delayed jobs that are now due.
     */
    private function processDelayedJobs(Queue $queue, string $delayedKey, string $streamKey): void
    {
        $now = (int)(\microtime(true) * 1000);

        // Only check periodically
        if ($now - $this->lastDelayedCheck < self::DELAYED_CHECK_INTERVAL_MS) {
            return;
        }
        $this->lastDelayedCheck = $now;

        // Get jobs that are now due (read without removing to prevent job loss on crash)
        $dueJobs = $this->connection->sortedSetRangeByScore($delayedKey, 0, (float)$now, 100);

        foreach ($dueJobs as $member => $score) {
            // In zRangeByScore with scores, member is the value and score is the key when WITHSCORES is used
            // But without WITHSCORES option, we get a simple array of members
            $jobData = is_string($member) ? $member : $score;
            $fields = \json_decode($jobData, true);

            if ($fields && \json_last_error() === JSON_ERROR_NONE) {
                // Add to stream first - if this fails, job stays in delayed set for retry
                $streamId = $this->connection->streamAdd($streamKey, $fields, '*', $this->maxStreamLength);

                // Only remove from delayed set after successful add to prevent job loss
                if ($streamId !== false) {
                    $this->connection->sortedSetRemove($delayedKey, $jobData);
                }
            }
        }
    }

    /**
     * Process scheduled jobs that are now due.
     */
    private function processScheduledJobs(Queue $queue): void
    {
        $now = (int)(\microtime(true) * 1000);

        // Only check periodically
        if ($now - $this->lastScheduleCheck < self::SCHEDULE_CHECK_INTERVAL_MS) {
            return;
        }
        $this->lastScheduleCheck = $now;

        $schedulesKey = $this->getSchedulesKey($queue);
        $nextKey = $this->getScheduleNextKey($queue);
        $streamKey = $this->getStreamKey($queue);

        // Get schedules that are due (read without removing to prevent duplicate processing)
        $dueScheduleIds = $this->connection->sortedSetRangeByScore($nextKey, 0, (float)$now, 100);

        foreach ($dueScheduleIds as $scheduleId) {
            $scheduleData = $this->connection->hashGet($schedulesKey, $scheduleId);
            if ($scheduleData === false) {
                // Schedule was deleted, remove from next run queue
                $this->connection->sortedSetRemove($nextKey, $scheduleId);
                continue;
            }

            $decodedData = \json_decode($scheduleData, true);
            if ($decodedData === null && \json_last_error() !== JSON_ERROR_NONE) {
                // Invalid JSON, remove from queue to prevent infinite loop
                $this->connection->sortedSetRemove($nextKey, $scheduleId);
                continue;
            }

            $schedule = Schedule::fromArray($decodedData);

            // Remove from next run queue first (atomic operation)
            $removed = $this->connection->sortedSetRemove($nextKey, $scheduleId);

            // Skip if another consumer already processed this (removed = 0)
            if ($removed === 0) {
                continue;
            }

            // Skip if not active (paused, max runs reached, etc.)
            if (!$schedule->isActive()) {
                continue;
            }

            // Enqueue the job
            $messageData = [
                'pid' => \uniqid(more_entropy: true),
                'queue' => $queue->name,
                'timestamp' => \time(),
                'payload' => $schedule->payload,
                'schedule_id' => $schedule->id,
            ];

            $encodedData = \json_encode($messageData);
            if ($encodedData === false) {
                throw new \RuntimeException('Failed to encode message data: ' . \json_last_error_msg());
            }

            $fields = [
                'data' => $encodedData,
                'retry_count' => '0',
            ];

            $this->connection->streamAdd($streamKey, $fields, '*', $this->maxStreamLength);

            // Update run count
            $updated = $schedule->incrementRunCount();
            $encodedUpdated = \json_encode($updated->toArray());
            if ($encodedUpdated === false) {
                throw new \RuntimeException('Failed to encode schedule data: ' . \json_last_error_msg());
            }
            $this->connection->hashSet($schedulesKey, $scheduleId, $encodedUpdated);

            // Calculate and store next run time
            $nextRun = $updated->getNextRunTime(\time());
            if ($nextRun !== null && $updated->isActive()) {
                $this->connection->sortedSetAdd($nextKey, (float)($nextRun * 1000), $scheduleId);
            }
        }
    }


    private function getStreamKey(Queue $queue): string
    {
        return "{$queue->namespace}.stream.{$queue->name}";
    }

    private function getDlqKey(Queue $queue): string
    {
        return "{$queue->namespace}.stream.{$queue->name}.dlq";
    }

    private function getDelayedKey(Queue $queue): string
    {
        return "{$queue->namespace}.delayed.{$queue->name}";
    }

    private function getGroupName(Queue $queue): string
    {
        return "{$queue->namespace}.group.{$queue->name}";
    }

    private function getSchedulesKey(Queue $queue): string
    {
        return "{$queue->namespace}.schedules.{$queue->name}";
    }

    private function getScheduleNextKey(Queue $queue): string
    {
        return "{$queue->namespace}.schedule.next.{$queue->name}";
    }
}
