<?php

namespace Tests\Queue\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Broker\RedisStreams;
use Utopia\Queue\Connection\RedisStream;
use Utopia\Queue\Message;
use Utopia\Queue\Queue;
use Utopia\Queue\Schedule;

/**
 * Unit tests for RedisStreams broker.
 *
 * Note: These tests require a running Redis instance.
 */
class RedisStreamsBrokerTest extends TestCase
{
    private RedisStream $connection;
    private RedisStreams $broker;
    private string $testNamespace;
    private Queue $queue;

    protected function setUp(): void
    {
        $this->connection = new RedisStream('redis', 6379);
        $this->broker = new RedisStreams($this->connection, 1000, 3, 1000);
        $this->testNamespace = 'test-' . uniqid();
        $this->queue = new Queue('test-queue', $this->testNamespace);
    }

    protected function tearDown(): void
    {
        // Clean up test keys
        $this->cleanupTestKeys();
        $this->connection->close();
    }

    private function cleanupTestKeys(): void
    {
        $redis = new \Redis();
        $redis->connect('redis', 6379);

        $keys = $redis->keys($this->testNamespace . '*');
        if (!empty($keys)) {
            $redis->del($keys);
        }

        $redis->close();
    }


    public function testEnqueue(): void
    {
        $result = $this->broker->enqueue($this->queue, ['task' => 'test', 'data' => 123]);

        $this->assertTrue($result);
        $this->assertGreaterThan(0, $this->broker->getQueueSize($this->queue));
    }

    public function testEnqueueMultiple(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->broker->enqueue($this->queue, ['index' => $i]);
        }

        $this->assertEquals(5, $this->broker->getQueueSize($this->queue));
    }

    public function testGetQueueSize(): void
    {
        $this->assertEquals(0, $this->broker->getQueueSize($this->queue));

        $this->broker->enqueue($this->queue, ['test' => 1]);
        $this->broker->enqueue($this->queue, ['test' => 2]);

        $this->assertEquals(2, $this->broker->getQueueSize($this->queue));
    }

    public function testGetQueueSizeFailedJobs(): void
    {
        // Initially no failed jobs
        $this->assertEquals(0, $this->broker->getQueueSize($this->queue, true));
    }

    public function testConsumerId(): void
    {
        // Default consumer ID
        $defaultId = $this->broker->getConsumerId();
        $this->assertStringStartsWith('worker-', $defaultId);

        // Set custom consumer ID
        $this->broker->setConsumerId('custom-worker-123');
        $this->assertEquals('custom-worker-123', $this->broker->getConsumerId());
    }


    public function testEnqueueDelayed(): void
    {
        $result = $this->broker->enqueueDelayed($this->queue, ['task' => 'delayed'], 60);

        $this->assertTrue($result);
        $this->assertEquals(1, $this->broker->getDelayedCount($this->queue));
    }

    public function testEnqueueAt(): void
    {
        $futureTime = time() + 3600;
        $result = $this->broker->enqueueAt($this->queue, ['task' => 'scheduled'], $futureTime);

        $this->assertTrue($result);
        $this->assertEquals(1, $this->broker->getDelayedCount($this->queue));
    }

    public function testGetDelayedCount(): void
    {
        $this->assertEquals(0, $this->broker->getDelayedCount($this->queue));

        $this->broker->enqueueDelayed($this->queue, ['task' => 1], 60);
        $this->broker->enqueueDelayed($this->queue, ['task' => 2], 120);

        $this->assertEquals(2, $this->broker->getDelayedCount($this->queue));
    }

    public function testQueueSizeIncludesDelayed(): void
    {
        $this->broker->enqueue($this->queue, ['immediate' => true]);
        $this->broker->enqueueDelayed($this->queue, ['delayed' => true], 60);

        // Total size should include both immediate and delayed
        $this->assertEquals(2, $this->broker->getQueueSize($this->queue));
    }


    public function testScheduleCron(): void
    {
        $schedule = new Schedule(
            id: 'test-cron-schedule',
            payload: ['type' => 'cron-job'],
            cron: '*/5 * * * *'
        );

        $result = $this->broker->schedule($this->queue, $schedule);
        $this->assertTrue($result);

        // Verify schedule was stored
        $retrieved = $this->broker->getSchedule($this->queue, 'test-cron-schedule');
        $this->assertNotNull($retrieved);
        $this->assertEquals('test-cron-schedule', $retrieved->id);
        $this->assertEquals('*/5 * * * *', $retrieved->cron);
    }

    public function testScheduleInterval(): void
    {
        $schedule = new Schedule(
            id: 'test-interval-schedule',
            payload: ['type' => 'interval-job'],
            interval: 300
        );

        $result = $this->broker->schedule($this->queue, $schedule);
        $this->assertTrue($result);

        $retrieved = $this->broker->getSchedule($this->queue, 'test-interval-schedule');
        $this->assertNotNull($retrieved);
        $this->assertEquals(300, $retrieved->interval);
    }

    public function testUnschedule(): void
    {
        $schedule = new Schedule(
            id: 'to-remove',
            payload: ['remove' => true],
            interval: 60
        );

        $this->broker->schedule($this->queue, $schedule);
        $this->assertNotNull($this->broker->getSchedule($this->queue, 'to-remove'));

        $result = $this->broker->unschedule($this->queue, 'to-remove');
        $this->assertTrue($result);

        $this->assertNull($this->broker->getSchedule($this->queue, 'to-remove'));
    }

    public function testGetSchedules(): void
    {
        $this->broker->schedule($this->queue, new Schedule('sched-1', ['a' => 1], interval: 60));
        $this->broker->schedule($this->queue, new Schedule('sched-2', ['b' => 2], interval: 120));
        $this->broker->schedule($this->queue, new Schedule('sched-3', ['c' => 3], cron: '0 * * * *'));

        $schedules = $this->broker->getSchedules($this->queue);

        $this->assertCount(3, $schedules);
        $this->assertArrayHasKey('sched-1', $schedules);
        $this->assertArrayHasKey('sched-2', $schedules);
        $this->assertArrayHasKey('sched-3', $schedules);
    }

    public function testPauseSchedule(): void
    {
        $schedule = new Schedule(
            id: 'pausable',
            payload: ['pause' => 'test'],
            interval: 60
        );

        $this->broker->schedule($this->queue, $schedule);

        $result = $this->broker->pauseSchedule($this->queue, 'pausable');
        $this->assertTrue($result);

        $retrieved = $this->broker->getSchedule($this->queue, 'pausable');
        $this->assertTrue($retrieved->isPaused());
        $this->assertFalse($retrieved->isActive());
    }

    public function testResumeSchedule(): void
    {
        $schedule = new Schedule(
            id: 'resumable',
            payload: ['resume' => 'test'],
            interval: 60
        );

        $this->broker->schedule($this->queue, $schedule);
        $this->broker->pauseSchedule($this->queue, 'resumable');

        $result = $this->broker->resumeSchedule($this->queue, 'resumable');
        $this->assertTrue($result);

        $retrieved = $this->broker->getSchedule($this->queue, 'resumable');
        $this->assertFalse($retrieved->isPaused());
        $this->assertTrue($retrieved->isActive());
    }

    public function testPauseNonExistentSchedule(): void
    {
        $result = $this->broker->pauseSchedule($this->queue, 'non-existent');
        $this->assertFalse($result);
    }

    public function testResumeNonExistentSchedule(): void
    {
        $result = $this->broker->resumeSchedule($this->queue, 'non-existent');
        $this->assertFalse($result);
    }


    public function testGetStreamInfo(): void
    {
        // Add some messages first
        $this->broker->enqueue($this->queue, ['test' => 1]);
        $this->broker->enqueue($this->queue, ['test' => 2]);

        $info = $this->broker->getStreamInfo($this->queue);

        $this->assertIsArray($info);
        $this->assertArrayHasKey('length', $info);
        $this->assertEquals(2, $info['length']);
    }

    public function testGetStreamInfoEmpty(): void
    {
        // Empty stream (doesn't exist yet)
        $info = $this->broker->getStreamInfo($this->queue);
        $this->assertIsArray($info);
    }

    public function testGetGroupInfo(): void
    {
        // Need to trigger group creation by enqueueing
        $this->broker->enqueue($this->queue, ['test' => true]);

        $groupInfo = $this->broker->getGroupInfo($this->queue);

        $this->assertIsArray($groupInfo);
        if (!empty($groupInfo)) {
            $this->assertArrayHasKey('name', $groupInfo);
        }
    }

    public function testGetConsumersInfo(): void
    {
        $this->broker->enqueue($this->queue, ['test' => true]);

        $consumers = $this->broker->getConsumersInfo($this->queue);
        $this->assertIsArray($consumers);
    }

    public function testGetLag(): void
    {
        $lag = $this->broker->getLag($this->queue);
        $this->assertIsInt($lag);
        $this->assertGreaterThanOrEqual(0, $lag);
    }

    public function testGetPendingCount(): void
    {
        $pending = $this->broker->getPendingCount($this->queue);
        $this->assertIsInt($pending);
        $this->assertGreaterThanOrEqual(0, $pending);
    }

    public function testGetMessages(): void
    {
        $this->broker->enqueue($this->queue, ['msg' => 1]);
        $this->broker->enqueue($this->queue, ['msg' => 2]);
        $this->broker->enqueue($this->queue, ['msg' => 3]);

        $messages = $this->broker->getMessages($this->queue, '-', '+', 10);

        $this->assertIsArray($messages);
        $this->assertCount(3, $messages);

        foreach ($messages as $message) {
            $this->assertInstanceOf(Message::class, $message);
        }
    }

    public function testGetMessagesWithLimit(): void
    {
        for ($i = 0; $i < 10; $i++) {
            $this->broker->enqueue($this->queue, ['index' => $i]);
        }

        $messages = $this->broker->getMessages($this->queue, '-', '+', 5);

        $this->assertCount(5, $messages);
    }

    public function testGetMessage(): void
    {
        $this->broker->enqueue($this->queue, ['specific' => 'message']);

        // Get all messages to find the ID
        $messages = $this->broker->getMessages($this->queue, '-', '+', 1);
        $this->assertNotEmpty($messages);

        $firstMessage = $messages[0];
        $streamId = $firstMessage->getPayload()['streamId'] ?? null;

        // Skip if streamId is not available in payload
        if ($streamId) {
            $retrieved = $this->broker->getMessage($this->queue, $streamId);
            $this->assertNotNull($retrieved);
        }
    }

    public function testGetMessageNonExistent(): void
    {
        $message = $this->broker->getMessage($this->queue, '0-0');
        $this->assertNull($message);
    }

    public function testTrimStream(): void
    {
        // Add 20 messages
        for ($i = 0; $i < 20; $i++) {
            $this->broker->enqueue($this->queue, ['index' => $i]);
        }

        $initialSize = $this->broker->getQueueSize($this->queue) - $this->broker->getDelayedCount($this->queue);
        $this->assertEquals(20, $initialSize);

        // Trim to 10 (uses exact trimming, not approximate)
        $trimmed = $this->broker->trimStream($this->queue, 10);

        // Verify stream was trimmed
        $finalSize = $this->broker->getQueueSize($this->queue) - $this->broker->getDelayedCount($this->queue);
        $this->assertLessThanOrEqual(10, $finalSize);
        $this->assertGreaterThanOrEqual(0, $trimmed);
    }

    public function testDeleteConsumer(): void
    {
        $this->broker->enqueue($this->queue, ['test' => true]);

        // This will create the consumer group
        $pending = $this->broker->deleteConsumer($this->queue, 'non-existent-consumer');
        $this->assertIsInt($pending);
    }


    public function testMessageFormat(): void
    {
        $payload = ['key' => 'value', 'nested' => ['a' => 1, 'b' => 2]];
        $this->broker->enqueue($this->queue, $payload);

        $messages = $this->broker->getMessages($this->queue, '-', '+', 1);
        $this->assertCount(1, $messages);

        $message = $messages[0];
        $this->assertInstanceOf(Message::class, $message);

        $messagePayload = $message->getPayload();
        $this->assertEquals('value', $messagePayload['key']);
        $this->assertEquals(['a' => 1, 'b' => 2], $messagePayload['nested']);
    }

    public function testMessageTimestamp(): void
    {
        $beforeEnqueue = time();
        $this->broker->enqueue($this->queue, ['test' => true]);
        $afterEnqueue = time();

        $messages = $this->broker->getMessages($this->queue, '-', '+', 1);
        $message = $messages[0];

        $timestamp = $message->getTimestamp();
        $this->assertGreaterThanOrEqual($beforeEnqueue, $timestamp);
        $this->assertLessThanOrEqual($afterEnqueue, $timestamp);
    }


    public function testRetryEmptyDLQ(): void
    {
        // Should not throw when DLQ is empty
        $this->broker->retry($this->queue);
        $this->assertTrue(true);
    }

    public function testRetryWithLimit(): void
    {
        // Should not throw
        $this->broker->retry($this->queue, 5);
        $this->assertTrue(true);
    }
}
