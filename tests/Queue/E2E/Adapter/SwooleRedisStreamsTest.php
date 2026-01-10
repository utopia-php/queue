<?php

namespace Tests\E2E\Adapter;

use Utopia\Queue\Broker\RedisStreams;
use Utopia\Queue\Connection\RedisStream;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class SwooleRedisStreamsTest extends Base
{
    protected function getPublisher(): Publisher
    {
        $connection = new RedisStream('redis', 6379);
        return new RedisStreams($connection);
    }

    protected function getQueue(): Queue
    {
        return new Queue('swoole-redis-streams');
    }

    /**
     * Test delayed job enqueueing.
     */
    public function testDelayedJobs(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        // Enqueue a delayed job
        $result = $publisher->enqueueDelayed($this->getQueue(), [
            'type' => 'test_string',
            'value' => 'delayed job'
        ], 1);

        $this->assertTrue($result);

        // Check delayed count
        $delayedCount = $publisher->getDelayedCount($this->getQueue());
        $this->assertGreaterThanOrEqual(1, $delayedCount);

        // Wait for the job to be processed
        // Worker's consume loop has a 2s block timeout + 1s delayed check interval
        sleep(5);

        // Delayed count should be back to 0
        $delayedCount = $publisher->getDelayedCount($this->getQueue());
        $this->assertEquals(0, $delayedCount);
    }

    /**
     * Test scheduled job enqueueing.
     */
    public function testScheduledJobs(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        // Enqueue a job scheduled for 2 seconds from now
        $result = $publisher->enqueueAt($this->getQueue(), [
            'type' => 'test_string',
            'value' => 'scheduled job'
        ], time() + 2);

        $this->assertTrue($result);

        sleep(3);
    }

    /**
     * Test stream observability.
     */
    public function testObservability(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        // Enqueue a job first
        $publisher->enqueue($this->getQueue(), [
            'type' => 'test_string',
            'value' => 'observability test'
        ]);

        sleep(1);

        // Test getStreamInfo
        $info = $publisher->getStreamInfo($this->getQueue());
        $this->assertIsArray($info);

        // Test getGroupInfo
        $groupInfo = $publisher->getGroupInfo($this->getQueue());
        $this->assertIsArray($groupInfo);

        // Test getConsumersInfo
        $consumers = $publisher->getConsumersInfo($this->getQueue());
        $this->assertIsArray($consumers);

        // Test getQueueSize
        $size = $publisher->getQueueSize($this->getQueue());
        $this->assertIsInt($size);

        // Test getLag
        $lag = $publisher->getLag($this->getQueue());
        $this->assertIsInt($lag);
    }

    /**
     * Test message history/replay functionality.
     */
    public function testMessageHistory(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        // Enqueue several jobs
        for ($i = 0; $i < 5; $i++) {
            $publisher->enqueue($this->getQueue(), [
                'type' => 'test_number',
                'value' => $i
            ]);
        }

        sleep(1);

        // Get message history
        $messages = $publisher->getMessages($this->getQueue(), '-', '+', 10);
        $this->assertIsArray($messages);
    }

    /**
     * Test schedule management.
     */
    public function testScheduleManagement(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        $schedule = new \Utopia\Queue\Schedule(
            id: 'e2e-test-schedule',
            payload: ['type' => 'test_string', 'value' => 'scheduled'],
            interval: 300
        );

        // Create schedule
        $result = $publisher->schedule($this->getQueue(), $schedule);
        $this->assertTrue($result);

        // Retrieve schedule
        $retrieved = $publisher->getSchedule($this->getQueue(), 'e2e-test-schedule');
        $this->assertNotNull($retrieved);
        $this->assertEquals('e2e-test-schedule', $retrieved->id);

        // Pause schedule
        $result = $publisher->pauseSchedule($this->getQueue(), 'e2e-test-schedule');
        $this->assertTrue($result);

        $paused = $publisher->getSchedule($this->getQueue(), 'e2e-test-schedule');
        $this->assertTrue($paused->isPaused());

        // Resume schedule
        $result = $publisher->resumeSchedule($this->getQueue(), 'e2e-test-schedule');
        $this->assertTrue($result);

        $resumed = $publisher->getSchedule($this->getQueue(), 'e2e-test-schedule');
        $this->assertFalse($resumed->isPaused());

        // List schedules
        $schedules = $publisher->getSchedules($this->getQueue());
        $this->assertArrayHasKey('e2e-test-schedule', $schedules);

        // Remove schedule
        $result = $publisher->unschedule($this->getQueue(), 'e2e-test-schedule');
        $this->assertTrue($result);

        $deleted = $publisher->getSchedule($this->getQueue(), 'e2e-test-schedule');
        $this->assertNull($deleted);
    }

    /**
     * Test cron schedule.
     */
    public function testCronSchedule(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        $schedule = new \Utopia\Queue\Schedule(
            id: 'e2e-cron-schedule',
            payload: ['type' => 'test_string', 'value' => 'cron job'],
            cron: '*/5 * * * *'
        );

        $result = $publisher->schedule($this->getQueue(), $schedule);
        $this->assertTrue($result);

        $retrieved = $publisher->getSchedule($this->getQueue(), 'e2e-cron-schedule');
        $this->assertEquals('*/5 * * * *', $retrieved->cron);

        // Cleanup
        $publisher->unschedule($this->getQueue(), 'e2e-cron-schedule');
    }

    /**
     * Test stream trimming.
     */
    public function testStreamTrimming(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        // Enqueue many messages
        for ($i = 0; $i < 20; $i++) {
            $publisher->enqueue($this->getQueue(), [
                'type' => 'test_number',
                'value' => $i
            ]);
        }

        sleep(1);

        // Trim the stream
        $trimmed = $publisher->trimStream($this->getQueue(), 5);
        $this->assertGreaterThan(0, $trimmed);
    }

    /**
     * Test pending count.
     */
    public function testPendingCount(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        $pending = $publisher->getPendingCount($this->getQueue());
        $this->assertIsInt($pending);
        $this->assertGreaterThanOrEqual(0, $pending);
    }

    /**
     * Test queue size with failed jobs.
     */
    public function testQueueSizeWithFailedJobs(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        $failedSize = $publisher->getQueueSize($this->getQueue(), true);
        $this->assertIsInt($failedSize);
        $this->assertGreaterThanOrEqual(0, $failedSize);
    }

    /**
     * Test consumer ID management.
     */
    public function testConsumerIdManagement(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        // Get default consumer ID
        $defaultId = $publisher->getConsumerId();
        $this->assertStringStartsWith('worker-', $defaultId);

        // Set custom consumer ID
        $publisher->setConsumerId('e2e-test-consumer');
        $this->assertEquals('e2e-test-consumer', $publisher->getConsumerId());
    }

    /**
     * Test various payload types are preserved.
     */
    public function testPayloadTypes(): void
    {
        /** @var RedisStreams $publisher */
        $publisher = $this->getPublisher();

        // Test complex nested payload
        $complexPayload = [
            'type' => 'test_assoc',
            'value' => [
                'string' => 'test',
                'number' => 123,
                'float' => 1.23,
                'bool' => true,
                'null' => null,
                'array' => [1, 2, 3],
                'nested' => [
                    'deep' => 'value'
                ]
            ]
        ];

        $result = $publisher->enqueue($this->getQueue(), $complexPayload);
        $this->assertTrue($result);

        sleep(1);

        // Verify messages can be retrieved
        $messages = $publisher->getMessages($this->getQueue(), '-', '+', 1);
        $this->assertNotEmpty($messages);
    }
}
