<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Broker\Redis as RedisBroker;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Queue;

/**
 * Verifies that priority jobs (pushed to the tail via rightPushArray) are consumed
 * before normal jobs (pushed to the head via leftPushArray) when BRPOP reads from
 * the tail.
 *
 * This test bypasses the worker and reads directly from the queue so it can assert ordering.
 */
class RedisPriorityTest extends TestCase
{
    private RedisBroker $broker;
    private Queue $queue;
    private Redis $connection;

    protected function setUp(): void
    {
        $this->connection = new Redis('redis', 6379);
        $this->broker = new RedisBroker($this->connection);
        $this->queue = new Queue('priority-e2e-test');

        // Flush any leftover state from previous runs.
        $key = "{$this->queue->namespace}.queue.{$this->queue->name}";
        while ($this->connection->rightPopArray($key, 0) !== false) {
            // drain
        }
    }

    public function testPriorityJobIsConsumedBeforeNormalJobs(): void
    {
        // Enqueue three normal jobs (pushed to head/left).
        $this->broker->enqueue($this->queue, ['order' => 'normal-1']);
        $this->broker->enqueue($this->queue, ['order' => 'normal-2']);
        $this->broker->enqueue($this->queue, ['order' => 'normal-3']);

        // Enqueue one priority job (pushed to tail/right — same end BRPOP reads from).
        $this->broker->enqueue($this->queue, ['order' => 'priority'], priority: true);

        $key = "{$this->queue->namespace}.queue.{$this->queue->name}";

        // The first pop should yield the priority job.
        $first = $this->connection->rightPopArray($key, 1);
        $this->assertNotFalse($first, 'Expected a job but queue was empty');
        $this->assertSame('priority', $first['payload']['order'], 'Priority job should be consumed first');

        // The remaining three should be normal jobs (consumed oldest-first).
        $second = $this->connection->rightPopArray($key, 1);
        $this->assertSame('normal-1', $second['payload']['order']);

        $third = $this->connection->rightPopArray($key, 1);
        $this->assertSame('normal-2', $third['payload']['order']);

        $fourth = $this->connection->rightPopArray($key, 1);
        $this->assertSame('normal-3', $fourth['payload']['order']);

        // Queue should now be empty.
        $this->assertFalse($this->connection->rightPopArray($key, 0));
    }

    public function testEnqueuePriorityReturnsBool(): void
    {
        $result = $this->broker->enqueue($this->queue, ['check' => 'return-value'], priority: true);
        $this->assertIsBool($result);
        $this->assertTrue($result);
    }
}
