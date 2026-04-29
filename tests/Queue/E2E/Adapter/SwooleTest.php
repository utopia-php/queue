<?php

namespace Tests\E2E\Adapter;

use Utopia\Queue\Broker\Redis as RedisBroker;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class SwooleTest extends Base
{
    private function getConnection(): Redis
    {
        return new Redis('redis', 6379);
    }

    protected function getPublisher(): Publisher
    {
        return new RedisBroker($this->getConnection());
    }

    protected function getQueue(): Queue
    {
        return new Queue('swoole');
    }

    public function testPriorityJobIsConsumedBeforeNormalJobs(): void
    {
        $connection = $this->getConnection();
        $key = "{$this->getQueue()->namespace}.queue.{$this->getQueue()->name}";

        // Flush any leftover state from previous runs.
        while ($connection->rightPopArray($key, 1) !== false) {
            // drain
        }

        // Enqueue three normal jobs (pushed to head/left).
        $this->getPublisher()->enqueue($this->getQueue(), ['order' => 'normal-1']);
        $this->getPublisher()->enqueue($this->getQueue(), ['order' => 'normal-2']);
        $this->getPublisher()->enqueue($this->getQueue(), ['order' => 'normal-3']);

        // Enqueue one priority job (pushed to tail/right — same end BRPOP reads from).
        $this->getPublisher()->enqueue($this->getQueue(), ['order' => 'priority'], priority: true);

        // The first pop should yield the priority job.
        $first = $connection->rightPopArray($key, 1);
        $this->assertNotFalse($first, 'Expected a job but queue was empty');
        $this->assertSame('priority', $first['payload']['order'], 'Priority job should be consumed first');

        // The remaining three should be normal jobs (consumed oldest-first).
        $second = $connection->rightPopArray($key, 1);
        $this->assertSame('normal-1', $second['payload']['order']);

        $third = $connection->rightPopArray($key, 1);
        $this->assertSame('normal-2', $third['payload']['order']);

        $fourth = $connection->rightPopArray($key, 1);
        $this->assertSame('normal-3', $fourth['payload']['order']);

        // Queue should now be empty.
        $this->assertFalse($connection->rightPopArray($key, 1));
    }
}
