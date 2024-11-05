<?php

namespace Utopia\Tests\Queue\Concurrency;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Adapter\Swoole\Redis;
use Utopia\Queue\Concurrency\Manager;
use Utopia\Queue\Message;
use Utopia\Queue\Connection\Redis as RedisConnection;

class ManagerE2ETest extends TestCase
{
    protected Manager $manager;
    protected Redis $redisAdapter;
    protected RedisConnection $connection;

    protected function setUp(): void
    {
        // Create Redis connection
        $this->connection = new RedisConnection(
            host: 'redis', // Docker service name
            port: 6379,
            user: '',
            password: ''
        );

        // Create Redis adapter
        $this->redisAdapter = new Redis('redis', 6379);

        // Create Manager with Redis adapter
        $this->manager = new class($this->redisAdapter, 2) extends Manager {
            public function getConcurrencyKey(Message $message): string
            {
                return "test_concurrent_{$message->getQueue()}";
            }
        };
    }

    private function createMessage(int $id = 1, string $queue = 'default'): Message
    {
        return new Message([
            'pid' => "test-pid-{$id}",
            'queue' => $queue,
            'timestamp' => time(),
            'payload' => ['id' => $id]
        ]);
    }

    public function testConcurrentJobProcessing(): void
    {
        $message1 = $this->createMessage(1);
        $message2 = $this->createMessage(2);
        $message3 = $this->createMessage(3);

        // First job should be allowed
        $this->assertTrue($this->manager->canProcessJob($message1));
        $this->manager->startJob($message1);

        // Second job should be allowed
        $this->assertTrue($this->manager->canProcessJob($message2));
        $this->manager->startJob($message2);

        // Third job should be rejected (at limit)
        $this->assertFalse($this->manager->canProcessJob($message3));

        // Finish first job
        $this->manager->finishJob($message1);

        // Now third job should be allowed
        $this->assertTrue($this->manager->canProcessJob($message3));

        // Clean up
        $this->manager->finishJob($message2);
    }

    public function testMultipleQueuesIndependentConcurrency(): void
    {
        $queue1Message1 = $this->createMessage(1, 'queue1');
        $queue1Message2 = $this->createMessage(2, 'queue1');
        $queue2Message1 = $this->createMessage(3, 'queue2');
        $queue2Message2 = $this->createMessage(4, 'queue2');

        // Queue 1 should allow two jobs
        $this->assertTrue($this->manager->canProcessJob($queue1Message1));
        $this->manager->startJob($queue1Message1);
        $this->assertTrue($this->manager->canProcessJob($queue1Message2));
        $this->manager->startJob($queue1Message2);

        // Queue 2 should also allow two jobs (independent limit)
        $this->assertTrue($this->manager->canProcessJob($queue2Message1));
        $this->manager->startJob($queue2Message1);
        $this->assertTrue($this->manager->canProcessJob($queue2Message2));
        $this->manager->startJob($queue2Message2);

        // Clean up
        $this->manager->finishJob($queue1Message1);
        $this->manager->finishJob($queue1Message2);
        $this->manager->finishJob($queue2Message1);
        $this->manager->finishJob($queue2Message2);
    }

    public function testConcurrencyCounterRecovery(): void
    {
        $message = $this->createMessage();

        // Simulate a crash by starting jobs without finishing them
        $this->manager->startJob($message);
        $this->manager->startJob($message);

        // Create new manager instance (simulating process restart)
        $newManager = new class($this->redisAdapter, 2) extends Manager {
            public function getConcurrencyKey(Message $message): string
            {
                return "test_concurrent_{$message->getQueue()}";
            }
        };

        // Counter should still be at 2
        $this->assertFalse($newManager->canProcessJob($message));

        // Reset counter (simulating manual intervention)
        $this->redisAdapter->set("test_concurrent_{$message->getQueue()}", "0");

        // Should now allow new jobs
        $this->assertTrue($newManager->canProcessJob($message));
    }

    public function testHighConcurrencyScenario(): void
    {
        // Create manager with higher limit
        $highConcurrencyManager = new class($this->redisAdapter, 10) extends Manager {
            public function getConcurrencyKey(Message $message): string
            {
                return "test_concurrent_{$message->getQueue()}";
            }
        };

        $processed = 0;
        $messages = [];

        // Simulate processing 20 jobs with a limit of 10 concurrent jobs
        for ($i = 0; $i < 20; $i++) {
            $message = $this->createMessage($i);
            $messages[] = $message;

            if ($highConcurrencyManager->canProcessJob($message)) {
                $highConcurrencyManager->startJob($message);
                $processed++;

                // Simulate completing some jobs to make room for others
                if ($processed > 5) {
                    for ($j = 0; $j < 3; $j++) {
                        $highConcurrencyManager->finishJob($messages[$j]);
                    }
                    $processed -= 3;
                }
            }
        }

        // Clean up remaining jobs
        foreach ($messages as $message) {
            $highConcurrencyManager->finishJob($message);
        }
    }
} 