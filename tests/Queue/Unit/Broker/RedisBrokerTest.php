<?php

namespace Tests\Unit\Broker;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Broker\Redis;
use Utopia\Queue\Connection;
use Utopia\Queue\Queue;

class RedisBrokerTest extends TestCase
{
    private Connection $connection;
    private Redis $broker;
    private Queue $queue;

    protected function setUp(): void
    {
        $this->connection = $this->createMock(Connection::class);
        $this->broker = new Redis($this->connection);
        $this->queue = new Queue('test');
    }

    public function testEnqueueNormalUsesLeftPush(): void
    {
        $this->connection
            ->expects($this->once())
            ->method('leftPushArray')
            ->with(
                $this->equalTo('utopia-queue.queue.test'),
                $this->callback(fn($p) => $p['queue'] === 'test' && $p['payload'] === ['foo' => 'bar'])
            )
            ->willReturn(true);

        $this->connection->expects($this->never())->method('rightPushArray');

        $result = $this->broker->enqueue($this->queue, ['foo' => 'bar']);

        $this->assertTrue($result);
    }

    public function testEnqueuePriorityFalseUsesLeftPush(): void
    {
        $this->connection
            ->expects($this->once())
            ->method('leftPushArray')
            ->willReturn(true);

        $this->connection->expects($this->never())->method('rightPushArray');

        $result = $this->broker->enqueue($this->queue, ['foo' => 'bar'], priority: false);

        $this->assertTrue($result);
    }

    public function testEnqueuePriorityUsesRightPush(): void
    {
        $this->connection
            ->expects($this->once())
            ->method('rightPushArray')
            ->with(
                $this->equalTo('utopia-queue.queue.test'),
                $this->callback(fn($p) => $p['queue'] === 'test' && $p['payload'] === ['urgent' => true])
            )
            ->willReturn(true);

        $this->connection->expects($this->never())->method('leftPushArray');

        $result = $this->broker->enqueue($this->queue, ['urgent' => true], priority: true);

        $this->assertTrue($result);
    }

    public function testEnqueuePriorityPayloadHasRequiredFields(): void
    {
        $capturedPayload = null;

        $this->connection
            ->expects($this->once())
            ->method('rightPushArray')
            ->with(
                $this->anything(),
                $this->callback(function ($p) use (&$capturedPayload) {
                    $capturedPayload = $p;
                    return true;
                })
            )
            ->willReturn(true);

        $this->broker->enqueue($this->queue, ['data' => 1], priority: true);

        $this->assertArrayHasKey('pid', $capturedPayload);
        $this->assertArrayHasKey('queue', $capturedPayload);
        $this->assertArrayHasKey('timestamp', $capturedPayload);
        $this->assertArrayHasKey('payload', $capturedPayload);
        $this->assertNotEmpty($capturedPayload['pid']);
    }
}
