<?php

namespace Utopia\Tests\Queue\Concurrency;

use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Utopia\Queue\Concurrency\Manager;
use Utopia\Queue\Concurrency\Adapter;
use Utopia\Queue\Message;

class ManagerTest extends TestCase
{
    protected Manager $manager;
    protected MockObject $mockAdapter;

    protected function setUp(): void
    {
        $this->mockAdapter = $this->createMock(Adapter::class);
        $this->manager = new class($this->mockAdapter, 2) extends Manager {
            public function getConcurrencyKey(Message $message): string
            {
                return 'test_key';
            }
        };
    }

    private function createMessage(int $id = 1): Message
    {
        return new Message([
            'pid' => "test-pid-{$id}",
            'queue' => 'test-queue',
            'timestamp' => time(),
            'payload' => ['id' => $id]
        ]);
    }

    public function testCanProcessJobWhenNoJobsRunning(): void
    {
        $message = $this->createMessage();

        $this->mockAdapter
            ->expects($this->once())
            ->method('get')
            ->with('test_key')
            ->willReturn(null);

        $this->mockAdapter
            ->expects($this->once())
            ->method('set')
            ->with('test_key', '0');

        $this->assertTrue($this->manager->canProcessJob($message));
    }

    public function testCanProcessJobWhenBelowLimit(): void
    {
        $message = $this->createMessage();

        $this->mockAdapter
            ->expects($this->once())
            ->method('get')
            ->with('test_key')
            ->willReturn('1');

        $this->assertTrue($this->manager->canProcessJob($message));
    }

    public function testCannotProcessJobWhenAtLimit(): void
    {
        $message = $this->createMessage();

        $this->mockAdapter
            ->expects($this->once())
            ->method('get')
            ->with('test_key')
            ->willReturn('2');

        $this->assertFalse($this->manager->canProcessJob($message));
    }

    public function testStartJobIncrementsCounter(): void
    {
        $message = $this->createMessage();

        $this->mockAdapter
            ->expects($this->once())
            ->method('increment')
            ->with('test_key');

        $this->manager->startJob($message);
    }

    public function testFinishJobDecrementsCounter(): void
    {
        $message = $this->createMessage();

        $this->mockAdapter
            ->expects($this->once())
            ->method('decrement')
            ->with('test_key');

        $this->manager->finishJob($message);
    }

    public function testFullConcurrencyFlow(): void
    {
        $message1 = $this->createMessage(1);
        $message2 = $this->createMessage(2);
        $message3 = $this->createMessage(3);

        // Initial state - no jobs running
        $this->mockAdapter
            ->method('get')
            ->willReturnOnConsecutiveCalls(null, '1', '2', '1');

        $this->mockAdapter
            ->expects($this->once())
            ->method('set')
            ->with('test_key', '0');

        // Should allow first job
        $this->assertTrue($this->manager->canProcessJob($message1));
        $this->manager->startJob($message1);

        // Should allow second job
        $this->assertTrue($this->manager->canProcessJob($message2));
        $this->manager->startJob($message2);

        // Should reject third job (at limit)
        $this->assertFalse($this->manager->canProcessJob($message3));

        // Complete first job
        $this->manager->finishJob($message1);

        // Should now allow third job
        $this->assertTrue($this->manager->canProcessJob($message3));
    }

    public function testWithCustomLimit(): void
    {
        // Create manager with limit of 3
        $manager = new class($this->mockAdapter, 3) extends Manager {
            public function getConcurrencyKey(Message $message): string
            {
                return 'test_key';
            }
        };

        $message = $this->createMessage();

        $this->mockAdapter
            ->method('get')
            ->willReturn('2');

        // Should allow job when count is 2 and limit is 3
        $this->assertTrue($manager->canProcessJob($message));
    }
}
