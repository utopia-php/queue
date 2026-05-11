<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Adapter;
use Utopia\Queue\Consumer;
use Utopia\Queue\Message;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;
use Utopia\Queue\Server;
use Utopia\Telemetry\Adapter\Test as TestTelemetry;

class ServerTelemetryTest extends TestCase
{
    public function testRecordsQueueDepth(): void
    {
        $consumer = new ServerTelemetryPublisherConsumer([3, 2]);
        $adapter = new ServerTelemetryAdapter($consumer, 1, 'emails', 'appwrite');
        $telemetry = new TestTelemetry();

        $server = new Server($adapter);
        $server->setTelemetry($telemetry);
        $server
            ->job()
            ->inject('message')
            ->action(fn (Message $message) => null);

        $server->start();

        $this->assertArrayHasKey('messaging.queue.depth', $telemetry->gauges);
        /** @var object{values: array<int, float|int>} $queueDepth */
        $queueDepth = $telemetry->gauges['messaging.queue.depth'];
        $this->assertObjectHasProperty('values', $queueDepth);
        $this->assertSame([3, 2], $queueDepth->values);
    }

    public function testSkipsQueueDepthWhenConsumerCannotReportSize(): void
    {
        $consumer = new ServerTelemetryConsumer();
        $adapter = new ServerTelemetryAdapter($consumer, 1, 'emails', 'appwrite');
        $telemetry = new TestTelemetry();

        $server = new Server($adapter);
        $server->setTelemetry($telemetry);
        $server
            ->job()
            ->inject('message')
            ->action(fn (Message $message) => null);

        $server->start();

        $this->assertArrayHasKey('messaging.queue.depth', $telemetry->gauges);
        /** @var object{values: array<int, float|int>} $queueDepth */
        $queueDepth = $telemetry->gauges['messaging.queue.depth'];
        $this->assertObjectHasProperty('values', $queueDepth);
        $this->assertSame([], $queueDepth->values);
    }

    public function testRecordsQueueDepthErrors(): void
    {
        $consumer = new ServerTelemetryFailingPublisherConsumer();
        $adapter = new ServerTelemetryAdapter($consumer, 1, 'emails', 'appwrite');
        $telemetry = new TestTelemetry();

        $server = new Server($adapter);
        $server->setTelemetry($telemetry);
        $server
            ->job()
            ->inject('message')
            ->action(fn (Message $message) => null);

        $server->start();

        $this->assertArrayHasKey('messaging.queue.depth.errors', $telemetry->counters);
        /** @var object{values: array<int, float|int>} $queueDepthErrors */
        $queueDepthErrors = $telemetry->counters['messaging.queue.depth.errors'];
        $this->assertObjectHasProperty('values', $queueDepthErrors);
        $this->assertSame([1, 1], $queueDepthErrors->values);
    }
}

final class ServerTelemetryAdapter extends Adapter
{
    /**
     * @var callable[]
     */
    private array $onWorkerStart = [];

    /**
     * @var callable[]
     */
    private array $onWorkerStop = [];

    public function __construct(Consumer $consumer, int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        parent::__construct($workerNum, $queue, $namespace);
        $this->consumer = $consumer;
    }

    public function start(): self
    {
        foreach ($this->onWorkerStart as $callback) {
            $callback('0');
        }

        foreach ($this->onWorkerStop as $callback) {
            $callback('0');
        }

        return $this;
    }

    public function stop(): self
    {
        return $this;
    }

    public function workerStart(callable $callback): self
    {
        $this->onWorkerStart[] = $callback;
        return $this;
    }

    public function workerStop(callable $callback): self
    {
        $this->onWorkerStop[] = $callback;
        return $this;
    }
}

class ServerTelemetryConsumer implements Consumer
{
    public function consume(
        Queue $queue,
        callable $messageCallback,
        callable $successCallback,
        callable $errorCallback
    ): void {
        $message = new Message([
            'pid' => 'test-pid',
            'queue' => $queue->name,
            'timestamp' => time() - 1,
            'payload' => [],
        ]);

        $messageCallback($message);
        $successCallback($message);
    }

    public function close(): void
    {
    }
}

final class ServerTelemetryPublisherConsumer extends ServerTelemetryConsumer implements Publisher
{
    /**
     * @param int[] $queueSizes
     */
    public function __construct(private array $queueSizes)
    {
    }

    public function enqueue(Queue $queue, array $payload, bool $priority = false): bool
    {
        return true;
    }

    public function retry(Queue $queue, ?int $limit = null): void
    {
    }

    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        return array_shift($this->queueSizes) ?? 0;
    }
}

final class ServerTelemetryFailingPublisherConsumer extends ServerTelemetryConsumer implements Publisher
{
    public function enqueue(Queue $queue, array $payload, bool $priority = false): bool
    {
        return true;
    }

    public function retry(Queue $queue, ?int $limit = null): void
    {
    }

    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        throw new \RuntimeException('Queue size unavailable.');
    }
}
