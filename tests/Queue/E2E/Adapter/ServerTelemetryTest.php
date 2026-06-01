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

        $this->assertArrayHasKey('messaging.queue.depth', $telemetry->observableGauges);
        // Each collection samples the live queue size, so two collections
        // observe the two successive depths reported by the consumer.
        $this->assertSame([3], $this->collectObservations($telemetry, 'messaging.queue.depth'));
        $this->assertSame([2], $this->collectObservations($telemetry, 'messaging.queue.depth'));
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

        $this->assertArrayHasKey('messaging.queue.depth', $telemetry->observableGauges);
        $this->assertSame([], $this->collectObservations($telemetry, 'messaging.queue.depth'));
    }

    public function testSkipsQueueDepthWhenConsumerCannotReadSize(): void
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

        $this->assertArrayHasKey('messaging.queue.depth', $telemetry->observableGauges);
        $this->assertSame([], $this->collectObservations($telemetry, 'messaging.queue.depth'));
        $this->assertArrayNotHasKey('messaging.queue.depth.errors', $telemetry->counters);
    }

    /**
     * Invoke an observable gauge's registered callback once and capture the
     * values it reports, mirroring a single telemetry collection cycle.
     *
     * @return array<int, float|int>
     */
    private function collectObservations(TestTelemetry $telemetry, string $name): array
    {
        /** @var object{callback: ?\Closure} $gauge */
        $gauge = $telemetry->observableGauges[$name];

        $values = [];
        if ($gauge->callback !== null) {
            ($gauge->callback)(function (float|int $value, iterable $attributes = []) use (&$values): void {
                $values[] = $value;
            });
        }

        return $values;
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
