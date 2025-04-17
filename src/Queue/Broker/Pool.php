<?php

namespace Utopia\Queue\Broker;

use Utopia\Queue\Consumer;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;
use Utopia\Pools\Pool as UtopiaPool;

readonly class Pool implements Publisher, Consumer
{
    public function __construct(
        private ?UtopiaPool $publisher = null,
        private ?UtopiaPool $consumer = null,
    )
    {
    }

    public function enqueue(Queue $queue, array $payload): bool
    {
        return $this->delegatePublish(__FUNCTION__, \func_get_args());
    }

    public function retry(Queue $queue, ?int $limit = null): void
    {
        $this->delegatePublish(__FUNCTION__, \func_get_args());
    }

    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        return $this->delegatePublish(__FUNCTION__, \func_get_args());
    }

    public function consume(Queue $queue, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $this->delegateConsumer(__FUNCTION__, \func_get_args());
    }

    public function close(): void
    {
        $this->delegateConsumer(__FUNCTION__, \func_get_args());
    }

    protected function delegatePublish(string $method, array $args): mixed
    {
        return $this->publisher?->use(function (Publisher $adapter) use ($method, $args) {
            return $adapter->$method(...$args);
        });
    }

    protected function delegateConsumer(string $method, array $args): mixed
    {
        return $this->consumer?->use(function (Consumer $adapter) use ($method, $args) {
            return $adapter->$method(...$args);
        });
    }
}