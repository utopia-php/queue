<?php

namespace Utopia\Queue\Broker;

use Utopia\Queue\Consumer;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;
use Utopia\Pools\Pool as UtopiaPool;

readonly class Pool implements Publisher, Consumer
{
    public function __construct(
        private UtopiaPool $publisherPool,
        private UtopiaPool $consumerPool,
    )
    {
    }

    protected function delegatePublish(string $method, array $args): mixed
    {
        return $this->publisherPool->use(function (Publisher $adapter) use ($method, $args) {
            return $adapter->$method(...$args);
        });
    }

    protected function delegateConsumer(string $method, array $args): mixed
    {
        return $this->consumerPool->use(function (Consumer $adapter) use ($method, $args) {
            return $adapter->$method(...$args);
        });
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
}