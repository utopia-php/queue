<?php

namespace Utopia\Queue\Broker;

use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Queue\Consumer;
use Utopia\Queue\Message;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

readonly class Pool implements Publisher, Consumer
{
    public function __construct(
        private ?UtopiaPool $publisher = null,
        private ?UtopiaPool $consumer = null,
    ) {}

    public function enqueue(Queue $queue, array $payload, bool $priority = false): bool
    {
        return $this->delegate($this->publisher, __FUNCTION__, \func_get_args());
    }

    public function retry(Queue $queue, ?int $limit = null): void
    {
        $this->delegate($this->publisher, __FUNCTION__, \func_get_args());
    }

    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        return $this->delegate($this->publisher, __FUNCTION__, \func_get_args());
    }

    public function receive(Queue $queue, int $timeout): ?Message
    {
        return $this->delegate($this->consumer, __FUNCTION__, \func_get_args());
    }

    public function commit(Queue $queue, Message $message): void
    {
        $this->delegate($this->consumer, __FUNCTION__, \func_get_args());
    }

    public function reject(Queue $queue, Message $message): void
    {
        $this->delegate($this->consumer, __FUNCTION__, \func_get_args());
    }

    public function close(): void
    {
        // TODO: Implement closing all connections in the pool
    }

    /**
     * @param array<mixed> $args
     */
    protected function delegate(?UtopiaPool $pool, string $method, array $args): mixed
    {
        return $pool?->use(fn(Publisher|Consumer $adapter) => $adapter->$method(...$args));
    }
}
