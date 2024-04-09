<?php

namespace Utopia\Queue;

abstract class Adapter
{
    public int $workerNum;

    /**
     * @var array<string> $queues
     */
    public array $queues;
    public string $namespace;
    public Connection $connection;

    public function __construct(int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        $this->workerNum = $workerNum;
        $this->queues = [$queue];
        $this->namespace = $namespace;
    }

    public function addQueue(string $queue): self
    {
        if (!(\in_array($queue, $this->queues))) {
            $this->queues[] = $queue;
        }

        return $this;
    }

    /**
     * Starts the Server.
     * @return self
     */
    abstract public function start(): self;

    /**
     * Stops the Server.
     * @return self
     */
    abstract public function stop(): self;

    /**
     * Is called when a Worker starts.
     * @param callable $callback
     * @return self
     */
    abstract public function workerStart(callable $callback): self;

    /**
     * Is called when a Worker stops.
     * @param callable $callback
     * @return self
     */
    abstract public function workerStop(callable $callback): self;

    /**
     * Returns the native server object from the Adapter.
     * @return mixed
     */
    abstract public function getNative(): mixed;
}
