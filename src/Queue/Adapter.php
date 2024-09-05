<?php

namespace Utopia\Queue;

abstract class Adapter
{
    public int $workerNum;
    public string $queue;
    public string $namespace;
    public Connection $connection;

    public function __construct(int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        $this->workerNum = $workerNum;
        $this->queue = $queue;
        $this->namespace = $namespace;
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
    abstract public function onWorkerStart(callable $callback): self;

    /**
     * Is called when a Worker stops.
     * @param callable $callback
     * @return self
     */
    abstract public function onWorkerStop(callable $callback): self;

    /**
     * Is called when a job is processed.
     * @param callable $callback
     * @return self
     */
    abstract public function onJob(callable $callback): self;

    /**
     * Returns the native server object from the Adapter.
     * @return mixed
     */
    abstract public function getNative(): mixed;
}
