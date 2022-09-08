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
     * @return void
     */
    abstract public function start(): void;

    /**
     * Shuts down the Server.
     * @return void
     */
    abstract public function shutdown(): void;

    /**
     * Is called when the Server starts.
     * @param callable $callback
     * @return self
     */
    abstract public function onStart(callable $callback): self;

    /**
     * Is called when a Worker receives a Job.
     * @param callable $callback
     * @return self
     */
    abstract public function onJob(callable $callback): self;

    /**
     * Is called when a Worker starts.
     * @param callable $callback
     * @return self
     */
    abstract public function onWorkerStart(callable $callback): self;

    /**
     * Returns the native server object from the Adapter.
     * @return mixed
     */
    abstract public function getNative(): mixed;
}
