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
     * @param callable $callback
     * @return self
     */
    abstract public function start(callable $callback): self;

    /**
     * Shuts down the Server.
     * @param callable $callback
     * @return self
     */
    abstract public function shutdown(callable $callback): self;

    /**
     * Is called when a Worker starts.
     * @param callable $callback
     * @return self
     */
    abstract public function workerStart(callable $callback): self;

    /**
     * Returns the native server object from the Adapter.
     * @return mixed
     */
    abstract public function getNative(): mixed;
}
