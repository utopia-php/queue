<?php

namespace Utopia\Queue;

/**
 * Utopia PHP Framework
 *
 * @package Utopia\Queue
 *
 * @link https://github.com/utopia-php/framework
 * @author Torsten Dittmann <torsten@appwrite.io>
 * @version 1.0 RC1
 * @license The MIT License (MIT) <http://www.opensource.org/licenses/mit-license.php>
 */
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
