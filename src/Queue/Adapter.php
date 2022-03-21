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
    protected int $workerNum;
    protected string $queue;

    function __construct(int $workerNum) {
        $this->workerNum = $workerNum;
    }

    public function setQueueName(string $queue): self
    {
        $this->queue = $queue;

        return $this;
    }

    /**
     * Starts the Server.
     * @return void 
     */
    public abstract function start(): void;

    /**
     * Shuts down the Server.
     * @return void 
     */
    public abstract function shutdown(): void;

    /**
     * Is called when the Server starts.
     * @param callable $callback 
     * @return self 
     */
    public abstract function onStart(callable $callback): self;

    /**
     * Is called when a Worker receives a Job.
     * @param callable $callback 
     * @return self 
     */
    public abstract function onJob(callable $callback): self;

    /**
     * Is called when a Worker starts.
     * @param callable $callback 
     * @return self 
     */
    public abstract function onWorkerStart(callable $callback): self;

    /**
     * Returns the native server object from the Adapter.
     * @return mixed 
     */
    public abstract function getNative(): mixed;
}