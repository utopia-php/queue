<?php

namespace Utopia\Queue\Adapter;

use Utopia\Queue\Adapter;
use Utopia\Queue\Connection;
use Workerman\Worker;

class Workerman extends Adapter
{
    protected Worker $worker;
    protected $shutdownCallback = null;
    protected $initCallback = null;

    public function __construct(Connection $connection, int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        parent::__construct($workerNum, $queue, $namespace);

        $this->worker = new Worker();
        $this->worker->count = $workerNum;
        $this->connection = $connection;
    }

    public function start(): self
    {
        Worker::runAll();
        if (is_callable($this->initCallback)) {
            call_user_func($this->initCallback);
        }
        return $this;
    }

    public function stop(): self
    {
        Worker::stopAll();
        if (is_callable($this->shutdownCallback)) {
            call_user_func($this->shutdownCallback);
        }
        return $this;
    }

    public function init(callable $callback): self
    {
        $this->initCallback = $callback;
        return $this;
    }

    public function shutdown(callable $callback): self
    {
        $this->shutdownCallback = $callback;
        return $this;
    }

    public function workerStart(callable $callback): self
    {
        $this->worker->onWorkerStart = function ($worker) use ($callback) {
            call_user_func($callback, $worker->workerId);
        };

        return $this;
    }
    public function workerStop(callable $callback): self
    {
        $this->worker->onWorkerStop = function ($worker) use ($callback) {
            call_user_func($callback, $worker->workerId);
        };

        return $this;
    }

    public function getNative(): Worker
    {
        return $this->worker;
    }
}
