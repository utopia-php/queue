<?php

namespace Utopia\Queue\Adapter\Workerman;

use Utopia\Queue\Adapter;
use Utopia\Queue\Connection;
use Workerman\Worker;

class Server extends Adapter
{
    protected Worker $worker;

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
        return $this;
    }

    public function stop(): self
    {
        Worker::stopAll();
        return $this;
    }

    public function onWorkerStart(callable $callback): self
    {
        $this->worker->onWorkerStart = function ($worker) use ($callback) {
            call_user_func($callback, $worker->workerId);
        };

        return $this;
    }
    public function onWorkerStop(callable $callback): self
    {
        $this->worker->onWorkerStop = function ($worker) use ($callback) {
            call_user_func($callback, $worker->workerId);
        };

        return $this;
    }

    public function onJob(callable $callback): self
    {
        call_user_func($callback);

        return $this;
    }

    public function getNative(): Worker
    {
        return $this->worker;
    }
}
