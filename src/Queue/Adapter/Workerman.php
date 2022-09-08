<?php

namespace Utopia\Queue\Adapter;

use Utopia\Queue\Adapter;
use Utopia\Queue\Connection;
use Workerman\Worker;

class Workerman extends Adapter
{
    protected Worker $worker;

    public function __construct(Connection $connection, int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        parent::__construct($workerNum, $queue, $namespace);

        $this->worker = new Worker();
        $this->worker->count = $workerNum;
        $this->connection = $connection;
    }

    public function start(callable $callback): self
    {
        Worker::runAll();
        call_user_func($callback);
        return $this;
    }

    public function shutdown(callable $callback): self
    {
        Worker::stopAll();
        call_user_func($callback);
        return $this;
    }

    public function workerStart(callable $callback): self
    {
        $this->worker->onWorkerStart = function ($worker) use ($callback) {
            call_user_func($callback, $worker->workerId);
        };

        return $this;
    }

    public function getNative(): Worker
    {
        return $this->worker;
    }
}
