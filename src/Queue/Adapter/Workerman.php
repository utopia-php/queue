<?php

namespace Utopia\Queue\Adapter;

use Swoole\Process\Pool;
use Utopia\Queue\Adapter;
use Utopia\Queue\Connection;
use Workerman\Worker;

class Workerman extends Adapter
{
    protected Worker $worker;
    protected mixed $onStartCallback = null;
    protected mixed $onWorkerStartCallback = null;

    public function __construct(Connection $connection, int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        parent::__construct($workerNum, $queue, $namespace);

        $this->worker = new Worker();
        $this->worker->count = $workerNum;
        $this->connection = $connection;
    }

    public function start(): void
    {
        Worker::runAll();
        call_user_func($this->onStartCallback);
    }

    public function shutdown(): void
    {
        Worker::stopAll();
    }

    public function onStart(callable $callback): self
    {
        $this->onStartCallback = $callback;

        return $this;
    }

    public function onWorkerStart(callable $callback): self
    {
        $this->onWorkerStartCallback = $callback;

        return $this;
    }

    public function onJob(callable $callback): self
    {
        $this->worker->onWorkerStart = function () use ($callback) {
            if (!is_null($this->onWorkerStartCallback)) {
                call_user_func($this->onWorkerStartCallback);
            }
            call_user_func($callback);
        };

        return $this;
    }

    public function getNative(): Pool
    {
        return $this->pool;
    }
}
