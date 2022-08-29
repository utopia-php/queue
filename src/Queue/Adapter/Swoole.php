<?php

namespace Utopia\Queue\Adapter;

use Swoole\Process\Pool;
use Utopia\Queue\Adapter;
use Utopia\Queue\Connection;

class Swoole extends Adapter
{
    protected Pool $pool;
    protected mixed $onWorkerStartCallback = null;

    public function __construct(Connection $connection, int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        parent::__construct($workerNum, $queue, $namespace);

        $this->connection = $connection;
        $this->pool = new Pool($workerNum);
    }

    public function start(): void
    {
        $this->pool->set(['enable_coroutine' => true]);
        $this->pool->start();
    }

    public function stop(): void
    {
        $this->pool->shutdown();
    }

    public function onStart(callable $callback): self
    {
        $this->pool->on('start', function () use ($callback) {
            call_user_func($callback);
        });

        return $this;
    }

    public function onWorkerStart(callable $callback): self
    {
        $this->onWorkerStartCallback = $callback;

        return $this;
    }

    public function onJob(callable $callback): self
    {
        $this->pool->on('WorkerStart', function (Pool $pool, string $workerId) use ($callback) {
            if (!is_null($this->onWorkerStartCallback)) {
                call_user_func($this->onWorkerStartCallback);
            }
            call_user_func($callback);
        });

        return $this;
    }

    public function getNative(): Pool
    {
        return $this->pool;
    }
}
