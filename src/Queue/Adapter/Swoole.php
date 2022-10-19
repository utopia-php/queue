<?php

namespace Utopia\Queue\Adapter;

use Swoole\Process\Pool;
use Utopia\Queue\Adapter;
use Utopia\Queue\Connection;

class Swoole extends Adapter
{
    protected Pool $pool;

    public function __construct(Connection $connection, int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        parent::__construct($workerNum, $queue, $namespace);

        $this->connection = $connection;
        $this->pool = new Pool($workerNum);
    }

    public function start(): self
    {
        $this->pool->set(['enable_coroutine' => true]);
        $this->pool->start();
        return $this;
    }

    public function stop(): self
    {
        $this->pool->shutdown();
        return $this;
    }

    public function workerStart(callable $callback): self
    {
        $this->pool->on('WorkerStart', function (Pool $pool, string $workerId) use ($callback) {
            call_user_func($callback, $workerId);
        });

        return $this;
    }

    public function workerStop(callable $callback): self
    {
        $this->pool->on('WorkerStart', function (Pool $pool, string $workerId) use ($callback) {
            call_user_func($callback, $workerId);
        });

        return $this;
    }

    public function getNative(): Pool
    {
        return $this->pool;
    }
}
