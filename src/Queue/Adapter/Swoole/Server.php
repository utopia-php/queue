<?php

namespace Utopia\Queue\Adapter\Swoole;

use Swoole\Process\Pool;
use Swoole\Runtime;
use Utopia\Queue\Adapter;
use Utopia\Queue\Connection;

class Server extends Adapter
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
        Runtime::enableCoroutine();
        $this->pool->set(['enable_coroutine' => true]);
        $this->pool->start();
        return $this;
    }

    public function stop(): self
    {
        $this->pool->shutdown();
        return $this;
    }

    public function onWorkerStart(callable $callback): self
    {
        $this->pool->on('WorkerStart', function (Pool $pool, string $workerId) use ($callback) {
            call_user_func($callback, $workerId);
        });

        return $this;
    }

    public function onWorkerStop(callable $callback): self
    {
        $this->pool->on('WorkerStart', function (Pool $pool, string $workerId) use ($callback) {
            call_user_func($callback, $workerId);
        });

        return $this;
    }

    public function onJob(callable $callback): self
    {
        call_user_func($callback);
        // go(function () use ($callback) {
        //     call_user_func($callback);
        // });

        return $this;
    }

    public function getNative(): Pool
    {
        return $this->pool;
    }
}
