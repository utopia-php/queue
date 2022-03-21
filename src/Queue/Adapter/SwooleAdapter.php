<?php

namespace Utopia\Queue\Adapter;

use Swoole\Process;
use Swoole\Process\Pool;
use Utopia\Queue\Adapter;
use Utopia\Queue\Connection;

/**
 * 
 * @package Utopia\Queue\Adapter
 */
class SwooleAdapter extends Adapter
{
    protected Pool $pool;
    protected Connection $connection;

    public function __construct(int $workerNum, Connection $connection)
    {
        parent::__construct($workerNum);

        $this->connection = $connection;
        $this->pool = new Pool($workerNum);
    }

    public function start(): void
    {
        $this->pool->set(['enable_coroutine' => true]);
        $this->pool->start();
    }

    public function shutdown(): void
    {
        $this->pool->shutdown();
    }

    public function onStart(callable $callback): self
    {
        $this->pool->on('start', function () use ($callback) {
            call_user_func($callback);

            Process::signal('2', function () {
                $this->shutdown();
            });
        });

        return $this;
    }

    public function onWorkerStart(callable $callback): self
    {
        return $this;
    }

    public function onJob(callable $callback): self
    {
        $this->pool->on('WorkerStart', function (Pool $pool, string $workerId) use ($callback) {
            while (true) {
                /**
                 * Waiting for next Job.
                 */
                $nextJob = $this->connection->rightPopLeftPush($this->queue, 'processing', 60);
                if (!$nextJob) continue;

                try {
                    call_user_func($callback, $nextJob);
                } catch (\Throwable $th) {
                    var_dump($th->getMessage());
                }
            }
        });

        return $this;
    }

    public function getNative(): Pool
    {
        return $this->pool;
    }
}
