<?php

namespace Utopia\Queue\Adapter;

use Swoole\Process;
use Swoole\Process\Pool;
use Utopia\Queue\Adapter;
use Utopia\Queue\Connection;
use Utopia\Queue\Job;

/**
 * 
 * @package Utopia\Queue\Adapter
 */
class SwooleAdapter extends Adapter
{
    protected Pool $pool;
    protected Connection $connection;
    protected bool $running = true;

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
        $this->running = false;
        // $this->pool->stop();
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
        return $this;
    }

    public function onJob(callable $callback): self
    {
        $this->pool->on('WorkerStart', function (Pool $pool, string $workerId) use ($callback) {
            while ($this->running) {
                /**
                 * Waiting for next Job.
                 */
                $nextJob = $this->connection->rightPopLeftPush($this->queue, 'processing', 5);
                if (!$nextJob) continue;

                $job = new Job();
                $job
                    ->setPid($nextJob['pid'])
                    ->setQueue($nextJob['queue'])
                    ->setTimestamp(\intval($nextJob['timestamp']))
                    ->setPayload($nextJob['payload']);

                try {
                    call_user_func($callback, $job);
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
