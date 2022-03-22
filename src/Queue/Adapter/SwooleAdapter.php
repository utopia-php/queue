<?php

namespace Utopia\Queue\Adapter;

use Swoole\Process;
use Swoole\Process\Pool;
use Utopia\CLI\Console;
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

    public function shutdown(): void
    {
        $this->running = false;
        $this->pool->shutdown();
    }

    public function onStart(callable $callback): self
    {
        $this->pool->on('start', function () use ($callback) {
            Console::success("[Worker] Queue Workers are starting");
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
            Console::success("[Worker] Worker {$workerId} is ready!");
            while ($this->running) {
                /**
                 * Waiting for next Job.
                 */
                $nextJob = $this->connection->rightPopArray("{$this->namespace}.queue.{$this->queue}", 5);
                if (!$nextJob) continue;

                $job = new Job();
                $job
                    ->setPid($nextJob['pid'])
                    ->setQueue($nextJob['queue'])
                    ->setTimestamp(\intval($nextJob['timestamp']))
                    ->setPayload($nextJob['payload']);

                Console::info("[Job] Received Job ({$job->getPid()}).");

                /**
                 * Move Job to Jobs and it's PID to the processing list.
                 */
                $this->connection->setArray("{$this->namespace}.jobs.{$this->queue}.{$job->getPid()}", $nextJob);
                $this->connection->leftPush("{$this->namespace}.processing.{$this->queue}", $job->getPid());

                try {
                    call_user_func($callback, $job);
                    $this->connection->remove("{$this->namespace}.jobs.{$this->queue}.{$job->getPid()}");
                    Console::success("[Job] ({$job->getPid()}) successfully run.");
                } catch (\Throwable $th) {
                    $this->connection->leftPush("{$this->namespace}.failed.{$this->queue}", $job->getPid());
                    Console::error("[Job] ({$job->getPid()}) failed to run.");
                } finally {
                    $this->connection->remove("{$this->namespace}.processing.{$this->queue}", $job->getPid());
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
