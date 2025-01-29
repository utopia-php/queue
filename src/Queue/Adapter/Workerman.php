<?php

namespace Utopia\Queue\Adapter;

use Utopia\Queue\Adapter;
use Utopia\Queue\Consumer;
use Workerman\Worker;

class Workerman extends Adapter
{
    protected Worker $worker;

    public function __construct(Consumer $consumer, int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        parent::__construct($workerNum, $queue, $namespace);

        $this->worker = new Worker();
        $this->worker->count = $workerNum;
        $this->consumer = $consumer;
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
