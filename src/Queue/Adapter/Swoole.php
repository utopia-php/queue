<?php

namespace Utopia\Queue\Adapter;

use Swoole\Constant;
use Swoole\Process\Pool;
use Utopia\Queue\Adapter;
use Utopia\Queue\Consumer;

class Swoole extends Adapter
{
    protected Pool $pool;

    /** @var callable */
    private $onStop;

    public function __construct(Consumer $consumer, int $workerNum, string $queue, string $namespace = 'utopia-queue')
    {
        parent::__construct($workerNum, $queue, $namespace);

        $this->consumer = $consumer;
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
        if ($this->onStop) {
            call_user_func($this->onStop);
        }
        $this->pool->shutdown();
        return $this;
    }

    public function workerStart(callable $callback): self
    {
        $this->pool->on(Constant::EVENT_WORKER_START, function (Pool $pool, string $workerId) use ($callback) {
            call_user_func($callback, $workerId);
        });

        return $this;
    }

    public function workerStop(callable $callback): self
    {
        $this->onStop = $callback;
        $this->pool->on(Constant::EVENT_WORKER_STOP, function (Pool $pool, string $workerId) use ($callback) {
            call_user_func($callback, $workerId);
        });

        return $this;
    }

    public function getNative(): Pool
    {
        return $this->pool;
    }
}
