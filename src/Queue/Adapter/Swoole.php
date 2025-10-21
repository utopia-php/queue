<?php

namespace Utopia\Queue\Adapter;

use Swoole\Constant;
use Swoole\Process\Pool;
use Utopia\CLI\Console;
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

        // Register signal handlers in the main process before starting pool
        if (extension_loaded('pcntl')) {
            pcntl_signal(SIGTERM, function () {
                Console::info("[Swoole] Received SIGTERM, initiating graceful shutdown...");
                $this->stop();
            });

            pcntl_signal(SIGINT, function () {
                Console::info("[Swoole] Received SIGINT, initiating graceful shutdown...");
                $this->stop();
            });

            // Enable async signals
            pcntl_async_signals(true);
        } else {
            Console::warning("[Swoole] pcntl extension is not loaded, worker will not shutdown gracefully.");
        }

        $this->pool->start();
        return $this;
    }

    public function stop(): self
    {
        if ($this->onStop) {
            call_user_func($this->onStop);
        }

        Console::info("[Swoole] Shutting down process pool...");
        $this->pool->shutdown();
        Console::success("[Swoole] Process pool stopped.");
        return $this;
    }

    public function workerStart(callable $callback): self
    {
        $this->pool->on(Constant::EVENT_WORKER_START, function (Pool $pool, string $workerId) use ($callback) {
            // Register signal handlers in each worker process for graceful shutdown
            if (extension_loaded('pcntl')) {
                pcntl_signal(SIGTERM, function () use ($workerId) {
                    Console::info("[Worker] Worker {$workerId} received SIGTERM, closing consumer...");
                    $this->consumer->close();
                });

                pcntl_signal(SIGINT, function () use ($workerId) {
                    Console::info("[Worker] Worker {$workerId} received SIGINT, closing consumer...");
                    $this->consumer->close();
                });

                pcntl_async_signals(true);
            }

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
