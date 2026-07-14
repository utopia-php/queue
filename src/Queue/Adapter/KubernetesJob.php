<?php

namespace Utopia\Queue\Adapter;

use Swoole\Coroutine;
use Swoole\Process;
use Utopia\DI\Container;
use Utopia\Queue\Adapter;
use Utopia\Queue\Message;

/**
 * Run-to-completion adapter for queue workers that run as Kubernetes Jobs — for
 * example Jobs that KEDA spawns off the queue depth. Unlike the long-running
 * Swoole/Workerman adapters, there is no worker pool: the current process drains
 * the queue and returns, so the Job completes. One pod is one worker.
 *
 * Producers still enqueue with any Publisher (e.g. the Redis broker); this only
 * changes how the messages are consumed.
 *
 * With Swoole loaded, the whole worker lifecycle runs inside one coroutine
 * scheduler: timers and signal watchers registered by workerStart hooks must be
 * created and cleared within the same scheduler, or Coroutine\run never returns
 * and the Job never completes. SIGTERM/SIGINT trigger stop() so pod termination
 * finishes the in-flight message instead of stranding it in the processing
 * list (pcntl conflicts with Swoole's signal handling, so Process::signal is
 * used; without Swoole, pcntl when available).
 */
class KubernetesJob extends Adapter
{
    /** @var callable[] */
    protected array $onWorkerStart = [];

    /** @var callable[] */
    protected array $onWorkerStop = [];

    public function start(): self
    {
        $lifecycle = function (): void {
            try {
                foreach ($this->onWorkerStart as $callback) {
                    $callback('0');
                }
            } finally {
                foreach ($this->onWorkerStop as $callback) {
                    $callback('0');
                }
            }
        };

        if (!\extension_loaded('swoole') || Coroutine::getCid() >= 0) {
            $lifecycle();

            return $this;
        }

        Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);

        $error = null;

        Coroutine\run(function () use (&$error, $lifecycle): void {
            try {
                $lifecycle();
            } catch (\Throwable $thrown) {
                $error = $thrown;
            }
        });

        if ($error instanceof \Throwable) {
            throw $error;
        }

        return $this;
    }

    public function stop(): self
    {
        $this->stopped = true;
        $this->consumer->close();

        return $this;
    }

    #[\Override]
    public function runsToCompletion(): bool
    {
        return true;
    }

    /**
     * Drain the queue, then return. Processes messages until a receive() times
     * out (the queue is empty) or stop() is called, so the Job completes rather
     * than blocking forever like the long-running adapters.
     */
    #[\Override]
    public function consume(callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $this->stopped = false;

        $swoole = \extension_loaded('swoole');

        if ($swoole) {
            Process::signal(SIGTERM, fn(): \Utopia\Queue\Adapter\KubernetesJob => $this->stop());
            Process::signal(SIGINT, fn(): \Utopia\Queue\Adapter\KubernetesJob => $this->stop());
        } elseif (\function_exists('pcntl_async_signals')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGTERM, $this->stop(...));
            pcntl_signal(SIGINT, $this->stop(...));
        }

        try {
            while (!$this->isStopped()) {
                $message = $this->consumer->receive($this->queue, static::RECEIVE_TIMEOUT);

                if (!$message instanceof Message) {
                    break;
                }

                $this->context = new Container($this->resources());
                $this->process($message, $messageCallback, $successCallback, $errorCallback);
            }
        } finally {
            if ($swoole) {
                Process::signal(SIGTERM, null);
                Process::signal(SIGINT, null);
            }
        }
    }

    public function workerStart(callable $callback): self
    {
        $this->onWorkerStart[] = $callback;

        return $this;
    }

    public function workerStop(callable $callback): self
    {
        $this->onWorkerStop[] = $callback;

        return $this;
    }
}
