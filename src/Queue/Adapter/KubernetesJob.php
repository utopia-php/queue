<?php

namespace Utopia\Queue\Adapter;

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
 */
class KubernetesJob extends Adapter
{
    /** @var callable[] */
    protected array $onWorkerStart = [];

    /** @var callable[] */
    protected array $onWorkerStop = [];

    public function start(): self
    {
        try {
            foreach ($this->onWorkerStart as $callback) {
                $callback('0');
            }
        } finally {
            foreach ($this->onWorkerStop as $callback) {
                $callback('0');
            }
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

        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, $this->stop(...));
        pcntl_signal(SIGINT, $this->stop(...));

        while (!$this->isStopped()) {
            $message = $this->consumer->receive($this->queue, static::RECEIVE_TIMEOUT);

            if (!$message instanceof Message) {
                break;
            }

            $this->context = new Container($this->resources());
            $this->process($message, $messageCallback, $successCallback, $errorCallback);
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
