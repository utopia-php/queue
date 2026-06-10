<?php

namespace Utopia\Queue\Adapter;

use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\WaitGroup;
use Swoole\Process;
use Utopia\DI\Container;
use Utopia\Queue\Adapter;
use Utopia\Queue\Consumer;

class Swoole extends Adapter
{
    protected const string CONTEXT_KEY = '__utopia__';

    /** @var Process[] */
    protected array $workers = [];

    /** @var callable[] */
    protected array $onWorkerStart = [];

    /** @var callable[] */
    protected array $onWorkerStop = [];

    protected int $maxCoroutines;

    public function __construct(
        Consumer $consumer,
        int $workerNum,
        string $queue,
        string $namespace = 'utopia-queue',
        int $maxCoroutines = 1,
        Container $resources = new Container(),
    ) {
        parent::__construct($consumer, $workerNum, $queue, $namespace, $resources);
        $this->maxCoroutines = max(1, $maxCoroutines);
    }

    public function start(): self
    {
        for ($i = 0; $i < $this->workerNum; $i++) {
            $this->spawnWorker($i);
        }

        Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);

        Coroutine\run(function () {
            Process::signal(SIGTERM, fn() => $this->stop());
            Process::signal(SIGINT, fn() => $this->stop());
            Process::signal(SIGCHLD, fn() => $this->reap());

            while (\count($this->workers) > 0) {
                Coroutine::sleep(1);
            }
        });

        return $this;
    }

    protected function spawnWorker(int $workerId): void
    {
        $process = new Process(function () use ($workerId) {
            Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);

            Coroutine\run(function () use ($workerId) {
                Process::signal(SIGTERM, function () {
                    $this->stopped = true;
                    $this->consumer->close();
                });

                foreach ($this->onWorkerStart as $callback) {
                    $callback((string) $workerId);
                }

                foreach ($this->onWorkerStop as $callback) {
                    $callback((string) $workerId);
                }
            });
        }, false, 0, false);

        $pid = $process->start();
        $this->workers[$pid] = $process;
    }

    /**
     * Receive on one loop, process each message on its own coroutine. The
     * channel caps concurrency at $maxCoroutines: push() blocks the loop while
     * the pool is full.
     */
    public function consume(callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $this->stopped = false;
        $slots = new Channel($this->maxCoroutines);
        $waitGroup = new WaitGroup();

        while (!$this->stopped) {
            $message = $this->consumer->receive($this->queue, static::RECEIVE_TIMEOUT);

            if ($message === null) {
                continue;
            }

            $slots->push(true);
            $waitGroup->add();

            Coroutine::create(function () use ($message, $messageCallback, $successCallback, $errorCallback, $slots, $waitGroup) {
                try {
                    $this->process($message, $messageCallback, $successCallback, $errorCallback);
                } catch (\Throwable $error) {
                    // process() is total; net for a stray throw so it isn't lost
                    error_log('Uncaught error while processing queue message: ' . $error->getMessage());
                } finally {
                    $waitGroup->done();
                    $slots->pop();
                }
            });
        }

        $waitGroup->wait();
    }

    public function context(): Container
    {
        // Each message runs in its own coroutine, so the container is created
        // lazily per coroutine and stays isolated across concurrent handlers.
        if (Coroutine::getCid() !== -1) {
            return Coroutine::getContext()[self::CONTEXT_KEY] ??= new Container($this->resources());
        }

        return $this->resources();
    }

    protected function reap(): void
    {
        while (($ret = Process::wait(false)) !== false) {
            unset($this->workers[$ret['pid']]);
        }
    }

    public function stop(): self
    {
        $this->stopped = true;

        foreach ($this->workers as $pid => $process) {
            Process::kill($pid, SIGTERM);
        }

        return $this;
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
