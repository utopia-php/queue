<?php

namespace Utopia\Queue\Adapter;

use Swoole\Coroutine;
use Swoole\Process;
use Utopia\DI\Container;
use Utopia\Queue\Adapter;
use Utopia\Queue\Concurrency\Coroutine as CoroutineExecutor;
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

    /**
     * @param int $maxCoroutines Number of messages a worker may process
     *                           concurrently. This is where the Swoole adapter
     *                           owns its concurrency: it fans message processing
     *                           out across coroutines while the receive loop
     *                           keeps pulling work.
     */
    public function __construct(
        Consumer $consumer,
        int $workerNum,
        string $queue,
        string $namespace = 'utopia-queue',
        int $maxCoroutines = 1,
        Container $resources = new Container(),
    ) {
        parent::__construct($consumer, $workerNum, $queue, $namespace, $resources);
        $this->executor = new CoroutineExecutor($maxCoroutines);
    }

    public function start(): self
    {
        for ($i = 0; $i < $this->workerNum; $i++) {
            $this->spawnWorker($i);
        }

        Coroutine::set(['hook_flags' => SWOOLE_HOOK_ALL]);

        Coroutine\run(function () {
            Process::signal(SIGTERM, fn () => $this->stop());
            Process::signal(SIGINT, fn () => $this->stop());
            Process::signal(SIGCHLD, fn () => $this->reap());

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
                    $callback((string)$workerId);
                }

                foreach ($this->onWorkerStop as $callback) {
                    $callback((string)$workerId);
                }
            });
        }, false, 0, false);

        $pid = $process->start();
        $this->workers[$pid] = $process;
    }

    /**
     * Store the per-message container in the coroutine context so concurrent
     * handlers each see their own, rather than sharing one on the adapter.
     */
    protected function setContext(Container $context): void
    {
        Coroutine::getContext()[self::CONTEXT_KEY] = $context;
    }

    public function context(): Container
    {
        if (Coroutine::getCid() !== -1) {
            return Coroutine::getContext()[self::CONTEXT_KEY] ?? $this->resources();
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
