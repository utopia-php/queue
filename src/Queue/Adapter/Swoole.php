<?php

namespace Utopia\Queue\Adapter;

use Swoole\Coroutine;
use Swoole\Process;
use Utopia\Queue\Adapter;
use Utopia\Queue\Consumer;

class Swoole extends Adapter
{
    /** @var Process[] */
    protected array $workers = [];

    /** @var callable[] */
    protected array $onWorkerStart = [];

    /** @var callable[] */
    protected array $onWorkerStop = [];

    public function __construct(
        Consumer $consumer,
        int $workerNum,
        string $queue,
        string $namespace = 'utopia-queue',
    ) {
        parent::__construct($workerNum, $queue, $namespace);
        $this->consumer = $consumer;
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
                Process::signal(SIGTERM, fn () => $this->consumer->close());

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

    protected function reap(): void
    {
        while (($ret = Process::wait(false)) !== false) {
            unset($this->workers[$ret['pid']]);
        }
    }

    public function stop(): self
    {
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
