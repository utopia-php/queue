<?php

namespace Utopia\Queue\Adapter;

use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Process;
use Utopia\DI\Container;
use Utopia\Queue\Adapter;
use Utopia\Queue\Consumer;
use Utopia\Queue\Error\ConsumerFailures;
use Utopia\Queue\Message;

class Swoole extends Adapter
{
    protected const string CONTEXT_KEY = '__utopia__';

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
        protected int $maxCoroutines = 1,
        Container $resources = new Container(),
    ) {
        parent::__construct($consumer, $workerNum, $queue, $namespace, $resources);
        $this->maxCoroutines = \max(1, $maxCoroutines);
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

    public function consume(callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $messageCallback = function (Message $message) use ($messageCallback) {
            Coroutine::getContext()[self::CONTEXT_KEY] = new Container($this->resources());

            return $messageCallback($message);
        };

        $errorCallback = function (?Message $message, \Throwable $error) use ($errorCallback) {
            if ($message === null) {
                Coroutine::getContext()[self::CONTEXT_KEY] = new Container($this->resources());
            }

            $errorCallback($message, $error);
        };

        $channel = new Channel($this->maxCoroutines);
        $errors = [];

        for ($i = 0; $i < $this->maxCoroutines; $i++) {
            Coroutine::create(function () use ($messageCallback, $successCallback, $errorCallback, $channel, &$errors) {
                try {
                    $this->consumer->consume(
                        $this->queue,
                        $messageCallback,
                        $successCallback,
                        $errorCallback,
                    );
                } catch (\Throwable $error) {
                    $errors[] = $error;
                    $this->consumer->close();
                    $channel->push(true);
                    return;
                }

                $channel->push(true);
            });
        }

        for ($i = 0; $i < $this->maxCoroutines; $i++) {
            $channel->pop();
        }

        $channel->close();

        if ($errors !== []) {
            throw new ConsumerFailures($errors);
        }
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
