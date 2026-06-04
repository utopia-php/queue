<?php

namespace Utopia\Queue;

use Utopia\DI\Container;

abstract class Adapter
{
    protected const int RECEIVE_TIMEOUT = 2;

    public Queue $queue;
    protected ?Container $context = null;
    protected bool $stopped = false;

    public function __construct(
        public Consumer $consumer,
        public int $workerNum,
        string $queue,
        public string $namespace = 'utopia-queue',
        protected Container $resources = new Container(),
    ) {
        $this->queue = new Queue($queue, $namespace);
    }

    /**
     * Starts the Server.
     * @return self
     */
    abstract public function start(): self;

    /**
     * Stops the Server.
     * @return self
     */
    abstract public function stop(): self;

    public function consume(callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $this->stopped = false;

        while (!$this->stopped) {
            $message = $this->consumer->receive($this->queue, static::RECEIVE_TIMEOUT);

            if ($message === null) {
                continue;
            }

            $this->process($message, $messageCallback, $successCallback, $errorCallback);
        }
    }

    /**
     * Never throws: a failing handler, commit, reject, or error callback is all
     * routed to $errorCallback so nothing escapes (and is lost) on a coroutine.
     */
    protected function process(Message $message, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        try {
            $this->setContext(new Container($this->resources()));

            try {
                $messageCallback($message);
                $this->consumer->commit($this->queue, $message);
                $successCallback($message);
            } catch (\Throwable $error) {
                $this->consumer->reject($this->queue, $message);
                $errorCallback($message, $error);
            }
        } catch (\Throwable $error) {
            try {
                $errorCallback($message, $error);
            } catch (\Throwable) {
                // the error callback itself failed; nothing left to do
            }
        }
    }

    protected function setContext(Container $context): void
    {
        $this->context = $context;
    }

    public function resources(): Container
    {
        return $this->resources;
    }

    public function context(): Container
    {
        return $this->context ??= new Container($this->resources());
    }

    /**
     * Is called when a Worker starts.
     * @param callable $callback
     * @return self
     */
    abstract public function workerStart(callable $callback): self;

    /**
     * Is called when a Worker stops.
     * @param callable $callback
     * @return self
     */
    abstract public function workerStop(callable $callback): self;
}
