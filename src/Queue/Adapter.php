<?php

namespace Utopia\Queue;

use Utopia\DI\Container;
use Utopia\Queue\Concurrency\Executor;
use Utopia\Queue\Concurrency\Inline;

abstract class Adapter
{
    /**
     * How long the receive loop blocks for a message before looping back to
     * re-check whether it should keep running.
     */
    protected const int RECEIVE_TIMEOUT = 2;

    public Queue $queue;
    protected ?Container $context = null;

    /**
     * Strategy for processing each received message. Defaults to running them
     * inline, one at a time; a concurrent adapter (e.g. Swoole) swaps in an
     * executor that fans processing out across coroutines. This is where the
     * adapter — not the broker — owns the concurrency model.
     */
    protected Executor $executor;

    /** Set to break out of the receive loop on its next iteration. */
    protected bool $stopped = false;

    public function __construct(
        public Consumer $consumer,
        public int $workerNum,
        string $queue,
        public string $namespace = 'utopia-queue',
        protected Container $resources = new Container(),
    ) {
        $this->queue = new Queue($queue, $namespace);
        $this->executor = new Inline();
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

    /**
     * Receive messages on a single loop and hand each one to the executor. With
     * the default {@see Inline} executor a message is processed before the next
     * is received; a concurrent executor processes several at once while the
     * loop keeps receiving (bounded by the executor's back-pressure).
     */
    public function consume(callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $this->stopped = false;

        while (!$this->stopped) {
            $message = $this->consumer->receive($this->queue, static::RECEIVE_TIMEOUT);

            if ($message === null) {
                continue;
            }

            $this->executor->submit(function () use ($message, $messageCallback, $successCallback, $errorCallback) {
                $this->process($message, $messageCallback, $successCallback, $errorCallback);
            });
        }

        $this->executor->drain();
    }

    /**
     * Process one message: give it a fresh context, run the handler, then
     * acknowledge or reject it.
     */
    protected function process(Message $message, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $this->setContext(new Container($this->resources()));

        try {
            $messageCallback($message);
            $this->consumer->commit($this->queue, $message);
            $successCallback($message);
        } catch (\Throwable $error) {
            $this->consumer->reject($this->queue, $message);
            $errorCallback($message, $error);
        }
    }

    /**
     * Install the per-message context container. Overridden by concurrent
     * adapters to keep the container coroutine-local.
     */
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
