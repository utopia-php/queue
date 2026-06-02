<?php

namespace Utopia\Queue;

use Utopia\DI\Container;

abstract class Adapter
{
    public Queue $queue;
    protected ?Container $context = null;

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
        $this->consumer->consume(
            $this->queue,
            function (Message $message) use ($messageCallback) {
                $this->context = new Container($this->resources());

                return $messageCallback($message);
            },
            $successCallback,
            function (?Message $message, \Throwable $error) use ($errorCallback) {
                if ($message === null) {
                    $this->context = new Container($this->resources());
                }

                $errorCallback($message, $error);
            },
        );
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
