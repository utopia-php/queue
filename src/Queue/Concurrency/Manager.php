<?php

namespace Utopia\Queue\Concurrency;

use Utopia\Queue\Message;

abstract class Manager
{

    protected string $queue;
    protected string $concurrencyKey;
    protected int $limit = 1;
    protected Adapter $adapter;

    public function __construct(string $queue, int $limit, Adapter $adapter)
    {
        $this->queue = $queue;
        $this->limit = $limit;
        $this->adapter = $adapter;
    }

    public function setConcurrencyKey($key)
    {
        $this->concurrencyKey = $key;
    }

    public function match(Message $message): bool
    {
        if ($this->queue === $message->getQueue())
            return true;
        return false;
    }

    public function canProcessJob(Message $message)
    {
        $key = $this->getConcurrencyKey($message);
        if ($this->adapter->get($key) === false) {
            $this->adapter->set($key, 0);
        }
        return $this->adapter->get($key) < $this->limit;
    }

    public function startJob(Message $message)
    {
        $key = $this->getConcurrencyKey($message);
        $this->adapter->increment($key);
    }

    public function finishJob(Message $message)
    {
        $key = $this->getConcurrencyKey($message);
        $this->adapter->decrement($key);
    }

    abstract protected function getConcurrencyKey(Message $message): string;
}
