<?php

namespace Utopia\Queue\Concurrency;

use Utopia\Queue\Connection;
use Utopia\Queue\Message;

abstract class Manager
{
    public function __construct(protected Connection $adapter, protected int $limit = 1)
    {
    }

    public function canProcessJob(Message $message): bool
    {
        $key = $this->getConcurrencyKey($message);
        if ($this->adapter->get($key) === null) {
            $this->adapter->set($key, 0);
        }
        return \intval($this->adapter->get($key)) < $this->limit;
    }

    public function startJob(Message $message): void
    {
        $key = $this->getConcurrencyKey($message);
        $this->adapter->increment($key);
    }

    public function finishJob(Message $message): void
    {
        $key = $this->getConcurrencyKey($message);
        $this->adapter->decrement($key);
    }

    abstract protected function getConcurrencyKey(Message $message): string;
}
