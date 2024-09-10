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
        $value = $this->adapter->get($key);
        if ($value === null) {
            $this->adapter->set($key, "0");
            $value = 0;
        }
        return \intval($value) < $this->limit;
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

    abstract public function getConcurrencyKey(Message $message): string;
}
