<?php

namespace Utopia\Queue\Concurrency;

use Utopia\Queue\Message;

abstract class Manager {
    protected $concurrencyKey;
    protected $maxLimit;
    protected $jobCount = [];
    protected Adapter $adapter;

    public function __construct(Adapter $adapter)
    {
        $this->adapter = $adapter;   
    }

    public function setConcurrencyKey($key) {
        $this->concurrencyKey = $key;
    }

    public function setMaxLimit($limit) {
        $this->maxLimit = $limit;
    }

    abstract protected function getConcurrencyKey(Message $message): string;

    public function canProcessJob(Message $message) {
        $key = $this->getConcurrencyKey($message);
        if ($this->adapter->get($key) === false) {
            $this->adapter->set($key, 0);
        }
        return $this->adapter->get($key) < $this->maxLimit;
    }

    public function startJob(Message $message) {
        $key = $this->getConcurrencyKey($message);
        $this->adapter->increment($key);
    }

    public function finishJob(Message $message) {
        $key = $this->getConcurrencyKey($message);
        $this->adapter->decrement($key);
    }
}
