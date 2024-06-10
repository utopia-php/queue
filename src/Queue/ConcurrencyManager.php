<?php

namespace Utopia\Queue;

abstract class ConcurrencyManager
{
    protected $concurrencyKey;
    protected $maxLimit;
    protected $jobCount = [];

    public function setConcurrencyKey($key)
    {
        $this->concurrencyKey = $key;
    }

    public function setMaxLimit($limit)
    {
        $this->maxLimit = $limit;
    }

    abstract protected function getConcurrencyKey($job);

    public function canProcessJob($job)
    {
        $key = $this->getConcurrencyKey($job);
        if (!isset($this->jobCount[$key])) {
            $this->jobCount[$key] = 0;
        }
        return $this->jobCount[$key] < $this->maxLimit;
    }

    public function startJob($job)
    {
        $key = $this->getConcurrencyKey($job);
        $this->jobCount[$key]++;
    }

    public function finishJob($job)
    {
        $key = $this->getConcurrencyKey($job);
        $this->jobCount[$key]--;
    }
}
