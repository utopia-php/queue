<?php

namespace Utopia\Queue\Adapter;

use Utopia\Queue\Adapter;

use Utopia\Pools\Pool as UtopiaPool;

class Pool extends Adapter
{
    /**
     * @var UtopiaPool<covariant Adapter>
     */
    protected UtopiaPool $pool;

    public function __construct(
        UtopiaPool $pool,
        int $workerNum,
        string $queue,
        string $namespace = 'utopia-queue'
    )
    {
        parent::__construct($workerNum, $queue, $namespace);

        $this->pool = $pool;
    }

    protected function delegate(string $method, array $args): mixed
    {
        return $this->pool->use(function (Adapter $adapter) use ($method, $args) {
            return $adapter->$method(...$args);
        });
    }

    public function start(): self
    {
        return $this->delegate(__FUNCTION__, func_get_args());
    }

    public function stop(): self
    {
        return $this->delegate(__FUNCTION__, func_get_args());
    }

    public function workerStart(callable $callback): self
    {
        return $this->delegate(__FUNCTION__, func_get_args());
    }

    public function workerStop(callable $callback): self
    {
        return $this->delegate(__FUNCTION__, func_get_args());
    }

    public function getNative(): mixed
    {
        return $this->delegate(__FUNCTION__, func_get_args());
    }
}