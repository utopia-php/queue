<?php

namespace Utopia\Queue\Adapter\Swoole;

use Swoole\Database\RedisConfig;
use Swoole\Database\RedisPool;
use Utopia\Queue\Connection\Redis as ConnectionRedis;

class Redis extends ConnectionRedis
{
    protected RedisPool $pool;

    protected int $poolSize = 50;

    public function __construct(string $host, int $port = 6379, ?string $user = null, ?string $password = null)
    {
        parent::__construct($host, $port, $user, $password);

        $this->pool = new RedisPool((new RedisConfig())
            ->withHost($this->host)
            ->withPort((int)$this->port)
            ->withAuth((string)$this->password)
        , $this->poolSize);
    }

    public function getConnection(): void
    {
        $this->redis = $this->pool->get();
    }

    public function putConnection(): void
    {
        if(is_null($this->redis)) {
            return;
        }

        $this->pool->put($this->redis);
    }
}