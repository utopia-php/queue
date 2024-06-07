<?php

namespace Utopia\Queue\Adapter\Swoole;

use Swoole\Database\RedisConfig;
use Swoole\Database\RedisPool;
use Utopia\Queue\Connection\Redis as ConnectionRedis;

class Redis extends ConnectionRedis
{
    protected RedisPool $pool;

    protected int $poolSize = 5;

    public function __construct(string $host, int $port = 6379, ?string $user = null, ?string $password = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->password = $password;

        $auth = (empty($this->user) || empty($this->password)) ? $this->user.$this->password : implode(':', array_filter([$this->user, $this->password]));

        $this->pool = new RedisPool((new RedisConfig())
            ->withHost($this->host)
            ->withPort($this->port)
            ->withAuth($auth), $this->poolSize);
    }

    public function getConnection(): void
    {
        $this->redis = $this->pool->get();
    }

    public function putConnection(): void
    {
        if (is_null($this->redis)) {
            return;
        }

        $this->pool->put($this->redis);
    }

    protected function getRedis(): \Redis
    {
        if (empty($this->redis)) {
            $this->redis = $this->pool->get();
        }

        return $this->redis;
    }
}
