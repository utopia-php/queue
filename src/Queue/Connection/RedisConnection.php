<?php

namespace Utopia\Queue\Connection;

use Swoole\Coroutine\Redis;
use Utopia\Queue\Connection;

class RedisConnection implements Connection
{
    protected string $host;
    protected int $port;
    protected ?string $user;
    protected ?string $password;
    protected ?Redis $redis = null;

    public function __construct(string $host, int $port = 6379, ?string $user = null, ?string $password = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->password = $password;
    }

    public function rightPopLeftPush(string $queue, string $destination, int $timeout): array|false
    {
        $response = $this->getRedis()->bRPopLPush($queue, $destination, $timeout);

        if (($response ?? false) === false) {
            return false;
        }

        return json_decode($response, true);
    }

    public function rightPush(string $queue, array $value): bool
    {
        return !!$this->getRedis()->rPush($queue, json_encode($value));
    }

    public function leftPush(string $queue, array $value): bool
    {
        return !!$this->getRedis()->lPush($queue, json_encode($value));
    }

    public function rightPop(string $queue, int $timeout): array|false
    {
        $response = $this->getRedis()->brPop($queue, $timeout);

        if (($response ?? false) === false) {
            return false;
        }

        return json_decode($response, true);
    }

    public function leftPop(string $queue, int $timeout): array|false
    {
        $response = $this->getRedis()->blPop($queue, $timeout);

        if (($response ?? false) === false) {
            return false;
        }

        return json_decode($response, true);
    }

    public function remove(string $queue, string $key): bool
    {
        return !!$this->getRedis()->lRemove($queue, $key, 1);
    }

    protected function getRedis(): Redis
    {
        if ($this->redis) {
            return $this->redis;
        }

        $this->redis = new Redis([
            'user' => $this->user,
            'password' => $this->password
        ]);

        $this->redis->connect($this->host, $this->port);

        return $this->getRedis();
    }
}