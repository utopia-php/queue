<?php

namespace Utopia\Queue\Connection;

use Utopia\Queue\Connection;

class RedisCluster implements Connection
{
    protected array $seeds;
    protected ?\RedisCluster $redis = null;

    public function __construct(array $seeds)
    {
        $this->seeds = $seeds;
    }

    public function rightPopLeftPushArray(string $queue, string $destination, int $timeout): array|false
    {
        $response = $this->rightPopLeftPush($queue, $destination, $timeout);

        if (!$response) {
            return false;
        }

        return json_decode($response, true);
    }
    public function rightPopLeftPush(string $queue, string $destination, int $timeout): string|false
    {
        $response = $this->getRedis()->bRPopLPush($queue, $destination, $timeout);

        if (!$response) {
            return false;
        }

        return $response;
    }
    public function rightPushArray(string $queue, array $value): bool
    {
        return !!$this->getRedis()->rPush($queue, json_encode($value));
    }

    public function rightPush(string $queue, string $value): bool
    {
        return !!$this->getRedis()->rPush($queue, $value);
    }

    public function leftPushArray(string $queue, array $value): bool
    {
        return !!$this->getRedis()->lPush($queue, json_encode($value));
    }

    public function leftPush(string $queue, string $value): bool
    {
        return !!$this->getRedis()->lPush($queue, $value);
    }

    public function rightPopArray(string $queue, int $timeout): array|false
    {
        $response = $this->rightPop($queue, $timeout);

        if ($response === false) {
            return false;
        }

        return json_decode($response, true) ?? false;
    }

    public function rightPop(string $queue, int $timeout): string|false
    {
        $response = $this->getRedis()->brPop([$queue], $timeout);

        if (empty($response)) {
            return false;
        }

        return $response[1];
    }

    public function leftPopArray(string $queue, int $timeout): array|false
    {
        $response = $this->getRedis()->blPop($queue, $timeout);

        if (empty($response)) {
            return false;
        }

        return json_decode($response[1], true) ?? false;
    }

    public function leftPop(string $queue, int $timeout): string|false
    {
        $response = $this->getRedis()->blPop($queue, $timeout);

        if (empty($response)) {
            return false;
        }

        return $response[1];
    }

    public function listRemove(string $queue, string $key): bool
    {
        return !!$this->getRedis()->lRem($queue, $key, 1);
    }

    public function remove(string $key): bool
    {
        return !!$this->getRedis()->del($key);
    }

    public function move(string $queue, string $destination): bool
    {
        // Move is not supported for Redis Cluster
        return false;
    }

    public function setArray(string $key, array $value): bool
    {
        return $this->set($key, json_encode($value));
    }

    public function set(string $key, string $value): bool
    {
        return $this->getRedis()->set($key, $value);
    }

    public function get(string $key): array|string|null
    {
        return $this->getRedis()->get($key);
    }

    public function listSize(string $key): int
    {
        return $this->getRedis()->lLen($key);
    }

    public function increment(string $key): int
    {
        return $this->getRedis()->incr($key);
    }

    public function decrement(string $key): int
    {
        return $this->getRedis()->decr($key);
    }

    public function listRange(string $key, int $total, int $offset): array
    {
        $start = $offset;
        $end = $start + $total - 1;
        $results = $this->getRedis()->lRange($key, $start, $end);

        return $results;
    }

    public function ping(): bool
    {
        try {
            foreach ($this->getRedis()->_masters() as $master) {
                $this->getRedis()->ping($master);
            }

            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    protected function getRedis(): \RedisCluster
    {
        if ($this->redis) {
            return $this->redis;
        }

        $this->redis = new \RedisCluster(null, $this->seeds);
        return $this->redis;
    }
}
