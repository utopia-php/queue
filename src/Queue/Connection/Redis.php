<?php

namespace Utopia\Queue\Connection;

use Swoole\Coroutine\Redis as SwooleRedis;
use Utopia\Queue\Connection;
use Utopia\Queue\Job;

class Redis implements Connection
{
    protected string $host;
    protected int $port;
    protected ?string $user;
    protected ?string $password;
    protected ?SwooleRedis $redis = null;

    public function __construct(string $host, int $port = 6379, ?string $user = null, ?string $password = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->password = $password;
    }

    public function rightPopLeftPushArray(string $queue, string $destination, int $timeout): array|false
    {
        $response = $this->rightPopLeftPush($queue, $destination, $timeout);

        if (!$response){
            return false;
        }

        return json_decode($response, true);
    }
    public function rightPopLeftPush(string $queue, string $destination, int $timeout): string|false
    {
        $response = $this->getRedis()->bRPopLPush($queue, $destination, $timeout);

        if (($response ?? false) === false) {
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

        return json_decode($response, true);
    }

    public function rightPop(string $queue, int $timeout): string|false
    {
        $response = $this->getRedis()->brPop($queue, $timeout);

        if (($response ?? false) === false) {
            return false;
        }
        return $response[1];
    }

    public function leftPopArray(string $queue, int $timeout): array|false
    {
        $response = $this->getRedis()->blPop($queue, $timeout);

        if ($response === false) {
            return false;
        }

        return json_decode($response, true);
    }

    public function leftPop(string $queue, int $timeout): string|false
    {
        $response = $this->getRedis()->blPop($queue, $timeout);

        if (($response ?? false) === false) {
            return false;
        }

        return $response[1];
    }

    public function listRemove(string $queue, string $key): bool
    {
        return !!$this->getRedis()->lRemove($queue, $key, 1);
    }

    public function remove(string $key): bool
    {
        return !!$this->getRedis()->del($key);
    }

    public function move(string $queue, string $destination): bool
    {
        return $this->getRedis()->move($queue, $destination);
    }

    public function setArray(string $key, array $value): bool
    {
        return $this->set($key, json_encode($value));
    }

    public function set(string $key, string $value): bool
    {
        return $this->getRedis()->set($key, $value);
    }

    public function get(string $key): array
    {
        return $this->getRedis()->get($key);
    }

    public function listSize(string $key): int
    {
        return $this->getRedis()->lSize($key);
    }

    public function listRange(string $key, int $total, int $offset): array
    {
        $start = $offset - 1;
        $end = ($total + $offset) -1;
        $results = $this->getRedis()->lrange($key, $start, $end);

        return array_map(fn(array $job) => new Job($job), $results);
    }

    protected function getRedis(): SwooleRedis
    {
        if ($this->redis) {
            return $this->redis;
        }

        $this->redis = new SwooleRedis([
            'user' => $this->user,
            'password' => $this->password
        ]);

        $this->redis->connect($this->host, $this->port);

        return $this->getRedis();
    }
}