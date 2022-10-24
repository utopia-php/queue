<?php

namespace Utopia\Queue\Connection;

use \Redis as Client;
use Utopia\Queue\Connection;
use Utopia\Queue\Message;

class Redis implements Connection
{
    /**
     * @var Client
     */
    protected Client $redis;

    /**
     * Redis constructor.
     *
     * @param  Client  $redis
     */
    public function __construct(Client $redis)
    {
        $this->redis = $redis;
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
        $response = $this->redis->bRPopLPush($queue, $destination, $timeout);

        if (!$response) {
            return false;
        }

        return $response;
    }
    public function rightPushArray(string $queue, array $value): bool
    {
        return !!$this->redis->rPush($queue, json_encode($value));
    }

    public function rightPush(string $queue, string $value): bool
    {
        return !!$this->redis->rPush($queue, $value);
    }

    public function leftPushArray(string $queue, array $value): bool
    {
        return !!$this->redis->lPush($queue, json_encode($value));
    }

    public function leftPush(string $queue, string $value): bool
    {
        return !!$this->redis->lPush($queue, $value);
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
        $response = $this->redis->brPop([$queue], $timeout);

        if (empty($response)) {
            return false;
        }

        return $response[1];
    }

    public function leftPopArray(string $queue, int $timeout): array|false
    {
        $response = $this->redis->blPop($queue, $timeout);

        if (empty($response)) {
            return false;
        }

        return json_decode($response[1], true);
    }

    public function leftPop(string $queue, int $timeout): string|false
    {
        $response = $this->redis->blPop($queue, $timeout);

        if (empty($response)) {
            return false;
        }

        return $response[1];
    }

    public function listRemove(string $queue, string $key): bool
    {
        return !!$this->redis->lRem($queue, $key, 1);
    }

    public function remove(string $key): bool
    {
        return !!$this->redis->del($key);
    }

    public function move(string $queue, string $destination): bool
    {
        return $this->redis->move($queue, $destination);
    }

    public function setArray(string $key, array $value): bool
    {
        return $this->set($key, json_encode($value));
    }

    public function set(string $key, string $value): bool
    {
        return $this->redis->set($key, $value);
    }

    public function get(string $key): array|string|null
    {
        return $this->redis->get($key);
    }

    public function listSize(string $key): int
    {
        return $this->redis->lLen($key);
    }

    public function increment(string $key): int
    {
        return $this->redis->incr($key);
    }

    public function decrement(string $key): int
    {
        return $this->redis->decr($key);
    }

    public function listRange(string $key, int $total, int $offset): array
    {
        $start = $offset - 1;
        $end = ($total + $offset) -1;
        $results = $this->redis->lrange($key, $start, $end);

        return array_map(fn (array $job) => new Message($job), $results);
    }

}
