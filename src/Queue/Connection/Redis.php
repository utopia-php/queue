<?php

namespace Utopia\Queue\Connection;

use Utopia\Queue\Connection;

class Redis implements Connection
{
    protected string $host;
    protected int $port;
    protected ?string $user;
    protected ?string $password;
    protected float $connectTimeout;
    protected float $readTimeout;
    protected ?\Redis $redis = null;

    /**
     * @param string      $host           Redis host.
     * @param int         $port           Redis port.
     * @param string|null $user           Redis ACL username (optional).
     * @param string|null $password       Redis password (optional).
     * @param float       $connectTimeout Connection timeout in seconds (0 = no timeout).
     * @param float       $readTimeout    Socket read timeout in seconds (-1 = infinite).
     *                                    Use -1 for consumers so blocking commands (BRPOP/BLPOP)
     *                                    are not interrupted; the per-call blockingReadTimeout()
     *                                    helper adds a safety buffer automatically.
     *                                    Use a positive value (e.g. 5) for publishers so a hung
     *                                    Redis fails fast rather than blocking indefinitely.
     */
    public function __construct(string $host, int $port = 6379, ?string $user = null, ?string $password = null, float $connectTimeout = 5, float $readTimeout = -1)
    {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->password = $password;
        $this->connectTimeout = $connectTimeout;
        $this->readTimeout = $readTimeout;
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
        $redis = $this->getRedis();
        $prev = $redis->getOption(\Redis::OPT_READ_TIMEOUT);
        $redis->setOption(\Redis::OPT_READ_TIMEOUT, $this->blockingReadTimeout($timeout));
        try {
            $response = $redis->bRPopLPush($queue, $destination, $timeout);
        } catch (\RedisException $e) {
            $this->redis = null;
            throw $e;
        } finally {
            if ($this->redis) {
                $redis->setOption(\Redis::OPT_READ_TIMEOUT, $prev);
            }
        }

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
        $redis = $this->getRedis();
        $prev = $redis->getOption(\Redis::OPT_READ_TIMEOUT);
        $redis->setOption(\Redis::OPT_READ_TIMEOUT, $this->blockingReadTimeout($timeout));
        try {
            $response = $redis->brPop([$queue], $timeout);
        } catch (\RedisException $e) {
            $this->redis = null;
            throw $e;
        } finally {
            if ($this->redis) {
                $redis->setOption(\Redis::OPT_READ_TIMEOUT, $prev);
            }
        }

        if (empty($response)) {
            return false;
        }

        return $response[1];
    }

    public function leftPopArray(string $queue, int $timeout): array|false
    {
        $response = $this->leftPop($queue, $timeout);

        if ($response === false) {
            return false;
        }

        return json_decode($response, true) ?? false;
    }

    public function leftPop(string $queue, int $timeout): string|false
    {
        $redis = $this->getRedis();
        $prev = $redis->getOption(\Redis::OPT_READ_TIMEOUT);
        $redis->setOption(\Redis::OPT_READ_TIMEOUT, $this->blockingReadTimeout($timeout));
        try {
            $response = $redis->blPop($queue, $timeout);
        } catch (\RedisException $e) {
            $this->redis = null;
            throw $e;
        } finally {
            if ($this->redis) {
                $redis->setOption(\Redis::OPT_READ_TIMEOUT, $prev);
            }
        }

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
        return $this->getRedis()->move($queue, $destination);
    }

    public function setArray(string $key, array $value, int $ttl = 0): bool
    {
        return $this->set($key, json_encode($value), $ttl);
    }

    public function set(string $key, string $value, int $ttl = 0): bool
    {
        if ($ttl > 0) {
            return $this->getRedis()->setex($key, $ttl, $value);
        }
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
            $this->getRedis()->ping();

            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    public function close(): void
    {
        $this->redis?->close();
        $this->redis = null;
    }

    protected function getRedis(): \Redis
    {
        if ($this->redis) {
            return $this->redis;
        }

        $this->redis = new \Redis();

        $this->redis->connect($this->host, $this->port, $this->connectTimeout);
        $this->redis->setOption(\Redis::OPT_READ_TIMEOUT, $this->readTimeout);

        return $this->redis;
    }

    /**
     * Returns the read timeout to use for a blocking command.
     * Ensures the socket does not time out before Redis returns.
     * A $timeout of 0 means block indefinitely, so we use -1 (infinite).
     */
    private function blockingReadTimeout(int $timeout): float
    {
        if ($timeout <= 0) {
            return -1;
        }
        // Add 1s buffer so the socket outlasts the Redis-side block timeout.
        // Also respect an explicit readTimeout if it is already larger.
        if ($this->readTimeout < 0) {
            return -1;
        }
        return max((float)($timeout + 1), $this->readTimeout);
    }
}
