<?php

namespace Utopia\Queue\Concurrency;

class Redis implements Adapter
{
    private \Redis $redis;

    public function get(string $key): string|bool
    {
        return $this->redis->get($key);
    }

    public function set(string $key, int $value): bool
    {
        return $this->redis->set($key, $value);
    }

    public function increment(string $key, int $value = 1): bool
    {
        return $this->redis->increment($key, $value);
    }

    public function decrement(string $key, int $value = 1): bool
    {
        return $this->redis->decrement($key, $value);
    }
}
