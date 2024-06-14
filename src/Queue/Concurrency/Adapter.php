<?php

namespace Utopia\Queue\Concurrency;

interface Adapter
{
    public function get(string $key): string|bool;
    public function set(string $key, int $value): bool;
    public function increment(string $key, int $value = 1): bool;
    public function decrement(string $key, int $value = 1): bool;
}
