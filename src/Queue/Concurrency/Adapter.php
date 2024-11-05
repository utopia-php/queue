<?php

namespace Utopia\Queue\Concurrency;

interface Adapter
{
    public function get(string $key): ?string;
    public function set(string $key, string $value): void;
    public function increment(string $key): int;
    public function decrement(string $key): int;
}
