<?php

namespace Utopia\Queue;

/**
 * Utopia PHP Queue
 *
 * @package Utopia\Queue
 *
 * @link https://github.com/utopia-php/queue
 * @author Torsten Dittmann <torsten@appwrite.io>
 * @version 1.0 RC1
 * @license The MIT License (MIT) <http://www.opensource.org/licenses/mit-license.php>
 */
interface Connection
{
    public function rightPushArray(string $queue, array $payload): bool;
    public function rightPopArray(string $queue, int $timeout): array|false;
    public function rightPopLeftPushArray(string $queue, string $destination, int $timeout): array|false;
    public function leftPushArray(string $queue, array $payload): bool;
    public function leftPopArray(string $queue, int $timeout): array|false;
    public function rightPush(string $queue, string $payload): bool;
    public function rightPop(string $queue, int $timeout): string|false;
    public function rightPopLeftPush(string $queue, string $destination, int $timeout): string|false;
    public function leftPush(string $queue, string $payload): bool;
    public function leftPop(string $queue, int $timeout): string|false;
    public function listRemove(string $queue, string $key): bool;
    public function listSize(string $key): int;
    public function listRange(string $key, int $total, int $offset): array;
    public function remove(string $key): bool;
    public function move(string $queue, string $destination): bool;
    public function set(string $key, string $value): bool;
    public function get(string $key): array|string|null;
    public function setArray(string $key, array $value): bool;
    public function increment(string $key): int;
    public function decrement(string $key): int;
}
