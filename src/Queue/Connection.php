<?php

namespace Utopia\Queue;

/**
 * Utopia PHP Framework
 *
 * @package Utopia\Queue
 *
 * @link https://github.com/utopia-php/framework
 * @author Torsten Dittmann <torsten@appwrite.io>
 * @version 1.0 RC1
 * @license The MIT License (MIT) <http://www.opensource.org/licenses/mit-license.php>
 */
interface Connection
{
    public function rightPush(string $queue, array $payload): bool;
    public function rightPop(string $queue, int $timeout): array|false;
    public function rightPopLeftPush(string $queue, string $destination, int $timeout): array|false;
    public function leftPush(string $queue, array $payload): bool;
    public function leftPop(string $queue, int $timeout): array|false;
    public function remove(string $queue, string $key): bool;
}