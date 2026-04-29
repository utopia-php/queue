<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Broker\Redis as RedisBroker;
use Utopia\Queue\Connection;
use Utopia\Queue\Queue;

class RedisReconnectCallbackTest extends TestCase
{
    public function testReconnectCallbackReceivesAttemptContext(): void
    {
        $queue = new Queue('reconnect-callback');
        $connection = new FailingRedisConnection();
        $broker = new RedisBroker($connection);
        $calls = [];

        $broker->setReconnectCallback(function (Queue $queue, \Throwable $error, int $attempt, int $sleepMs) use (&$calls, $broker): void {
            $calls[] = [
                'queue' => $queue,
                'error' => $error,
                'attempt' => $attempt,
                'sleepMs' => $sleepMs,
            ];

            $broker->close();
        });

        $broker->consume(
            $queue,
            fn () => null,
            fn () => null,
            fn () => null,
        );

        $this->assertSame(1, $connection->popAttempts);
        $this->assertCount(1, $calls);
        $this->assertSame($queue, $calls[0]['queue']);
        $this->assertInstanceOf(\RedisException::class, $calls[0]['error']);
        $this->assertSame(1, $calls[0]['attempt']);
        $this->assertIsInt($calls[0]['sleepMs']);
        $this->assertGreaterThanOrEqual(0, $calls[0]['sleepMs']);
        $this->assertLessThanOrEqual(100, $calls[0]['sleepMs']);
    }

    public function testReconnectSuccessCallbackReceivesAttemptCount(): void
    {
        $queue = new Queue('reconnect-success-callback');
        $connection = new RecoveringRedisConnection();
        $broker = new RedisBroker($connection);
        $calls = [];

        $broker->setReconnectCallback(fn () => null);
        $broker->setReconnectSuccessCallback(function (Queue $queue, int $attempts) use (&$calls, $broker): void {
            $calls[] = [
                'queue' => $queue,
                'attempts' => $attempts,
            ];

            $broker->close();
        });

        $broker->consume(
            $queue,
            fn () => null,
            fn () => null,
            fn () => null,
        );

        $this->assertSame(2, $connection->popAttempts);
        $this->assertCount(1, $calls);
        $this->assertSame($queue, $calls[0]['queue']);
        $this->assertSame(1, $calls[0]['attempts']);
    }
}

class FailingRedisConnection implements Connection
{
    public int $popAttempts = 0;

    public function rightPushArray(string $queue, array $payload): bool
    {
        return true;
    }

    public function rightPopArray(string $queue, int $timeout): array|false
    {
        $this->popAttempts++;

        throw new \RedisException('Redis is unavailable.');
    }

    public function rightPopLeftPushArray(string $queue, string $destination, int $timeout): array|false
    {
        return false;
    }

    public function leftPushArray(string $queue, array $payload): bool
    {
        return true;
    }

    public function leftPopArray(string $queue, int $timeout): array|false
    {
        return false;
    }

    public function rightPush(string $queue, string $payload): bool
    {
        return true;
    }

    public function rightPop(string $queue, int $timeout): string|false
    {
        return false;
    }

    public function rightPopLeftPush(string $queue, string $destination, int $timeout): string|false
    {
        return false;
    }

    public function leftPush(string $queue, string $payload): bool
    {
        return true;
    }

    public function leftPop(string $queue, int $timeout): string|false
    {
        return false;
    }

    public function listRemove(string $queue, string $key): bool
    {
        return true;
    }

    public function listSize(string $key): int
    {
        return 0;
    }

    public function listRange(string $key, int $total, int $offset): array
    {
        return [];
    }

    public function remove(string $key): bool
    {
        return true;
    }

    public function move(string $queue, string $destination): bool
    {
        return true;
    }

    public function set(string $key, string $value, int $ttl = 0): bool
    {
        return true;
    }

    public function get(string $key): array|string|null
    {
        return null;
    }

    public function setArray(string $key, array $value, int $ttl = 0): bool
    {
        return true;
    }

    public function increment(string $key): int
    {
        return 1;
    }

    public function decrement(string $key): int
    {
        return 0;
    }

    public function ping(): bool
    {
        return false;
    }

    public function close(): void
    {
    }
}

class RecoveringRedisConnection extends FailingRedisConnection
{
    public function rightPopArray(string $queue, int $timeout): array|false
    {
        $this->popAttempts++;

        if ($this->popAttempts === 1) {
            throw new \RedisException('Redis is unavailable.');
        }

        return false;
    }
}
