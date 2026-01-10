<?php

namespace Utopia\Queue\Connection;

use Utopia\Queue\StreamConnection;

class RedisStream extends Redis implements StreamConnection
{
    /**
     * @inheritDoc
     */
    public function streamAdd(string $stream, array $fields, string $id = '*', ?int $maxLen = null, bool $approximate = false): string|false
    {
        $redis = $this->getRedis();

        if ($maxLen !== null) {
            // Use exact MAXLEN (approximate=false) by default for reliable trimming
            // Note: approximate trimming with phpredis may not trim immediately
            return $redis->xAdd($stream, $id, $fields, $maxLen, $approximate);
        }

        return $redis->xAdd($stream, $id, $fields);
    }

    /**
     * @inheritDoc
     */
    public function streamCreateGroup(string $stream, string $group, string $id = '0', bool $mkstream = true): bool
    {
        $redis = $this->getRedis();

        try {
            $result = $redis->xGroup('CREATE', $stream, $group, $id, $mkstream);
            // phpredis may return false instead of throwing on BUSYGROUP
            if ($result === false) {
                $error = $redis->getLastError();
                $redis->clearLastError();
                if ($error !== null && str_contains($error, 'BUSYGROUP')) {
                    return true;
                }
                return false;
            }
            return (bool)$result;
        } catch (\RedisException $e) {
            // Group already exists - BUSYGROUP error
            if (str_contains($e->getMessage(), 'BUSYGROUP')) {
                return true;
            }
            throw $e;
        }
    }

    /**
     * @inheritDoc
     */
    public function streamDestroyGroup(string $stream, string $group): bool
    {
        $redis = $this->getRedis();

        try {
            $result = $redis->xGroup('DESTROY', $stream, $group);
            // phpredis may return false instead of throwing on errors
            if ($result === false) {
                $error = $redis->getLastError();
                $redis->clearLastError();
                // Stream doesn't exist or group doesn't exist - treat as success (already destroyed)
                if ($error !== null && (
                    str_contains($error, 'NOGROUP') ||
                    str_contains($error, 'no such key') ||
                    str_contains($error, 'key to exist')
                )) {
                    return true;
                }
                return false;
            }
            // Result of 0 means the group didn't exist - that's fine, it's "destroyed"
            return true;
        } catch (\RedisException $e) {
            // Group doesn't exist
            if (str_contains($e->getMessage(), 'NOGROUP')) {
                return true;
            }
            throw $e;
        }
    }

    /**
     * @inheritDoc
     */
    public function streamDeleteConsumer(string $stream, string $group, string $consumer): int
    {
        try {
            return $this->getRedis()->xGroup('DELCONSUMER', $stream, $group, $consumer);
        } catch (\RedisException $e) {
            // Group doesn't exist
            if (str_contains($e->getMessage(), 'NOGROUP')) {
                return 0;
            }
            throw $e;
        }
    }

    /**
     * @inheritDoc
     */
    public function streamReadGroup(
        string $group,
        string $consumer,
        array $streams,
        int $count = 1,
        int $block = 0,
        bool $noack = false
    ): array|false {
        $streamIds = [];
        foreach ($streams as $stream) {
            $streamIds[$stream] = '>'; // Read only new messages
        }

        // Build options array for xReadGroup
        $options = [];
        if ($noack) {
            $options['NOACK'] = true;
        }

        $redis = $this->getRedis();

        // phpredis doesn't support NOACK in xReadGroup directly, so we need to use rawCommand
        if ($noack) {
            // Build the command manually for NOACK support
            $command = ['XREADGROUP', 'GROUP', $group, $consumer];
            if ($count > 0) {
                $command[] = 'COUNT';
                $command[] = (string)$count;
            }
            if ($block > 0) {
                $command[] = 'BLOCK';
                $command[] = (string)$block;
            }
            $command[] = 'NOACK';
            $command[] = 'STREAMS';

            foreach ($streamIds as $stream => $id) {
                $command[] = $stream;
            }
            foreach ($streamIds as $stream => $id) {
                $command[] = $id;
            }

            try {
                $result = $redis->rawCommand(...$command);
                return $result ?: false;
            } catch (\RedisException $e) {
                return false;
            }
        }

        $result = $redis->xReadGroup(
            $group,
            $consumer,
            $streamIds,
            $count,
            $block > 0 ? $block : null
        );

        return $result ?: false;
    }

    /**
     * @inheritDoc
     */
    public function streamAck(string $stream, string $group, string|array $ids): int
    {
        $ids = is_array($ids) ? $ids : [$ids];
        return $this->getRedis()->xAck($stream, $group, $ids);
    }

    /**
     * @inheritDoc
     */
    public function streamPendingSummary(string $stream, string $group): array
    {
        try {
            return $this->getRedis()->xPending($stream, $group) ?: [];
        } catch (\RedisException $e) {
            if (str_contains($e->getMessage(), 'NOGROUP')) {
                return [];
            }
            throw $e;
        }
    }

    /**
     * @inheritDoc
     */
    public function streamPending(
        string $stream,
        string $group,
        string $start = '-',
        string $end = '+',
        int $count = 100,
        ?string $consumer = null
    ): array {
        try {
            if ($consumer !== null) {
                return $this->getRedis()->xPending($stream, $group, $start, $end, $count, $consumer) ?: [];
            }
            return $this->getRedis()->xPending($stream, $group, $start, $end, $count) ?: [];
        } catch (\RedisException $e) {
            if (str_contains($e->getMessage(), 'NOGROUP')) {
                return [];
            }
            throw $e;
        }
    }

    /**
     * @inheritDoc
     */
    public function streamClaim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleTime,
        array $ids,
        bool $justId = false
    ): array {
        try {
            $options = $justId ? ['JUSTID'] : [];
            return $this->getRedis()->xClaim($stream, $group, $consumer, $minIdleTime, $ids, $options) ?: [];
        } catch (\RedisException $e) {
            if (str_contains($e->getMessage(), 'NOGROUP')) {
                return [];
            }
            throw $e;
        }
    }

    /**
     * @inheritDoc
     */
    public function streamAutoClaim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleTime,
        string $start = '0-0',
        int $count = 100
    ): array {
        try {
            $result = $this->getRedis()->xAutoClaim($stream, $group, $consumer, $minIdleTime, $start, $count);
            return $result ?: ['0-0', [], []];
        } catch (\RedisException $e) {
            if (str_contains($e->getMessage(), 'NOGROUP')) {
                return ['0-0', [], []];
            }
            throw $e;
        }
    }

    /**
     * @inheritDoc
     */
    public function streamDel(string $stream, array $ids): int
    {
        if (empty($ids)) {
            return 0;
        }
        return $this->getRedis()->xDel($stream, $ids);
    }

    /**
     * @inheritDoc
     */
    public function streamLen(string $stream): int
    {
        return $this->getRedis()->xLen($stream);
    }

    /**
     * @inheritDoc
     */
    public function streamTrim(string $stream, int $maxLen, bool $approximate = true): int
    {
        return $this->getRedis()->xTrim($stream, $maxLen, $approximate);
    }

    /**
     * @inheritDoc
     */
    public function streamInfo(string $stream): array
    {
        try {
            return $this->getRedis()->xInfo('STREAM', $stream) ?: [];
        } catch (\RedisException $e) {
            // Stream doesn't exist yet
            return [];
        }
    }

    /**
     * @inheritDoc
     */
    public function streamGroupInfo(string $stream): array
    {
        try {
            return $this->getRedis()->xInfo('GROUPS', $stream) ?: [];
        } catch (\RedisException $e) {
            // Stream doesn't exist yet
            return [];
        }
    }

    /**
     * @inheritDoc
     */
    public function streamConsumersInfo(string $stream, string $group): array
    {
        try {
            return $this->getRedis()->xInfo('CONSUMERS', $stream, $group) ?: [];
        } catch (\RedisException $e) {
            return [];
        }
    }

    /**
     * @inheritDoc
     */
    public function streamRange(string $stream, string $start = '-', string $end = '+', ?int $count = null): array
    {
        if ($count !== null) {
            return $this->getRedis()->xRange($stream, $start, $end, $count) ?: [];
        }
        return $this->getRedis()->xRange($stream, $start, $end) ?: [];
    }

    /**
     * @inheritDoc
     */
    public function streamRevRange(string $stream, string $end = '+', string $start = '-', ?int $count = null): array
    {
        if ($count !== null) {
            return $this->getRedis()->xRevRange($stream, $end, $start, $count) ?: [];
        }
        return $this->getRedis()->xRevRange($stream, $end, $start) ?: [];
    }

    /**
     * @inheritDoc
     */
    public function sortedSetAdd(string $key, float $score, string $member): int
    {
        return $this->getRedis()->zAdd($key, $score, $member);
    }

    /**
     * @inheritDoc
     */
    public function sortedSetPopByScore(string $key, float $min, float $max, int $limit = 100): array
    {
        $redis = $this->getRedis();

        // Limit to prevent Lua stack overflow (unpack has ~8000 item limit)
        if ($limit > 5000) {
            $limit = 5000;
        }

        // Use Lua script for atomic pop by score
        $script = <<<'LUA'
local members = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'LIMIT', 0, ARGV[3])
if #members > 0 then
    redis.call('ZREM', KEYS[1], unpack(members))
end
return members
LUA;

        $result = $redis->eval($script, [$key, (string)$min, (string)$max, (string)$limit], 1);
        return $result ?: [];
    }

    /**
     * @inheritDoc
     */
    public function sortedSetRangeByScore(string $key, float $min, float $max, ?int $limit = null): array
    {
        $options = [];
        if ($limit !== null) {
            $options['limit'] = [0, $limit];
        }
        return $this->getRedis()->zRangeByScore($key, (string)$min, (string)$max, $options) ?: [];
    }

    /**
     * @inheritDoc
     */
    public function sortedSetSize(string $key): int
    {
        return $this->getRedis()->zCard($key);
    }

    /**
     * @inheritDoc
     */
    public function sortedSetRemove(string $key, string $member): int
    {
        return $this->getRedis()->zRem($key, $member);
    }

    /**
     * @inheritDoc
     */
    public function sortedSetScore(string $key, string $member): float|false
    {
        return $this->getRedis()->zScore($key, $member);
    }

    /**
     * @inheritDoc
     */
    public function hashSet(string $key, string $field, string $value): bool
    {
        return $this->getRedis()->hSet($key, $field, $value) !== false;
    }

    /**
     * @inheritDoc
     */
    public function hashGet(string $key, string $field): string|false
    {
        return $this->getRedis()->hGet($key, $field);
    }

    /**
     * @inheritDoc
     */
    public function hashGetAll(string $key): array
    {
        return $this->getRedis()->hGetAll($key) ?: [];
    }

    /**
     * @inheritDoc
     */
    public function hashDel(string $key, string $field): int
    {
        return $this->getRedis()->hDel($key, $field);
    }

    /**
     * @inheritDoc
     */
    public function hashExists(string $key, string $field): bool
    {
        return $this->getRedis()->hExists($key, $field);
    }

    /**
     * @inheritDoc
     */
    public function hashLen(string $key): int
    {
        return $this->getRedis()->hLen($key);
    }
}
