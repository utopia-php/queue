<?php

namespace Utopia\Queue;

interface StreamConnection extends Connection
{
    /**
     * Add entry to a stream (XADD)
     *
     * @param string $stream Stream key
     * @param array $fields Key-value pairs to add
     * @param string $id Entry ID ('*' for auto-generate)
     * @param int|null $maxLen MAXLEN for stream trimming
     * @param bool $approximate Use approximate (~) trimming (default: false for exact)
     * @return string|false Entry ID on success, false on failure
     */
    public function streamAdd(string $stream, array $fields, string $id = '*', ?int $maxLen = null, bool $approximate = false): string|false;

    /**
     * Create a consumer group (XGROUP CREATE)
     *
     * @param string $stream Stream key
     * @param string $group Consumer group name
     * @param string $id Starting ID ('0' for beginning, '$' for new messages only)
     * @param bool $mkstream Create stream if not exists
     * @return bool
     */
    public function streamCreateGroup(string $stream, string $group, string $id = '0', bool $mkstream = true): bool;

    /**
     * Destroy a consumer group (XGROUP DESTROY)
     *
     * @param string $stream Stream key
     * @param string $group Consumer group name
     * @return bool
     */
    public function streamDestroyGroup(string $stream, string $group): bool;

    /**
     * Delete a consumer from a group (XGROUP DELCONSUMER)
     *
     * @param string $stream Stream key
     * @param string $group Consumer group name
     * @param string $consumer Consumer name
     * @return int Number of pending messages that were owned by the consumer
     */
    public function streamDeleteConsumer(string $stream, string $group, string $consumer): int;

    /**
     * Read from stream as consumer group (XREADGROUP)
     *
     * @param string $group Consumer group name
     * @param string $consumer Consumer name
     * @param array $streams Array of stream keys
     * @param int $count Max entries to return
     * @param int $block Block timeout in milliseconds (0 = no block)
     * @param bool $noack Auto-acknowledge messages
     * @return array|false Array of entries or false
     */
    public function streamReadGroup(
        string $group,
        string $consumer,
        array $streams,
        int $count = 1,
        int $block = 0,
        bool $noack = false
    ): array|false;

    /**
     * Acknowledge message (XACK)
     *
     * @param string $stream Stream key
     * @param string $group Consumer group name
     * @param string|array $ids Entry ID(s) to acknowledge
     * @return int Number of acknowledged entries
     */
    public function streamAck(string $stream, string $group, string|array $ids): int;

    /**
     * Get pending messages summary (XPENDING)
     *
     * @param string $stream Stream key
     * @param string $group Consumer group name
     * @return array Pending summary [count, first-id, last-id, [[consumer, count], ...]]
     */
    public function streamPendingSummary(string $stream, string $group): array;

    /**
     * Get pending messages details (XPENDING with range)
     *
     * @param string $stream Stream key
     * @param string $group Consumer group name
     * @param string $start Start ID ('-' for minimum)
     * @param string $end End ID ('+' for maximum)
     * @param int $count Max entries
     * @param string|null $consumer Filter by consumer
     * @return array Pending entries [[id, consumer, idle_time, delivery_count], ...]
     */
    public function streamPending(
        string $stream,
        string $group,
        string $start = '-',
        string $end = '+',
        int $count = 100,
        ?string $consumer = null
    ): array;

    /**
     * Claim pending messages (XCLAIM)
     *
     * @param string $stream Stream key
     * @param string $group Consumer group name
     * @param string $consumer Consumer claiming the messages
     * @param int $minIdleTime Minimum idle time in milliseconds
     * @param array $ids Entry IDs to claim
     * @param bool $justId Return just IDs without data
     * @return array Claimed entries
     */
    public function streamClaim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleTime,
        array $ids,
        bool $justId = false
    ): array;

    /**
     * Auto-claim pending messages (XAUTOCLAIM) - Redis 6.2+
     *
     * @param string $stream Stream key
     * @param string $group Consumer group name
     * @param string $consumer Consumer claiming the messages
     * @param int $minIdleTime Minimum idle time in milliseconds
     * @param string $start Start ID
     * @param int $count Max entries
     * @return array [next_start_id, claimed_entries, deleted_ids]
     */
    public function streamAutoClaim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleTime,
        string $start = '0-0',
        int $count = 100
    ): array;

    /**
     * Delete entries from stream (XDEL)
     *
     * @param string $stream Stream key
     * @param array $ids Entry IDs to delete
     * @return int Number of deleted entries
     */
    public function streamDel(string $stream, array $ids): int;

    /**
     * Get stream length (XLEN)
     *
     * @param string $stream Stream key
     * @return int Stream length
     */
    public function streamLen(string $stream): int;

    /**
     * Trim stream (XTRIM)
     *
     * @param string $stream Stream key
     * @param int $maxLen Maximum length
     * @param bool $approximate Use ~ for approximate trimming
     * @return int Number of trimmed entries
     */
    public function streamTrim(string $stream, int $maxLen, bool $approximate = true): int;

    /**
     * Get stream info (XINFO STREAM)
     *
     * @param string $stream Stream key
     * @return array Stream info
     */
    public function streamInfo(string $stream): array;

    /**
     * Get consumer group info (XINFO GROUPS)
     *
     * @param string $stream Stream key
     * @return array Groups info
     */
    public function streamGroupInfo(string $stream): array;

    /**
     * Get consumers info (XINFO CONSUMERS)
     *
     * @param string $stream Stream key
     * @param string $group Consumer group name
     * @return array Consumers info
     */
    public function streamConsumersInfo(string $stream, string $group): array;

    /**
     * Read stream entries (XRANGE)
     *
     * @param string $stream Stream key
     * @param string $start Start ID ('-' for minimum)
     * @param string $end End ID ('+' for maximum)
     * @param int|null $count Max entries
     * @return array Stream entries
     */
    public function streamRange(string $stream, string $start = '-', string $end = '+', ?int $count = null): array;

    /**
     * Read stream entries in reverse (XREVRANGE)
     *
     * @param string $stream Stream key
     * @param string $end End ID ('+' for maximum)
     * @param string $start Start ID ('-' for minimum)
     * @param int|null $count Max entries
     * @return array Stream entries
     */
    public function streamRevRange(string $stream, string $end = '+', string $start = '-', ?int $count = null): array;

    /**
     * Add to sorted set (ZADD)
     *
     * @param string $key Sorted set key
     * @param float $score Score (timestamp for delayed jobs)
     * @param string $member Member value
     * @return int 1 if added, 0 if updated
     */
    public function sortedSetAdd(string $key, float $score, string $member): int;

    /**
     * Get and remove members by score range (ZRANGEBYSCORE + ZREM) - atomic via Lua
     *
     * @param string $key Sorted set key
     * @param float $min Minimum score
     * @param float $max Maximum score
     * @param int $limit Max members to return
     * @return array Members that were removed
     */
    public function sortedSetPopByScore(string $key, float $min, float $max, int $limit = 100): array;

    /**
     * Get members by score range without removing (ZRANGEBYSCORE)
     *
     * @param string $key Sorted set key
     * @param float $min Minimum score
     * @param float $max Maximum score
     * @param int|null $limit Max members to return
     * @return array Members in range
     */
    public function sortedSetRangeByScore(string $key, float $min, float $max, ?int $limit = null): array;

    /**
     * Get sorted set size (ZCARD)
     *
     * @param string $key Sorted set key
     * @return int Set cardinality
     */
    public function sortedSetSize(string $key): int;

    /**
     * Remove from sorted set (ZREM)
     *
     * @param string $key Sorted set key
     * @param string $member Member to remove
     * @return int 1 if removed, 0 if not found
     */
    public function sortedSetRemove(string $key, string $member): int;

    /**
     * Get score of a member (ZSCORE)
     *
     * @param string $key Sorted set key
     * @param string $member Member
     * @return float|false Score or false if not found
     */
    public function sortedSetScore(string $key, string $member): float|false;

    /**
     * Set hash field (HSET)
     *
     * @param string $key Hash key
     * @param string $field Field name
     * @param string $value Field value
     * @return bool
     */
    public function hashSet(string $key, string $field, string $value): bool;

    /**
     * Get hash field (HGET)
     *
     * @param string $key Hash key
     * @param string $field Field name
     * @return string|false Value or false if not found
     */
    public function hashGet(string $key, string $field): string|false;

    /**
     * Get all hash fields (HGETALL)
     *
     * @param string $key Hash key
     * @return array Key-value pairs
     */
    public function hashGetAll(string $key): array;

    /**
     * Delete hash field (HDEL)
     *
     * @param string $key Hash key
     * @param string $field Field name
     * @return int 1 if deleted, 0 if not found
     */
    public function hashDel(string $key, string $field): int;

    /**
     * Check if hash field exists (HEXISTS)
     *
     * @param string $key Hash key
     * @param string $field Field name
     * @return bool
     */
    public function hashExists(string $key, string $field): bool;

    /**
     * Get hash size (HLEN)
     *
     * @param string $key Hash key
     * @return int Number of fields
     */
    public function hashLen(string $key): int;
}
