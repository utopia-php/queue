<?php

/**
 * Stub file for RedisCluster Redis Streams methods
 * This file provides type hints for PHPStan static analysis
 * These methods exist in the phpredis extension but may not be in all stub libraries
 */

class RedisCluster
{
    /**
     * Adds a message to a stream
     *
     * @param string $key Stream key
     * @param string $id Entry ID (usually '*' for auto-generation)
     * @param array $fields Key-value pairs for the stream entry
     * @param int $maxlen Maximum stream length (0 = unlimited)
     * @param bool $approx Whether to use approximate trimming (~)
     * @return string|false The entry ID on success, false on failure
     */
    public function xAdd(string $key, string $id, array $fields, int $maxlen = 0, bool $approx = false): string|false {}

    /**
     * Manages consumer groups
     *
     * @param string $operation Operation: CREATE, DESTROY, SETID, CREATECONSUMER, DELCONSUMER
     * @param string|null $key Stream key
     * @param string|null $group Consumer group name
     * @param string|null $id_or_consumer Entry ID or consumer name depending on operation
     * @param bool $mkstream Whether to create the stream if it doesn't exist
     * @param int $entries_read Number of entries read (for SETID with ENTRIESREAD)
     * @return mixed Result varies by operation
     */
    public function xGroup(string $operation, ?string $key = null, ?string $group = null, ?string $id_or_consumer = null, bool $mkstream = false, int $entries_read = -2): mixed {}

    /**
     * Reads messages from a stream using a consumer group
     *
     * @param string $group Consumer group name
     * @param string $consumer Consumer name
     * @param array $streams Array of stream => ID pairs
     * @param int $count Maximum number of messages to return
     * @param int $block Block for N milliseconds (0 = don't block)
     * @return array|false Array of messages or false on failure
     */
    public function xReadGroup(string $group, string $consumer, array $streams, int $count = 1, int $block = 1): array|false {}

    /**
     * Acknowledges messages in a consumer group
     *
     * @param string $key Stream key
     * @param string $group Consumer group name
     * @param array $ids Message IDs to acknowledge
     * @return int Number of messages acknowledged
     */
    public function xAck(string $key, string $group, array $ids): int|false {}

    /**
     * Gets pending messages information
     *
     * @param string $key Stream key
     * @param string $group Consumer group name
     * @param string|null $start Start ID (optional)
     * @param string|null $end End ID (optional)
     * @param int $count Maximum number of entries (optional)
     * @param string|null $consumer Filter by consumer (optional)
     * @return array|false Array of pending messages or false on failure
     */
    public function xPending(string $key, string $group, ?string $start = null, ?string $end = null, int $count = -1, ?string $consumer = null): array|false {}

    /**
     * Claims pending messages
     *
     * @param string $key Stream key
     * @param string $group Consumer group name
     * @param string $consumer Consumer name
     * @param int $min_idle Minimum idle time in milliseconds
     * @param array $ids Message IDs to claim
     * @param array $options Additional options (e.g., ['JUSTID'])
     * @return array|false Claimed messages or false on failure
     */
    public function xClaim(string $key, string $group, string $consumer, int $min_idle, array $ids, array $options): array|false {}

    /**
     * Automatically claims pending messages
     *
     * @param string $key Stream key
     * @param string $group Consumer group name
     * @param string $consumer Consumer name
     * @param int $min_idle Minimum idle time in milliseconds
     * @param string $start Start ID
     * @param int $count Maximum number of messages
     * @param bool $justid Return only IDs
     * @return array|false Array with cursor and messages or false on failure
     */
    public function xAutoClaim(string $key, string $group, string $consumer, int $min_idle, string $start, int $count = -1, bool $justid = false): array|false {}

    /**
     * Deletes messages from a stream
     *
     * @param string $key Stream key
     * @param array $ids Message IDs to delete
     * @return int|false Number of messages deleted or false on failure
     */
    public function xDel(string $key, array $ids): int|false {}

    /**
     * Gets the length of a stream
     *
     * @param string $key Stream key
     * @return int|false Stream length or false on failure
     */
    public function xLen(string $key): int|false {}

    /**
     * Trims a stream to a maximum length
     *
     * @param string $key Stream key
     * @param int $maxlen Maximum stream length
     * @param bool $approx Whether to use approximate trimming
     * @param bool $minid Whether to trim by minimum ID instead of length
     * @param int $limit Maximum number of entries to trim in a single operation
     * @return int|false Number of entries deleted or false on failure
     */
    public function xTrim(string $key, int $maxlen, bool $approx = false, bool $minid = false, int $limit = -1): int|false {}

    /**
     * Gets information about a stream, consumer group, or consumers
     *
     * @param string $operation Operation: STREAM, GROUPS, CONSUMERS
     * @param string|null $arg1 Stream key
     * @param string|null $arg2 Group name (for CONSUMERS operation)
     * @param int $count Maximum number of entries (for STREAM operation)
     * @return mixed Information array or false on failure
     */
    public function xInfo(string $operation, ?string $arg1 = null, ?string $arg2 = null, int $count = -1): mixed {}

    /**
     * Gets messages in a stream range
     *
     * @param string $key Stream key
     * @param string $start Start ID
     * @param string $end End ID
     * @param int $count Maximum number of entries
     * @return array|false Array of messages or false on failure
     */
    public function xRange(string $key, string $start, string $end, int $count = -1): array|false {}

    /**
     * Gets messages in a stream range in reverse order
     *
     * @param string $key Stream key
     * @param string $start Start ID (end in reverse)
     * @param string $end End ID (start in reverse)
     * @param int $count Maximum number of entries
     * @return array|false Array of messages or false on failure
     */
    public function xRevRange(string $key, string $start, string $end, int $count = -1): array|false {}

    /**
     * Evaluates a Lua script
     *
     * @param string $script Lua script
     * @param array $args Script arguments
     * @param int $num_keys Number of keys in arguments
     * @return mixed Script result
     */
    public function eval(string $script, array $args, int $num_keys): mixed {}
}
