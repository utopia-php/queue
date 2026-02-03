<?php

namespace Tests\Queue\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Connection\RedisStream;

/**
 * Unit tests for RedisStream connection.
 *
 * Note: These tests require a running Redis instance.
 * They test the actual Redis commands to ensure correct behavior.
 */
class RedisStreamConnectionTest extends TestCase
{
    private RedisStream $connection;
    private string $testPrefix;

    protected function setUp(): void
    {
        $this->connection = new RedisStream('redis', 6379);
        $this->testPrefix = 'test-' . uniqid() . '-';
    }

    protected function tearDown(): void
    {
        // Clean up test keys
        $this->cleanupTestKeys();
        $this->connection->close();
    }

    private function cleanupTestKeys(): void
    {
        $redis = new \Redis();
        $redis->connect('redis', 6379);

        $keys = $redis->keys($this->testPrefix . '*');
        if (!empty($keys)) {
            $redis->del($keys);
        }

        $redis->close();
    }


    public function testStreamAdd(): void
    {
        $stream = $this->testPrefix . 'stream';

        $id = $this->connection->streamAdd($stream, ['field1' => 'value1', 'field2' => 'value2']);

        $this->assertIsString($id);
        $this->assertMatchesRegularExpression('/^\d+-\d+$/', $id);
    }

    public function testStreamAddWithMaxLen(): void
    {
        $stream = $this->testPrefix . 'stream-maxlen';

        // Add 10 entries with maxlen of 5 (exact trimming by default)
        for ($i = 0; $i < 10; $i++) {
            $this->connection->streamAdd($stream, ['index' => (string)$i], '*', 5);
        }

        $len = $this->connection->streamLen($stream);
        // With exact trimming (default), should have exactly 5
        $this->assertEquals(5, $len);
    }

    public function testStreamCreateGroup(): void
    {
        $stream = $this->testPrefix . 'stream-group';
        $group = 'test-group';

        // Create group (also creates stream with MKSTREAM)
        $result = $this->connection->streamCreateGroup($stream, $group, '0', true);
        $this->assertTrue($result);

        // Creating same group again should return true (BUSYGROUP handled)
        $result = $this->connection->streamCreateGroup($stream, $group, '0', true);
        $this->assertTrue($result);
    }

    public function testStreamDestroyGroup(): void
    {
        $stream = $this->testPrefix . 'stream-destroy';
        $group = 'test-group';

        // Create and then destroy
        $this->connection->streamCreateGroup($stream, $group, '0', true);
        $result = $this->connection->streamDestroyGroup($stream, $group);
        $this->assertTrue($result);

        // Destroying non-existent group should return true (NOGROUP handled)
        $result = $this->connection->streamDestroyGroup($stream, 'non-existent');
        $this->assertTrue($result);
    }

    public function testStreamReadGroupAndAck(): void
    {
        $stream = $this->testPrefix . 'stream-read';
        $group = 'test-group';
        $consumer = 'test-consumer';

        // Create group and add message
        $this->connection->streamCreateGroup($stream, $group, '0', true);
        $messageId = $this->connection->streamAdd($stream, ['data' => 'test-message']);

        // Read message
        $result = $this->connection->streamReadGroup($group, $consumer, [$stream], 1, 100);

        $this->assertIsArray($result);
        $this->assertArrayHasKey($stream, $result);
        $this->assertNotEmpty($result[$stream]);

        // Get the entry ID from result
        $entryId = array_key_first($result[$stream]);

        // Acknowledge
        $ackCount = $this->connection->streamAck($stream, $group, $entryId);
        $this->assertEquals(1, $ackCount);
    }

    public function testStreamPendingSummary(): void
    {
        $stream = $this->testPrefix . 'stream-pending';
        $group = 'test-group';
        $consumer = 'test-consumer';

        // Setup
        $this->connection->streamCreateGroup($stream, $group, '0', true);
        $this->connection->streamAdd($stream, ['data' => 'message1']);
        $this->connection->streamAdd($stream, ['data' => 'message2']);

        // Read without acknowledging
        $this->connection->streamReadGroup($group, $consumer, [$stream], 2, 100);

        // Check pending
        $pending = $this->connection->streamPendingSummary($stream, $group);

        $this->assertIsArray($pending);
        $this->assertEquals(2, $pending[0]); // 2 pending messages
    }

    public function testStreamPendingDetails(): void
    {
        $stream = $this->testPrefix . 'stream-pending-detail';
        $group = 'test-group';
        $consumer = 'test-consumer';

        // Setup
        $this->connection->streamCreateGroup($stream, $group, '0', true);
        $this->connection->streamAdd($stream, ['data' => 'message1']);

        // Read without acknowledging
        $this->connection->streamReadGroup($group, $consumer, [$stream], 1, 100);

        // Get pending details
        $pending = $this->connection->streamPending($stream, $group, '-', '+', 10);

        $this->assertIsArray($pending);
        $this->assertCount(1, $pending);
        $this->assertEquals($consumer, $pending[0][1]); // Consumer name
    }

    public function testStreamClaim(): void
    {
        $stream = $this->testPrefix . 'stream-claim';
        $group = 'test-group';
        $consumer1 = 'consumer-1';
        $consumer2 = 'consumer-2';

        // Setup
        $this->connection->streamCreateGroup($stream, $group, '0', true);
        $messageId = $this->connection->streamAdd($stream, ['data' => 'claim-test']);

        // Consumer 1 reads
        $this->connection->streamReadGroup($group, $consumer1, [$stream], 1, 100);

        // Consumer 2 claims (with 0 idle time for testing)
        $claimed = $this->connection->streamClaim($stream, $group, $consumer2, 0, [$messageId]);

        $this->assertIsArray($claimed);
        $this->assertNotEmpty($claimed);
    }

    public function testStreamAutoClaim(): void
    {
        $stream = $this->testPrefix . 'stream-autoclaim';
        $group = 'test-group';
        $consumer1 = 'consumer-1';
        $consumer2 = 'consumer-2';

        // Setup
        $this->connection->streamCreateGroup($stream, $group, '0', true);
        $this->connection->streamAdd($stream, ['data' => 'autoclaim-test']);

        // Consumer 1 reads
        $this->connection->streamReadGroup($group, $consumer1, [$stream], 1, 100);

        // Consumer 2 auto-claims (with 0 idle time for testing)
        $result = $this->connection->streamAutoClaim($stream, $group, $consumer2, 0, '0-0', 10);

        $this->assertIsArray($result);
        $this->assertCount(3, $result); // [next_id, claimed_entries, deleted_ids]
    }

    public function testStreamDel(): void
    {
        $stream = $this->testPrefix . 'stream-del';

        $id1 = $this->connection->streamAdd($stream, ['data' => 'message1']);
        $id2 = $this->connection->streamAdd($stream, ['data' => 'message2']);

        $this->assertEquals(2, $this->connection->streamLen($stream));

        $deleted = $this->connection->streamDel($stream, [$id1]);
        $this->assertEquals(1, $deleted);
        $this->assertEquals(1, $this->connection->streamLen($stream));
    }

    public function testStreamLen(): void
    {
        $stream = $this->testPrefix . 'stream-len';

        $this->assertEquals(0, $this->connection->streamLen($stream));

        $this->connection->streamAdd($stream, ['data' => '1']);
        $this->connection->streamAdd($stream, ['data' => '2']);
        $this->connection->streamAdd($stream, ['data' => '3']);

        $this->assertEquals(3, $this->connection->streamLen($stream));
    }

    public function testStreamTrim(): void
    {
        $stream = $this->testPrefix . 'stream-trim';

        // Add 10 entries
        for ($i = 0; $i < 10; $i++) {
            $this->connection->streamAdd($stream, ['index' => (string)$i]);
        }

        $this->assertEquals(10, $this->connection->streamLen($stream));

        // Trim to 5
        $trimmed = $this->connection->streamTrim($stream, 5, false);
        $this->assertEquals(5, $trimmed);
        $this->assertEquals(5, $this->connection->streamLen($stream));
    }

    public function testStreamInfo(): void
    {
        $stream = $this->testPrefix . 'stream-info';

        $this->connection->streamAdd($stream, ['data' => 'test']);

        $info = $this->connection->streamInfo($stream);

        $this->assertIsArray($info);
        $this->assertArrayHasKey('length', $info);
        $this->assertEquals(1, $info['length']);
    }

    public function testStreamGroupInfo(): void
    {
        $stream = $this->testPrefix . 'stream-group-info';
        $group = 'test-group';

        $this->connection->streamCreateGroup($stream, $group, '0', true);

        $groups = $this->connection->streamGroupInfo($stream);

        $this->assertIsArray($groups);
        $this->assertCount(1, $groups);
        $this->assertEquals($group, $groups[0]['name']);
    }

    public function testStreamConsumersInfo(): void
    {
        $stream = $this->testPrefix . 'stream-consumers-info';
        $group = 'test-group';
        $consumer = 'test-consumer';

        $this->connection->streamCreateGroup($stream, $group, '0', true);
        $this->connection->streamAdd($stream, ['data' => 'test']);
        $this->connection->streamReadGroup($group, $consumer, [$stream], 1, 100);

        $consumers = $this->connection->streamConsumersInfo($stream, $group);

        $this->assertIsArray($consumers);
        $this->assertCount(1, $consumers);
        $this->assertEquals($consumer, $consumers[0]['name']);
    }

    public function testStreamRange(): void
    {
        $stream = $this->testPrefix . 'stream-range';

        $id1 = $this->connection->streamAdd($stream, ['index' => '1']);
        $id2 = $this->connection->streamAdd($stream, ['index' => '2']);
        $id3 = $this->connection->streamAdd($stream, ['index' => '3']);

        // Get all
        $entries = $this->connection->streamRange($stream, '-', '+');
        $this->assertCount(3, $entries);

        // Get with count
        $entries = $this->connection->streamRange($stream, '-', '+', 2);
        $this->assertCount(2, $entries);

        // Get specific range
        $entries = $this->connection->streamRange($stream, $id2, $id2);
        $this->assertCount(1, $entries);
        $this->assertEquals('2', $entries[$id2]['index']);
    }

    public function testStreamRevRange(): void
    {
        $stream = $this->testPrefix . 'stream-revrange';

        $this->connection->streamAdd($stream, ['index' => '1']);
        $this->connection->streamAdd($stream, ['index' => '2']);
        $this->connection->streamAdd($stream, ['index' => '3']);

        $entries = $this->connection->streamRevRange($stream, '+', '-', 2);

        $this->assertCount(2, $entries);
        // First entry should be the latest (index 3)
        $firstEntry = reset($entries);
        $this->assertEquals('3', $firstEntry['index']);
    }

    public function testStreamDeleteConsumer(): void
    {
        $stream = $this->testPrefix . 'stream-del-consumer';
        $group = 'test-group';
        $consumer = 'test-consumer';

        $this->connection->streamCreateGroup($stream, $group, '0', true);
        $this->connection->streamAdd($stream, ['data' => 'test']);
        $this->connection->streamReadGroup($group, $consumer, [$stream], 1, 100);

        // Delete consumer
        $pending = $this->connection->streamDeleteConsumer($stream, $group, $consumer);
        $this->assertIsInt($pending);

        // Consumer should be gone
        $consumers = $this->connection->streamConsumersInfo($stream, $group);
        $this->assertEmpty($consumers);
    }


    public function testSortedSetAdd(): void
    {
        $key = $this->testPrefix . 'zset-add';

        $result = $this->connection->sortedSetAdd($key, 1.0, 'member1');
        $this->assertEquals(1, $result);

        // Adding same member updates score, returns 0
        $result = $this->connection->sortedSetAdd($key, 2.0, 'member1');
        $this->assertEquals(0, $result);
    }

    public function testSortedSetPopByScore(): void
    {
        $key = $this->testPrefix . 'zset-pop';

        $this->connection->sortedSetAdd($key, 100, 'a');
        $this->connection->sortedSetAdd($key, 200, 'b');
        $this->connection->sortedSetAdd($key, 300, 'c');
        $this->connection->sortedSetAdd($key, 400, 'd');

        // Pop scores 0-250
        $popped = $this->connection->sortedSetPopByScore($key, 0, 250, 10);

        $this->assertCount(2, $popped);
        $this->assertContains('a', $popped);
        $this->assertContains('b', $popped);

        // Verify they're removed
        $this->assertEquals(2, $this->connection->sortedSetSize($key));
    }

    public function testSortedSetRangeByScore(): void
    {
        $key = $this->testPrefix . 'zset-range';

        $this->connection->sortedSetAdd($key, 100, 'a');
        $this->connection->sortedSetAdd($key, 200, 'b');
        $this->connection->sortedSetAdd($key, 300, 'c');

        $members = $this->connection->sortedSetRangeByScore($key, 150, 350);

        $this->assertCount(2, $members);
        $this->assertContains('b', $members);
        $this->assertContains('c', $members);
    }

    public function testSortedSetSize(): void
    {
        $key = $this->testPrefix . 'zset-size';

        $this->assertEquals(0, $this->connection->sortedSetSize($key));

        $this->connection->sortedSetAdd($key, 1, 'a');
        $this->connection->sortedSetAdd($key, 2, 'b');

        $this->assertEquals(2, $this->connection->sortedSetSize($key));
    }

    public function testSortedSetRemove(): void
    {
        $key = $this->testPrefix . 'zset-remove';

        $this->connection->sortedSetAdd($key, 1, 'member');

        $result = $this->connection->sortedSetRemove($key, 'member');
        $this->assertEquals(1, $result);

        $result = $this->connection->sortedSetRemove($key, 'non-existent');
        $this->assertEquals(0, $result);
    }

    public function testSortedSetScore(): void
    {
        $key = $this->testPrefix . 'zset-score';

        $this->connection->sortedSetAdd($key, 123.456, 'member');

        $score = $this->connection->sortedSetScore($key, 'member');
        $this->assertEquals(123.456, $score);

        $score = $this->connection->sortedSetScore($key, 'non-existent');
        $this->assertFalse($score);
    }


    public function testHashSet(): void
    {
        $key = $this->testPrefix . 'hash-set';

        $result = $this->connection->hashSet($key, 'field1', 'value1');
        $this->assertTrue($result);
    }

    public function testHashGet(): void
    {
        $key = $this->testPrefix . 'hash-get';

        $this->connection->hashSet($key, 'field1', 'value1');

        $value = $this->connection->hashGet($key, 'field1');
        $this->assertEquals('value1', $value);

        $value = $this->connection->hashGet($key, 'non-existent');
        $this->assertFalse($value);
    }

    public function testHashGetAll(): void
    {
        $key = $this->testPrefix . 'hash-getall';

        $this->connection->hashSet($key, 'field1', 'value1');
        $this->connection->hashSet($key, 'field2', 'value2');

        $all = $this->connection->hashGetAll($key);

        $this->assertEquals(['field1' => 'value1', 'field2' => 'value2'], $all);
    }

    public function testHashDel(): void
    {
        $key = $this->testPrefix . 'hash-del';

        $this->connection->hashSet($key, 'field1', 'value1');

        $result = $this->connection->hashDel($key, 'field1');
        $this->assertEquals(1, $result);

        $result = $this->connection->hashDel($key, 'non-existent');
        $this->assertEquals(0, $result);
    }

    public function testHashExists(): void
    {
        $key = $this->testPrefix . 'hash-exists';

        $this->connection->hashSet($key, 'field1', 'value1');

        $this->assertTrue($this->connection->hashExists($key, 'field1'));
        $this->assertFalse($this->connection->hashExists($key, 'non-existent'));
    }

    public function testHashLen(): void
    {
        $key = $this->testPrefix . 'hash-len';

        $this->assertEquals(0, $this->connection->hashLen($key));

        $this->connection->hashSet($key, 'field1', 'value1');
        $this->connection->hashSet($key, 'field2', 'value2');

        $this->assertEquals(2, $this->connection->hashLen($key));
    }
}
