<?php

namespace Tests\Queue\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Schedule;

class ScheduleTest extends TestCase
{
    public function testCronScheduleCreation(): void
    {
        $schedule = new Schedule(
            id: 'test-cron',
            payload: ['task' => 'cleanup'],
            cron: '*/5 * * * *'
        );

        $this->assertEquals('test-cron', $schedule->id);
        $this->assertEquals(['task' => 'cleanup'], $schedule->payload);
        $this->assertEquals('*/5 * * * *', $schedule->cron);
        $this->assertNull($schedule->interval);
        $this->assertTrue($schedule->isActive());
    }

    public function testIntervalScheduleCreation(): void
    {
        $schedule = new Schedule(
            id: 'test-interval',
            payload: ['task' => 'sync'],
            interval: 300
        );

        $this->assertEquals('test-interval', $schedule->id);
        $this->assertEquals(300, $schedule->interval);
        $this->assertNull($schedule->cron);
        $this->assertTrue($schedule->isActive());
    }

    public function testInvalidScheduleNoCronOrInterval(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Either cron or interval must be specified');

        new Schedule(
            id: 'invalid',
            payload: []
        );
    }

    public function testInvalidScheduleBothCronAndInterval(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot specify both cron and interval');

        new Schedule(
            id: 'invalid',
            payload: [],
            cron: '* * * * *',
            interval: 60
        );
    }

    public function testInvalidCronExpression(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid cron expression');

        new Schedule(
            id: 'invalid',
            payload: [],
            cron: 'not a valid cron'
        );
    }

    public function testInvalidIntervalZero(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Interval must be greater than 0');

        new Schedule(
            id: 'invalid',
            payload: [],
            interval: 0
        );
    }

    public function testIntervalNextRunTime(): void
    {
        $schedule = new Schedule(
            id: 'test',
            payload: [],
            interval: 60
        );

        $now = time();
        $nextRun = $schedule->getNextRunTime();

        // First run should be now or very close to now
        $this->assertLessThanOrEqual($now + 1, $nextRun);

        // Second run should be 60 seconds after first
        $nextRunAfterFirst = $schedule->getNextRunTime($nextRun);
        $this->assertEquals($nextRun + 60, $nextRunAfterFirst);
    }

    public function testCronNextRunTime(): void
    {
        // Every minute
        $schedule = new Schedule(
            id: 'test',
            payload: [],
            cron: '* * * * *'
        );

        $now = time();
        $nextRun = $schedule->getNextRunTime();

        // Should be within the next minute
        $this->assertGreaterThan($now, $nextRun);
        $this->assertLessThanOrEqual($now + 60, $nextRun);
    }

    public function testStartAtConstraint(): void
    {
        $futureTime = time() + 3600; // 1 hour from now

        $schedule = new Schedule(
            id: 'test',
            payload: [],
            interval: 60,
            startAt: $futureTime
        );

        $nextRun = $schedule->getNextRunTime();

        // Should not run before startAt
        $this->assertGreaterThanOrEqual($futureTime, $nextRun);
    }

    public function testEndAtConstraint(): void
    {
        $pastTime = time() - 3600; // 1 hour ago

        $schedule = new Schedule(
            id: 'test',
            payload: [],
            interval: 60,
            endAt: $pastTime
        );

        // Schedule should not be active since endAt has passed
        $this->assertFalse($schedule->isActive());
        $this->assertNull($schedule->getNextRunTime());
    }

    public function testMaxRunsConstraint(): void
    {
        $schedule = new Schedule(
            id: 'test',
            payload: [],
            interval: 60,
            maxRuns: 3
        );

        $this->assertTrue($schedule->isActive());

        // Simulate 3 runs
        $schedule = $schedule->incrementRunCount();
        $this->assertEquals(1, $schedule->getRunCount());

        $schedule = $schedule->incrementRunCount();
        $this->assertEquals(2, $schedule->getRunCount());

        $schedule = $schedule->incrementRunCount();
        $this->assertEquals(3, $schedule->getRunCount());

        // Should no longer be active after max runs
        $this->assertFalse($schedule->isActive());
        $this->assertNull($schedule->getNextRunTime());
    }

    public function testPauseAndResume(): void
    {
        $schedule = new Schedule(
            id: 'test',
            payload: [],
            interval: 60
        );

        $this->assertTrue($schedule->isActive());
        $this->assertFalse($schedule->isPaused());

        // Pause
        $paused = $schedule->pause();
        $this->assertTrue($paused->isPaused());
        $this->assertFalse($paused->isActive());
        $this->assertNull($paused->getNextRunTime());

        // Resume
        $resumed = $paused->resume();
        $this->assertFalse($resumed->isPaused());
        $this->assertTrue($resumed->isActive());
        $this->assertNotNull($resumed->getNextRunTime());
    }

    public function testToArrayAndFromArray(): void
    {
        $schedule = new Schedule(
            id: 'test-serialization',
            payload: ['key' => 'value', 'nested' => ['a' => 1]],
            cron: '0 9 * * *',
            startAt: 1704067200,
            endAt: 1735689600,
            maxRuns: 100
        );

        $array = $schedule->toArray();

        $this->assertEquals('test-serialization', $array['id']);
        $this->assertEquals(['key' => 'value', 'nested' => ['a' => 1]], $array['payload']);
        $this->assertEquals('0 9 * * *', $array['cron']);
        $this->assertEquals(1704067200, $array['startAt']);
        $this->assertEquals(1735689600, $array['endAt']);
        $this->assertEquals(100, $array['maxRuns']);

        // Reconstruct from array
        $reconstructed = Schedule::fromArray($array);

        $this->assertEquals($schedule->id, $reconstructed->id);
        $this->assertEquals($schedule->payload, $reconstructed->payload);
        $this->assertEquals($schedule->cron, $reconstructed->cron);
        $this->assertEquals($schedule->startAt, $reconstructed->startAt);
        $this->assertEquals($schedule->endAt, $reconstructed->endAt);
        $this->assertEquals($schedule->maxRuns, $reconstructed->maxRuns);
    }

    public function testGetDescription(): void
    {
        $cronSchedule = new Schedule(
            id: 'cron',
            payload: [],
            cron: '0 9 * * *'
        );
        $this->assertStringContainsString('Cron:', $cronSchedule->getDescription());

        $secondsSchedule = new Schedule(
            id: 'seconds',
            payload: [],
            interval: 30
        );
        $this->assertStringContainsString('second', $secondsSchedule->getDescription());

        $minutesSchedule = new Schedule(
            id: 'minutes',
            payload: [],
            interval: 300
        );
        $this->assertStringContainsString('minute', $minutesSchedule->getDescription());

        $hoursSchedule = new Schedule(
            id: 'hours',
            payload: [],
            interval: 7200
        );
        $this->assertStringContainsString('hour', $hoursSchedule->getDescription());

        $daysSchedule = new Schedule(
            id: 'days',
            payload: [],
            interval: 172800
        );
        $this->assertStringContainsString('day', $daysSchedule->getDescription());
    }

    public function testInvalidIntervalNegative(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Interval must be greater than 0');

        new Schedule(
            id: 'invalid',
            payload: [],
            interval: -10
        );
    }

    public function testCronVariousExpressions(): void
    {
        // Daily at midnight
        $daily = new Schedule('daily', [], cron: '0 0 * * *');
        $this->assertEquals('0 0 * * *', $daily->cron);

        // Every hour
        $hourly = new Schedule('hourly', [], cron: '0 * * * *');
        $this->assertEquals('0 * * * *', $hourly->cron);

        // Weekdays at 9am
        $weekdays = new Schedule('weekdays', [], cron: '0 9 * * 1-5');
        $this->assertEquals('0 9 * * 1-5', $weekdays->cron);

        // Every 15 minutes
        $quarter = new Schedule('quarter', [], cron: '*/15 * * * *');
        $this->assertEquals('*/15 * * * *', $quarter->cron);
    }

    public function testIntervalCatchUp(): void
    {
        $schedule = new Schedule(
            id: 'catch-up',
            payload: [],
            interval: 60
        );

        // Simulate a last run 5 minutes ago
        $lastRun = time() - 300;
        $nextRun = $schedule->getNextRunTime($lastRun);

        // Should catch up - next run should be in the future or very close
        $this->assertGreaterThanOrEqual(time() - 1, $nextRun);
    }

    public function testImmutableOperations(): void
    {
        $original = new Schedule(
            id: 'immutable-test',
            payload: ['data' => 'value'],
            interval: 60
        );

        // Increment should return new instance
        $incremented = $original->incrementRunCount();
        $this->assertEquals(0, $original->getRunCount());
        $this->assertEquals(1, $incremented->getRunCount());

        // Pause should return new instance
        $paused = $original->pause();
        $this->assertFalse($original->isPaused());
        $this->assertTrue($paused->isPaused());

        // Resume should return new instance
        $resumed = $paused->resume();
        $this->assertTrue($paused->isPaused());
        $this->assertFalse($resumed->isPaused());
    }

    public function testFromArrayWithDefaults(): void
    {
        $minimal = [
            'id' => 'minimal',
            'payload' => ['test' => true],
            'interval' => 60,
        ];

        $schedule = Schedule::fromArray($minimal);

        $this->assertEquals('minimal', $schedule->id);
        $this->assertNull($schedule->cron);
        $this->assertEquals(60, $schedule->interval);
        $this->assertNull($schedule->startAt);
        $this->assertNull($schedule->endAt);
        $this->assertNull($schedule->maxRuns);
        $this->assertEquals(0, $schedule->getRunCount());
        $this->assertFalse($schedule->isPaused());
    }

    public function testFromArrayPreservesState(): void
    {
        $data = [
            'id' => 'stateful',
            'payload' => [],
            'interval' => 60,
            'runCount' => 5,
            'paused' => true,
        ];

        $schedule = Schedule::fromArray($data);

        $this->assertEquals(5, $schedule->getRunCount());
        $this->assertTrue($schedule->isPaused());
    }

    public function testEndAtInFuture(): void
    {
        $futureEndAt = time() + 3600; // 1 hour from now

        $schedule = new Schedule(
            id: 'future-end',
            payload: [],
            interval: 60,
            endAt: $futureEndAt
        );

        $this->assertTrue($schedule->isActive());
        $nextRun = $schedule->getNextRunTime();
        $this->assertNotNull($nextRun);
        $this->assertLessThan($futureEndAt, $nextRun);
    }

    public function testNextRunRespectsFutureEndAt(): void
    {
        // endAt is 30 seconds from now
        $endAt = time() + 30;

        $schedule = new Schedule(
            id: 'end-soon',
            payload: [],
            interval: 60, // 60 second interval
            endAt: $endAt
        );

        // First run is now
        $firstRun = $schedule->getNextRunTime();
        $this->assertNotNull($firstRun);

        // Next run would be 60s after first run, which is after endAt
        $secondRun = $schedule->getNextRunTime($firstRun);
        $this->assertNull($secondRun); // Should be null because it would exceed endAt
    }

    public function testComplexPayload(): void
    {
        $complexPayload = [
            'string' => 'value',
            'number' => 42,
            'float' => 3.14,
            'bool' => true,
            'null' => null,
            'array' => [1, 2, 3],
            'nested' => [
                'deep' => [
                    'value' => 'found'
                ]
            ]
        ];

        $schedule = new Schedule(
            id: 'complex',
            payload: $complexPayload,
            interval: 60
        );

        $this->assertEquals($complexPayload, $schedule->payload);

        // Verify serialization preserves structure
        $array = $schedule->toArray();
        $reconstructed = Schedule::fromArray($array);

        $this->assertEquals($complexPayload, $reconstructed->payload);
    }
}
