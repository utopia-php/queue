<?php

namespace Utopia\Queue;

use Cron\CronExpression;

/**
 * Schedule represents a recurring job schedule.
 *
 * Supports two schedule types:
 * - Cron: Standard cron expressions (e.g., "0 9 * * *" for daily at 9 AM)
 * - Interval: Fixed interval in seconds (e.g., 300 for every 5 minutes)
 */
class Schedule
{
    /**
     * @param string $id Unique schedule identifier
     * @param array $payload Job payload to enqueue
     * @param string|null $cron Cron expression (e.g., "0 9 * * *")
     * @param int|null $interval Interval in seconds (alternative to cron)
     * @param int|null $startAt Unix timestamp when to start (null = immediately)
     * @param int|null $endAt Unix timestamp when to stop (null = never)
     * @param int|null $maxRuns Maximum number of executions (null = unlimited)
     */
    public function __construct(
        public readonly string $id,
        public readonly array $payload,
        public readonly ?string $cron = null,
        public readonly ?int $interval = null,
        public readonly ?int $startAt = null,
        public readonly ?int $endAt = null,
        public readonly ?int $maxRuns = null,
        private int $runCount = 0,
        private bool $paused = false,
    ) {
        if ($cron === null && $interval === null) {
            throw new \InvalidArgumentException('Either cron or interval must be specified');
        }

        if ($cron !== null && $interval !== null) {
            throw new \InvalidArgumentException('Cannot specify both cron and interval');
        }

        if ($cron !== null && !CronExpression::isValidExpression($cron)) {
            throw new \InvalidArgumentException("Invalid cron expression: {$cron}");
        }

        if ($interval !== null && $interval <= 0) {
            throw new \InvalidArgumentException('Interval must be greater than 0');
        }
    }

    /**
     * Calculate the next run time.
     *
     * @param int|null $lastRun Unix timestamp of last run (null for first run)
     * @return int|null Unix timestamp for next run, or null if schedule is complete
     */
    public function getNextRunTime(?int $lastRun = null): ?int
    {
        // Check if schedule is still active
        if (!$this->isActive()) {
            return null;
        }

        $now = time();
        $baseTime = $lastRun ?? $now;

        // Apply startAt constraint
        if ($this->startAt !== null && $baseTime < $this->startAt) {
            $baseTime = $this->startAt;
        }

        if ($this->cron !== null) {
            // Cron-based schedule
            $cronExpression = new CronExpression($this->cron);
            $nextRun = $cronExpression->getNextRunDate(\DateTime::createFromFormat('U', (string)$baseTime))->getTimestamp();
        } else {
            // Interval-based schedule
            if ($lastRun === null) {
                // First run - use startAt or now
                $nextRun = $this->startAt ?? $now;
            } else {
                // Subsequent runs - add interval
                $nextRun = $lastRun + $this->interval;

                // If we've passed the next run time, schedule for next interval from now
                if ($nextRun < $now) {
                    $elapsed = $now - $lastRun;
                    $intervals = (int)ceil($elapsed / $this->interval);
                    $nextRun = $lastRun + ($intervals * $this->interval);
                }
            }
        }

        // Check endAt constraint
        if ($this->endAt !== null && $nextRun > $this->endAt) {
            return null;
        }

        return $nextRun;
    }

    /**
     * Check if the schedule is still active (not exceeded limits).
     *
     * @return bool
     */
    public function isActive(): bool
    {
        if ($this->paused) {
            return false;
        }

        // Check max runs
        if ($this->maxRuns !== null && $this->runCount >= $this->maxRuns) {
            return false;
        }

        // Check end time
        if ($this->endAt !== null && time() > $this->endAt) {
            return false;
        }

        return true;
    }

    /**
     * Check if the schedule is paused.
     *
     * @return bool
     */
    public function isPaused(): bool
    {
        return $this->paused;
    }

    /**
     * Get the current run count.
     *
     * @return int
     */
    public function getRunCount(): int
    {
        return $this->runCount;
    }

    /**
     * Increment the run count and return a new instance.
     *
     * @return self
     */
    public function incrementRunCount(): self
    {
        return new self(
            $this->id,
            $this->payload,
            $this->cron,
            $this->interval,
            $this->startAt,
            $this->endAt,
            $this->maxRuns,
            $this->runCount + 1,
            $this->paused
        );
    }

    /**
     * Create a paused copy of this schedule.
     *
     * @return self
     */
    public function pause(): self
    {
        return new self(
            $this->id,
            $this->payload,
            $this->cron,
            $this->interval,
            $this->startAt,
            $this->endAt,
            $this->maxRuns,
            $this->runCount,
            true
        );
    }

    /**
     * Create a resumed copy of this schedule.
     *
     * @return self
     */
    public function resume(): self
    {
        return new self(
            $this->id,
            $this->payload,
            $this->cron,
            $this->interval,
            $this->startAt,
            $this->endAt,
            $this->maxRuns,
            $this->runCount,
            false
        );
    }

    /**
     * Serialize to array for storage.
     *
     * @return array
     */
    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'payload' => $this->payload,
            'cron' => $this->cron,
            'interval' => $this->interval,
            'startAt' => $this->startAt,
            'endAt' => $this->endAt,
            'maxRuns' => $this->maxRuns,
            'runCount' => $this->runCount,
            'paused' => $this->paused,
        ];
    }

    /**
     * Deserialize from array.
     *
     * @param array $data
     * @return self
     */
    public static function fromArray(array $data): self
    {
        return new self(
            $data['id'],
            $data['payload'],
            $data['cron'] ?? null,
            $data['interval'] ?? null,
            $data['startAt'] ?? null,
            $data['endAt'] ?? null,
            $data['maxRuns'] ?? null,
            $data['runCount'] ?? 0,
            $data['paused'] ?? false,
        );
    }

    /**
     * Get a human-readable description of the schedule.
     *
     * @return string
     */
    public function getDescription(): string
    {
        if ($this->cron !== null) {
            return "Cron: {$this->cron}";
        }

        $interval = $this->interval;

        // Try to find the largest clean unit that divides evenly
        if ($interval >= 86400 && $interval % 86400 === 0) {
            $days = (int)($interval / 86400);
            return "Every {$days} day" . ($days !== 1 ? 's' : '');
        }
        if ($interval >= 3600 && $interval % 3600 === 0) {
            $hours = (int)($interval / 3600);
            return "Every {$hours} hour" . ($hours !== 1 ? 's' : '');
        }
        if ($interval >= 60 && $interval % 60 === 0) {
            $minutes = (int)($interval / 60);
            return "Every {$minutes} minute" . ($minutes !== 1 ? 's' : '');
        }

        return "Every {$interval} second" . ($interval !== 1 ? 's' : '');
    }
}
