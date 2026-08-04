<?php

namespace Utopia\Queue;

class Message
{
    protected string $pid;
    protected string $queue;
    protected int $timestamp;
    protected array $payload;
    protected int $attempts = 0;

    public function __construct(array $array = [])
    {
        if ($array === []) {
            return;
        }

        $this->pid = $array['pid'];
        $this->queue = $array['queue'];
        $this->timestamp = $array['timestamp'];
        $this->payload = $array['payload'] ?? [];
        $this->attempts = $array['attempts'] ?? 0;
    }

    public function setPid(string $pid): self
    {
        $this->pid = $pid;

        return $this;
    }

    public function setQueue(string $queue): self
    {
        $this->queue = $queue;

        return $this;
    }

    public function setTimestamp(int $timestamp): self
    {
        $this->timestamp = $timestamp;

        return $this;
    }

    public function setPayload(array $payload): self
    {
        $this->payload = $payload;

        return $this;
    }

    public function getPid(): string
    {
        return $this->pid;
    }

    public function getQueue(): string
    {
        return $this->queue;
    }

    public function getTimestamp(): int
    {
        return $this->timestamp;
    }

    public function getPayload(): array
    {
        return $this->payload;
    }

    /**
     * Times this message has been requeued after a failed or stranded run.
     */
    public function getAttempts(): int
    {
        return $this->attempts;
    }

    public function setAttempts(int $attempts): self
    {
        $this->attempts = $attempts;

        return $this;
    }

    public function asArray(): array
    {
        return [
            'pid' => $this->pid,
            'queue' => $this->queue,
            'timestamp' => $this->timestamp,
            'payload' => $this->payload ?? null,
            'attempts' => $this->attempts,
        ];
    }
}
