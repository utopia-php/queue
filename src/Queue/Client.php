<?php

namespace Utopia\Queue;

class Client
{
    protected string $queue;
    protected Connection $connection;
    public function __construct(string $queue, Connection $connection)
    {
        $this->queue = $queue;
        $this->connection = $connection;
    }

    public function enqueue(array $payload): bool
    {
        $payload = [
            'pid' => \uniqid(more_entropy: true),
            'queue' => $this->queue,
            'timestamp' => \intval(\microtime()),
            'payload' => $payload
        ];

        return $this->connection->leftPush($this->queue, $payload);
    }
}