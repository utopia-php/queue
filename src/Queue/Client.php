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

    public function queue(array $payload): bool
    {
        return $this->connection->rightPush($this->queue, $payload);
    }
}