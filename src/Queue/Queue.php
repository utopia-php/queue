<?php

namespace Utopia\Queue;

readonly class Queue
{
    public function __construct(
        public string $name,
        public string $namespace = 'utopia-queue',
    ) {
        if (empty($this->name)) {
            throw new \InvalidArgumentException('Cannot create queue with empty name.');
        }
    }
}
