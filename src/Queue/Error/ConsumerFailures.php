<?php

namespace Utopia\Queue\Error;

class ConsumerFailures extends \RuntimeException
{
    /**
     * @param \Throwable[] $errors
     */
    public function __construct(private array $errors)
    {
        parent::__construct(
            'Queue consumers failed with ' . \count($errors) . ' error(s).',
            previous: $errors[0] ?? null,
        );
    }

    /**
     * @return \Throwable[]
     */
    public function getErrors(): array
    {
        return $this->errors;
    }
}
