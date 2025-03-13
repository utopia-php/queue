<?php

namespace Utopia\Queue;

use Utopia\Queue\Result\Commit;
use Utopia\Queue\Result\NoCommit;

interface Consumer
{
    /**
     * @param Queue $queue
     * @param callable(Message $message): Commit|NoCommit|mixed $messageCallback
     * @param callable(Message $message): void $successCallback
     * @param callable(Message $message, \Throwable $th): void $errorCallback
     * @return void
     */
    public function consume(Queue $queue, callable $messageCallback, callable $successCallback, callable $errorCallback): void;

    /**
     * Closes the consumer and free's any underlying resources.
     */
    public function close(): void;
}
