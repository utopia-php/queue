<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue\Broker\Redis as RedisBroker;
use Utopia\Queue\Connection\Redis as RedisConnection;
use Utopia\Queue\Queue;

/**
 * One-shot worker for the KEDA ScaledJob POC. KEDA spawns this as a Kubernetes
 * Job whenever the Redis queue has pending messages; it drains the queue with
 * the ordinary Redis broker, then exits so the Job completes. The payload never
 * touches Kubernetes — it stays in Redis and is pulled here.
 */
$connection = new RedisConnection(getenv('REDIS_HOST') ?: 'redis', 6379);
$broker = new RedisBroker($connection, $connection);
$queue = new Queue(getenv('QUEUE_NAME') ?: 'keda', getenv('QUEUE_NAMESPACE') ?: 'utopia-queue');

$processed = 0;
while (($message = $broker->receive($queue, 2)) instanceof \Utopia\Queue\Message) {
    try {
        handleRequest($message);
        $broker->commit($queue, $message);
        $processed++;
    } catch (\Throwable $error) {
        $broker->reject($queue, $message);
        fwrite(STDERR, "Job {$message->getPid()} failed: {$error->getMessage()}\n");
    }
}

fwrite(STDOUT, "Drained {$processed} message(s), exiting\n");
exit(0);
