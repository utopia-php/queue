<?php

declare(strict_types=1);

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Adapter\KubernetesJob;
use Utopia\Queue\Broker\Redis;
use Utopia\Queue\Message;
use Utopia\Queue\Queue;
use Utopia\Queue\Server;

/**
 * Unit coverage for the run-to-completion KubernetesJob adapter: it drains the
 * queue and returns (so a Kubernetes Job completes) rather than blocking like
 * the long-running adapters. Runs on a bare host against InMemoryConnection.
 */
final class KubernetesJobAdapterTest extends TestCase
{
    private const string QUEUE = 'keda-unit';
    private const string NAMESPACE = 'tests';

    private function server(Redis $broker, callable $action): Server
    {
        $server = new Server(new KubernetesJob($broker, 1, self::QUEUE, self::NAMESPACE));
        $server->job()->inject('message')->action($action);

        return $server;
    }

    public function testDrainsQueueThenReturns(): void
    {
        $connection = new InMemoryConnection();
        $broker = new Redis($connection, $connection);
        $queue = new Queue(self::QUEUE, self::NAMESPACE);

        foreach (range(1, 5) as $n) {
            $broker->enqueue($queue, ['n' => $n]);
        }

        $processed = [];
        $this->server($broker, function (Message $message) use (&$processed): void {
            $processed[] = $message->getPayload()['n'];
        })->start();

        $this->assertSame([1, 2, 3, 4, 5], $processed, 'every queued message is processed once, in order');
        $this->assertSame(0, $broker->getQueueSize($queue), 'the queue is drained');
    }

    public function testReturnsImmediatelyWhenQueueEmpty(): void
    {
        $connection = new InMemoryConnection();
        $broker = new Redis($connection, $connection);

        $processed = 0;
        $this->server($broker, function () use (&$processed): void {
            $processed++;
        })->start();

        $this->assertSame(0, $processed, 'an empty queue processes nothing and the worker exits');
    }

    public function testFailedMessageIsRejectedAndDrainContinues(): void
    {
        $connection = new InMemoryConnection();
        $broker = new Redis($connection, $connection);
        $queue = new Queue(self::QUEUE, self::NAMESPACE);

        $broker->enqueue($queue, ['ok' => false]);
        $broker->enqueue($queue, ['ok' => true]);

        $succeeded = 0;
        $this->server($broker, function (Message $message) use (&$succeeded): void {
            if ($message->getPayload()['ok'] === false) {
                throw new \RuntimeException('boom');
            }
            $succeeded++;
        })->start();

        $this->assertSame(1, $succeeded, 'the drain continues past a failing message');
        $this->assertSame(0, $broker->getQueueSize($queue), 'the main queue is drained');
        $this->assertSame(1, $broker->getQueueSize($queue, failedJobs: true), 'the failed message lands on the failed queue');
    }
}
