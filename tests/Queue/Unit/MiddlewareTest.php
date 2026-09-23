<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Adapter;
use Utopia\Queue\Consumer;
use Utopia\Queue\Message;
use Utopia\Queue\Queue;
use Utopia\Queue\Server;

/**
 * An init hook can only stop a job by throwing, and the broker rejects a
 * throw: on Redis the payload stays on the failed list for the retry sweep.
 * A worker that refused over-limit tenants that way kept 273k refusals
 * (~2 GB) in its queue in one week. Middleware that returns without calling
 * `next` must end the message the way a finished job does.
 */
final class MiddlewareTest extends TestCase
{
    /** @var list<string> */
    private array $trace = [];

    protected function setUp(): void
    {
        $this->trace = [];
    }

    public function testMiddlewareThatDoesNotCallNextCommitsWithoutRunningTheJob(): void
    {
        $consumer = new VerdictConsumer();
        $server = $this->server($consumer);

        $server->middleware()->action(function (): void {
            $this->trace[] = 'refused';
        });

        $server->start();

        $this->assertSame(['refused'], $this->trace);
        $this->assertSame(['commit'], $consumer->verdicts);
    }

    public function testNextRunsTheJobAndPassesItsResultBack(): void
    {
        $consumer = new VerdictConsumer();
        $server = $this->server($consumer);
        $returned = null;

        $server->middleware()->inject('next')->action(function (callable $next) use (&$returned): mixed {
            $returned = $next();

            return $returned;
        });

        $server->start();

        $this->assertSame(['job'], $this->trace);
        $this->assertSame('job result', $returned);
        $this->assertSame(['commit'], $consumer->verdicts);
    }

    public function testChainRunsInitHooksThenMiddlewareOutermostFirstAndOnlyForMatchingGroups(): void
    {
        $consumer = new VerdictConsumer();
        $server = $this->server($consumer, groups: ['functions']);

        $server->init()->action(function (): void {
            $this->trace[] = 'init';
        });
        foreach (['global' => ['*'], 'functions' => ['functions'], 'mails' => ['mails']] as $name => $groups) {
            $server->middleware()->groups($groups)->inject('next')->action(function (callable $next) use ($name): mixed {
                $this->trace[] = "{$name}:before";
                $result = $next();
                $this->trace[] = "{$name}:after";

                return $result;
            });
        }

        $server->start();

        $this->assertSame(
            ['init', 'global:before', 'functions:before', 'job', 'functions:after', 'global:after'],
            $this->trace,
        );
        $this->assertSame(['commit'], $consumer->verdicts);
    }

    public function testMiddlewareThatThrowsIsStillAFailure(): void
    {
        $consumer = new VerdictConsumer();
        $server = $this->server($consumer);

        $server->middleware()->action(static function (): never {
            throw new \RuntimeException('middleware blew up');
        });

        $server->start();

        $this->assertSame([], $this->trace, 'the job must not run past a throwing middleware');
        $this->assertSame(['reject'], $consumer->verdicts);
    }

    /**
     * @param list<string> $groups
     */
    private function server(VerdictConsumer $consumer, array $groups = []): Server
    {
        $server = new Server(new OneMessageAdapter($consumer));
        $server->job('q')->groups($groups)->action(function (): string {
            $this->trace[] = 'job';

            return 'job result';
        });

        return $server;
    }
}

/**
 * Starts one worker and hands it one message through the shared phases, so the
 * verdict comes from the same commit/reject decision a real broker gets.
 */
final class OneMessageAdapter extends Adapter
{
    public function __construct(Consumer $consumer)
    {
        parent::__construct($consumer, 1);
    }

    #[\Override]
    public function consume(callable $messageCallback, callable $successCallback, callable $errorCallback, array $queues): void
    {
        $message = new Message(['pid' => 'p1', 'queue' => 'q', 'timestamp' => time(), 'payload' => []]);

        $this->processFrom($message, $messageCallback, $successCallback, $errorCallback, new Queue('q'), $this->consumer);
    }

    public function start(): self
    {
        return $this;
    }

    public function stop(): self
    {
        return $this;
    }

    public function workerStart(callable $callback): self
    {
        $callback('w1');

        return $this;
    }

    public function workerStop(callable $callback): self
    {
        return $this;
    }

    #[\Override]
    protected function withAckExtension(Consumer $consumer, Queue $queue, Message $message, \Closure $work): void
    {
        $work();
    }
}

final class VerdictConsumer implements Consumer
{
    /** @var list<string> */
    public array $verdicts = [];

    public function receive(Queue $queue, int $timeout, int $n = 1): array
    {
        return [];
    }

    public function commit(Queue $queue, Message $message): void
    {
        $this->verdicts[] = 'commit';
    }

    public function reject(Queue $queue, Message $message): void
    {
        $this->verdicts[] = 'reject';
    }

    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        return 0;
    }

    public function getFailedCount(Queue $queue): int
    {
        return 0;
    }

    public function close(): void {}
}
