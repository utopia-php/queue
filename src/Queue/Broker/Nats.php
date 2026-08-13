<?php

declare(strict_types=1);

namespace Utopia\Queue\Broker;

use Utopia\NATS\Connection as NatsConnection;
use Utopia\NATS\JetStream\AckPolicy;
use Utopia\NATS\JetStream\Consumer as NatsConsumer;
use Utopia\NATS\JetStream\ConsumerConfig;
use Utopia\NATS\JetStream\JetStream;
use Utopia\NATS\JetStream\JetStreamMessage;
use Utopia\NATS\JetStream\RetentionPolicy;
use Utopia\NATS\JetStream\StorageType;
use Utopia\NATS\JetStream\StreamConfig;
use Utopia\Queue\Consumer;
use Utopia\Queue\Message;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

/**
 * NATS JetStream broker.
 *
 * Each queue is a WorkQueue-retention stream (a message is removed once acked)
 * with two subjects — normal and priority — served by two durable pull consumers.
 * Redelivery and dead-lettering are native: a rejected message is NAK'd and
 * redelivered until MaxDeliver, after which it is TERM'd and copied to a per-queue
 * dead stream. This replaces the Redis broker's hand-rolled processing/failed/dead
 * lists and its reap()/retry() sweeps (AckWait redelivery reclaims stranded jobs).
 */
class Nats implements Publisher, Consumer
{
    // Wire-level identifiers (stream/subject naming, durable consumers, advisories).
    private const string STREAM_PREFIX = 'QUEUE_';
    private const string DEAD_STREAM_SUFFIX = '_DEAD';
    private const string SUBJECT_PREFIX = 'Q.';
    private const string SUBJECT_NORMAL = 'normal';
    private const string SUBJECT_PRIORITY = 'priority';
    private const string SUBJECT_DEAD = 'dead';
    private const string CONSUMER_NORMAL = 'worker';
    private const string CONSUMER_PRIORITY = 'worker_priority';
    private const string CONSUMER_RETRY = 'retry';
    private const string ADVISORY_MAX_DELIVERIES = '$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES';

    /** @var array<string, bool> queues whose streams/consumers have been provisioned */
    private array $provisioned = [];

    /** @var array<string, array{normal: NatsConsumer, priority: NatsConsumer}> */
    private array $consumers = [];

    /** @var array<string, JetStreamMessage> in-flight messages keyed by pid, for commit/reject */
    private array $inFlight = [];

    /** @var array<string, \Utopia\NATS\Subscription> max-deliveries advisory subscription per queue */
    private array $advisories = [];

    private ?NatsConnection $connection = null;
    private ?JetStream $js = null;

    /**
     * A NATS Connection is single-owner (one socket, one shared read pump), so it is
     * NOT safe to share across concurrent coroutines. Pass a Closure factory rather
     * than a live Connection when the consumer forks or reconnects per worker (each
     * worker resolves its own connection), and run at most one message at a time per
     * connection (e.g. Swoole adapter with maxCoroutines: 1) or lease one connection
     * per coroutine from a pool.
     *
     * commit()/reject() correlate the JetStream acknowledgement to a message through an
     * in-instance map keyed by pid, so a message must be committed/rejected on the SAME
     * instance that received it: use one consumer instance (Broker\Pool is for the
     * publisher side).
     *
     * @param NatsConnection|(\Closure(): NatsConnection) $source
     */
    public function __construct(
        private readonly NatsConnection|\Closure $source,
        private readonly float $ackWait = 30.0,
        private readonly int $maxDeliver = 5,
        private readonly int $replicas = 1,
    ) {}

    private function connection(): NatsConnection
    {
        return $this->connection ??= $this->source instanceof \Closure ? ($this->source)() : $this->source;
    }

    private function js(): JetStream
    {
        return $this->js ??= $this->connection()->jetStream();
    }

    public function enqueue(Queue $queue, array $payload, bool $priority = false): bool
    {
        $this->ensure($queue);

        // Match the Redis broker's message shape so Message round-trips identically.
        $message = [
            'pid' => uniqid('', true),
            'queue' => $queue->name,
            'timestamp' => time(),
            'payload' => $payload,
        ];

        $subject = $priority ? $this->prioritySubject($queue) : $this->workSubject($queue);
        $this->js()->publish($subject, (string) json_encode($message));

        return true;
    }

    public function receive(Queue $queue, int $timeout): ?Message
    {
        $this->ensure($queue);
        $key = $this->identity($queue);
        $this->drainDeadLetters($queue, $key);

        // Priority first (no_wait poll), then the normal queue for up to $timeout.
        $jsMessage = $this->fetchOne($this->consumers[$key]['priority'], 0.25, true)
            ?? $this->fetchOne($this->consumers[$key]['normal'], (float) $timeout, false);

        if (!$jsMessage instanceof JetStreamMessage) {
            return null;
        }

        /** @var array{pid: string, queue: string, timestamp: int, payload: array<mixed>} $data */
        $data = json_decode($jsMessage->getData(), true);
        $this->inFlight[$data['pid']] = $jsMessage;

        return new Message($data)
            // JetStream counts deliveries from 1; expose it as the Redis-style attempt count.
            ->setAttempts(max(0, $jsMessage->metadata()->numDelivered - 1));
    }

    public function commit(Queue $queue, Message $message): void
    {
        $pid = $message->getPid();
        $jsMessage = $this->inFlight[$pid] ?? null;
        if ($jsMessage instanceof JetStreamMessage) {
            $jsMessage->ackSync();
            unset($this->inFlight[$pid]);
        }
    }

    public function reject(Queue $queue, Message $message): void
    {
        $pid = $message->getPid();
        $jsMessage = $this->inFlight[$pid] ?? null;
        if (!$jsMessage instanceof JetStreamMessage) {
            return;
        }
        unset($this->inFlight[$pid]);

        if ($jsMessage->metadata()->numDelivered >= $this->maxDeliver) {
            // Exhausted: park on the dead stream and drop it from the work stream.
            $this->js()->publish($this->deadSubject($queue), $jsMessage->getData());
            $jsMessage->term('max deliveries exceeded');

            return;
        }

        // Redeliver later (AckWait/NAK); a crashed worker is reclaimed the same way.
        $jsMessage->nak();
    }

    /**
     * Re-drive dead-lettered messages back onto the work queue, up to $limit.
     *
     * $maxAttempts and $newerThan exist only for signature compatibility with
     * Broker\Redis::retry() (cloud calls it with them); they are not applied here.
     * In the JetStream model attempts are capped server-side by maxDeliver before a
     * message reaches the dead stream, so there is nothing left to gate on re-drive.
     */
    public function retry(Queue $queue, ?int $limit = null, ?int $maxAttempts = null, ?int $newerThan = null): void
    {
        $this->ensure($queue);

        $consumer = $this->js()->createConsumer($this->deadStream($queue), new ConsumerConfig(
            durableName: self::CONSUMER_RETRY,
            ackPolicy: AckPolicy::Explicit,
            ackWait: $this->ackWait,
            filterSubject: $this->deadSubject($queue),
        ));

        $remaining = $limit ?? 500;
        while ($remaining > 0) {
            $jsMessage = $this->fetchOne($consumer, 1.0, false);
            if (!$jsMessage instanceof JetStreamMessage) {
                break;
            }
            // Re-drive onto the work queue, then remove it from the dead stream.
            $this->js()->publish($this->workSubject($queue), $jsMessage->getData());
            $jsMessage->ackSync();
            $remaining--;
        }
    }

    /**
     * Reaping stranded in-flight jobs is unnecessary on JetStream: AckWait redelivery
     * reclaims a message whose worker died before committing. Kept for drop-in
     * compatibility with the Redis broker's call sites; always returns 0.
     */
    public function reap(Queue $queue, int $olderThan = 90000, ?int $limit = null, ?int $maxAttempts = null, ?int $newerThan = null): int
    {
        return 0;
    }

    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        $this->ensure($queue);
        $key = $this->identity($queue);
        $this->drainDeadLetters($queue, $key);

        if ($failedJobs) {
            return $this->js()->getStreamInfo($this->deadStream($queue))->state->messages;
        }

        return $this->consumers[$key]['normal']->info(true)->numPending
            + $this->consumers[$key]['priority']->info(true)->numPending;
    }

    public function close(): void
    {
        if ($this->connection instanceof NatsConnection) {
            $this->connection->close();
        }
    }

    /** Fetch a single message, or null on timeout / empty. */
    private function fetchOne(NatsConsumer $consumer, float $timeout, bool $noWait): ?JetStreamMessage
    {
        foreach ($consumer->fetch(1, $timeout, $noWait) as $message) {
            return $message;
        }

        return null;
    }

    /** Idempotently provision the work + dead streams and the durable consumers. */
    private function ensure(Queue $queue): void
    {
        $key = $this->identity($queue);
        if (isset($this->provisioned[$key])) {
            return;
        }

        $maxAge = $queue->jobTtl > 0 ? (float) $queue->jobTtl : null;

        $this->js()->createOrUpdateStream(new StreamConfig(
            name: $this->workStream($queue),
            subjects: [$this->workSubject($queue), $this->prioritySubject($queue)],
            retention: RetentionPolicy::WorkQueue,
            maxAge: $maxAge,
            storage: StorageType::File,
            replicas: $this->replicas,
        ));

        $this->js()->createOrUpdateStream(new StreamConfig(
            name: $this->deadStream($queue),
            subjects: [$this->deadSubject($queue)],
            retention: RetentionPolicy::WorkQueue,
            storage: StorageType::File,
            replicas: $this->replicas,
        ));

        $this->consumers[$key] = [
            'normal' => $this->js()->createConsumer($this->workStream($queue), new ConsumerConfig(
                durableName: self::CONSUMER_NORMAL,
                ackPolicy: AckPolicy::Explicit,
                ackWait: $this->ackWait,
                maxDeliver: $this->maxDeliver,
                filterSubject: $this->workSubject($queue),
            )),
            'priority' => $this->js()->createConsumer($this->workStream($queue), new ConsumerConfig(
                durableName: self::CONSUMER_PRIORITY,
                ackPolicy: AckPolicy::Explicit,
                ackWait: $this->ackWait,
                maxDeliver: $this->maxDeliver,
                filterSubject: $this->prioritySubject($queue),
            )),
        ];

        // Best-effort terminal dead-lettering for the crash-loop case: a worker that
        // dies (never reject()s) is redelivered by AckWait until maxDeliver, after which
        // JetStream stops delivering and emits this advisory. We drain it in receive()/
        // getQueueSize() and move the stuck message to the dead stream. Caveat: core
        // advisories are ephemeral, so a message that exhausts while no broker is
        // subscribed stays as pending backlog (still visible) rather than dead-lettered.
        $this->advisories[$key] = $this->connection()->subscribe(
            self::ADVISORY_MAX_DELIVERIES . ".{$this->workStream($queue)}.*",
        );

        $this->provisioned[$key] = true;
    }

    /** Move messages that exhausted maxDeliver (per the advisory) onto the dead stream. */
    private function drainDeadLetters(Queue $queue, string $key): void
    {
        $advisory = $this->advisories[$key] ?? null;
        if (!$advisory instanceof \Utopia\NATS\Subscription) {
            return;
        }

        while (($event = $advisory->nextMessage(0.0)) instanceof \Utopia\NATS\Message) {
            $decoded = json_decode($event->data, true);
            $seq = \is_array($decoded) ? ($decoded['stream_seq'] ?? null) : null;
            if (!\is_int($seq)) {
                continue;
            }

            try {
                $stored = $this->js()->getMessage($this->workStream($queue), $seq);
                $this->js()->publish($this->deadSubject($queue), $stored->data);
                $this->js()->deleteMessage($this->workStream($queue), $seq);
            } catch (\Throwable) {
                // Already acked/deleted or raced away — nothing to reclaim.
            }
        }
    }

    /** Logical queue identity (namespace + name); used for cache keys and stream naming. */
    private function identity(Queue $queue): string
    {
        // Length-prefix the namespace so a delimiter in either field can't create an
        // ambiguous join (ns "a.b"+name "c" vs "a"+"b.c"). Byte-safe: unlike json_encode
        // it never fails on invalid UTF-8 (which would collapse to an empty identity).
        return \strlen($queue->namespace) . ':' . $queue->namespace . ':' . $queue->name;
    }

    private function workStream(Queue $queue): string
    {
        // Fixed width so the name can never exceed JetStream's 255-byte limit: a bounded
        // readable prefix plus a full sha256 of the identity. 256 bits makes a collision
        // infeasible (unlike the earlier 40-bit truncation), while an unbounded injective
        // encoding (bin2hex) would blow the length limit for long queue names.
        return self::STREAM_PREFIX . substr($this->sanitize("{$queue->namespace}_{$queue->name}"), 0, 40) . '_' . hash('sha256', $this->identity($queue));
    }

    private function deadStream(Queue $queue): string
    {
        return $this->workStream($queue) . self::DEAD_STREAM_SUFFIX;
    }

    /**
     * Collision-free subject namespace for a queue: a fixed leading token, an identity
     * hash, and a category tail. Raw namespace/name are NOT interpolated — they can
     * contain dots (even the literal "queue"/"priority"/"dead"), so building subjects
     * from them is ambiguous (e.g. ns "a" + name "b.queue.c" vs ns "a.queue.b" + name
     * "c" both yield "a.queue.b.queue.c"). A sha256 of the identity is a single dot-free,
     * fixed-width token, so distinct queues never share a subject.
     */
    private function subjectBase(Queue $queue): string
    {
        return self::SUBJECT_PREFIX . hash('sha256', $this->identity($queue));
    }

    private function workSubject(Queue $queue): string
    {
        return $this->subjectBase($queue) . '.' . self::SUBJECT_NORMAL;
    }

    private function prioritySubject(Queue $queue): string
    {
        return $this->subjectBase($queue) . '.' . self::SUBJECT_PRIORITY;
    }

    private function deadSubject(Queue $queue): string
    {
        return $this->subjectBase($queue) . '.' . self::SUBJECT_DEAD;
    }

    /** Stream names allow only A-Z a-z 0-9 _ - (no dots), unlike subject/queue names. */
    private function sanitize(string $name): string
    {
        return (string) preg_replace('/[^A-Za-z0-9_-]/', '_', $name);
    }
}
