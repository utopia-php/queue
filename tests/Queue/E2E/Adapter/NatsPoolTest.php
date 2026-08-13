<?php

declare(strict_types=1);

namespace Tests\E2E\Adapter;

use Utopia\NATS\Connection;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Queue\Broker\Nats;
use Utopia\Queue\Broker\Pool;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

/**
 * The NATS broker used through Broker\Pool — the pooled wiring cloud uses. The pool
 * leases one broker (and therefore one single-owner connection) per caller, which is
 * the recommended way to use Broker\Nats concurrently.
 */
final class NatsPoolTest extends Base
{
    protected function getPublisher(): Publisher
    {
        $factory = fn(): Nats => new Nats(
            fn(): Connection => Connection::connect('nats://127.0.0.1:14225'),
            maxDeliver: 3,
        );
        $pool = new UtopiaPool(new Stack(), 'nats', 1, $factory, timeout: 0.0);

        return new Pool($pool, $pool);
    }

    protected function getQueue(): Queue
    {
        return new Queue('nats-pool');
    }
}
