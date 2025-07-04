<?php

namespace Tests\E2E\Adapter;

use Utopia\Queue\Broker\AMQP;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class AMQPTest extends Base
{
    protected function getPublisher(): Publisher
    {
        return new AMQP(
            host: 'amqp',
            port: 5672,
            user: 'amqp',
            password: 'amqp'
        );
    }

    protected function getQueue(): Queue
    {
        return new Queue('amqp');
    }
}
