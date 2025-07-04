<?php

namespace Utopia\Queue\Broker;

use PhpAmqpLib\Connection\AMQPSwooleConnection;

class AMQPSwoole extends AMQP
{
    #[\Override]
    public function getConnectionType(): string
    {
        return AMQPSwooleConnection::class;
    }
}
