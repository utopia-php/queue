<?php

namespace Utopia\Queue\Broker;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPSwooleConnection;

class AMQPSwoole extends AMQP
{
    /**
     * Override the withChannel method to use AMQPSwooleConnection instead of AMQPStreamConnection
     *
     * @param callable(AMQPChannel $channel): void $callback
     * @throws \Exception
     */
    protected function withChannel(callable $callback): void
    {
        $createChannel = function (): AMQPChannel {
            $connection = new AMQPSwooleConnection(
                $this->host,
                $this->port,
                $this->user,
                $this->password,
                $this->vhost,
                connection_timeout: $this->connectTimeout,
                read_write_timeout: $this->readWriteTimeout,
                heartbeat: $this->heartbeat,
            );

            if (\is_callable($this->connectionConfigHook)) {
                ($this->connectionConfigHook)($connection);
            }

            $channel = $connection->channel();

            if (\is_callable($this->channelConfigHook)) {
                ($this->channelConfigHook)($channel);
            }

            return $channel;
        };

        if (!$this->channel) {
            $this->channel = $createChannel();
        }

        try {
            $callback($this->channel);
        } catch (\Throwable) {
            // createChannel() might throw, in that case set the channel to `null` first.
            $this->channel = null;
            // try creating a new connection once, if this still fails, throw the error
            $this->channel = $createChannel();
            $callback($this->channel);
        }
    }
}
