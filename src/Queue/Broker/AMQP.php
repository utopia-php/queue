<?php

namespace Utopia\Queue\Broker;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Utopia\Fetch\Client;
use Utopia\Queue\Consumer;
use Utopia\Queue\Error\Retryable;
use Utopia\Queue\Message;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

class AMQP implements Publisher, Consumer
{
    private ?AMQPChannel $channel = null;
    private array $exchangeArguments = [];
    private array $queueArguments = [];
    private array $consumerArguments = [];

    /**
     * @var callable(AbstractConnection $connection): void
     */
    private $connectionConfigHook;

    /**
     * @var callable(AMQPChannel $channel): void
     */
    private $channelConfigHook;

    public function __construct(
        private readonly string $host,
        private readonly int $port = 5672,
        private readonly int $httpPort = 15672,
        private readonly ?string $user = null,
        private readonly ?string $password = null,
        private readonly string $vhost = '/',
        private readonly int $heartbeat = 0,
    ) {
    }

    public function setExchangeArgument(string $key, string $value): void
    {
        $this->exchangeArguments[$key] = $value;
    }

    public function setQueueArgument(string $key, string $value): void
    {
        $this->queueArguments[$key] = $value;
    }

    public function setConsumerArguments(string $key, string $value): void
    {
        $this->consumerArguments[$key] = $value;
    }

    public function configureConnection(callable $callback): void
    {
        $this->connectionConfigHook = $callback;
    }

    public function configureChannel(callable $callback): void
    {
        $this->channelConfigHook = $callback;
    }

    public function consume(Queue $queue, callable $messageCallback, callable $successCallback, callable $errorCallback): void
    {
        $processMessage = function (AMQPMessage $amqpMessage) use ($messageCallback, $successCallback, $errorCallback) {
            try {
                $nextMessage = json_decode($amqpMessage->getBody(), associative: true) ?? false;
                if (!$nextMessage) {
                    $amqpMessage->nack(requeue: false);
                    return;
                }

                $nextMessage['timestamp'] = (int)$nextMessage['timestamp'];
                $message = new Message($nextMessage);

                $messageCallback($message);
                $amqpMessage->ack();
                $successCallback($message);
            } catch (Retryable $e) {
                $amqpMessage->nack(requeue: true);
                $errorCallback($message, $e);
            } catch (\Throwable $th) {
                $amqpMessage->nack(requeue: false);
                $errorCallback($message, $th);
            }
        };

        $this->withChannel(function (AMQPChannel $channel) use ($queue, $processMessage) {
            // It's good practice for the consumer to set up exchange and queues.
            // This approach uses TOPICs (https://www.rabbitmq.com/tutorials/amqp-concepts#exchange-topic) and
            // dead-letter-exchanges (https://www.rabbitmq.com/docs/dlx) for failed messages.

            // 1. Declare the exchange and a dead-letter-exchange.
            $channel->exchange_declare($queue->namespace, AMQPExchangeType::TOPIC, durable: true, auto_delete: false, arguments: new AMQPTable($this->exchangeArguments));
            $channel->exchange_declare("{$queue->namespace}.failed", AMQPExchangeType::TOPIC, durable: true, auto_delete: false, arguments: new AMQPTable($this->exchangeArguments));

            // 2. Declare the working queue and configure the DLX for receiving rejected messages.
            $channel->queue_declare($queue->name, durable: true, auto_delete: false, arguments: new AMQPTable(array_merge($this->queueArguments, ["x-dead-letter-exchange" => "{$queue->namespace}.failed"])));
            $channel->queue_bind($queue->name, $queue->namespace, routing_key: $queue->name);

            // 3. Declare the dead-letter-queue and bind it to the DLX.
            $channel->queue_declare("{$queue->name}.failed", durable: true, auto_delete: false, arguments: new AMQPTable($this->queueArguments));
            $channel->queue_bind("{$queue->name}.failed", "{$queue->namespace}.failed", routing_key: $queue->name);

            // 4. Instruct to consume on the working queue.
            $channel->basic_consume($queue->name, callback: $processMessage, arguments: new AMQPTable($this->consumerArguments));
        });

        // Run ->consume in own callback to avoid re-running queue creation flow on error.
        $this->withChannel(function (AMQPChannel $channel) {
            // 5. Consume. This blocks until the connection gets closed.
            $channel->consume();
        });
    }

    public function close(): void
    {
        if ($this->channel) {
            $this->channel->getConnection()?->close();
        }
    }

    public function ping(): bool
    {
        return $this->withChannel(fn (AMQPChannel $channel) => $channel->is_open());
    }

    public function enqueue(Queue $queue, array $payload): bool
    {
        $payload = [
            'pid' => \uniqid(more_entropy: true),
            'queue' => $queue->name,
            'timestamp' => time(),
            'payload' => $payload
        ];
        $message = new AMQPMessage(json_encode($payload), ['content_type' => 'application/json', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        $this->withChannel(function (AMQPChannel $channel) use ($message, $queue) {
            $channel->basic_publish($message, $queue->namespace, routing_key: $queue->name);
        });
        return true;
    }

    public function retry(Queue $queue, int $limit = null): void
    {
        // This is a no-op for AMQP
    }

    public function getQueueSize(Queue $queue, bool $failedJobs = false): int
    {
        $queueName = $queue->name;
        if ($failedJobs) {
            $queueName = $queueName . ".failed";
        }

        $client = new Client();
        $response = $client->fetch(sprintf('http://%s:%s@%s:%s/api/queues/%s/%s', $this->user, $this->password, $this->host, $this->httpPort, urlencode($this->vhost), $queueName));

        // If this queue does not exist (yet), the queue size is 0.
        if ($response->getStatusCode() === 404) {
            return 0;
        }

        if ($response->getStatusCode() !== 200) {
            throw new \Exception(sprintf('Invalid status code %d: %s', $response->getStatusCode(), $response->getBody()));
        }

        $data = $response->json();
        return $data['messages'];
    }

    /**
     * @param callable(AMQPChannel $channel): mixed $callback
     * @throws \Exception
     */
    private function withChannel(callable $callback): mixed
    {
        $createChannel = function (): AMQPChannel {
            $connection = new AMQPStreamConnection($this->host, $this->port, $this->user, $this->password, $this->vhost, heartbeat: $this->heartbeat);
            if (is_callable($this->connectionConfigHook)) {
                call_user_func($this->connectionConfigHook, $connection);
            }
            $channel = $connection->channel();
            if (is_callable($this->channelConfigHook)) {
                call_user_func($this->channelConfigHook, $channel);
            }
            return $channel;
        };

        if (!$this->channel) {
            $this->channel = $createChannel();
        }

        try {
            return $callback($this->channel);
        } catch (\Throwable $th) {
            // createChannel() might throw, in that case set the channel to `null` first.
            $this->channel = null;
            // try creating a new connection once, if this still fails, throw the error
            $this->channel = $createChannel();
            return $callback($this->channel);
        }
    }
}
