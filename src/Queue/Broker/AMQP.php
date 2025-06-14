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
use Utopia\Queue\Result\Commit;
use Utopia\Queue\Result\NoCommit;

class AMQP implements Publisher, Consumer
{
    protected ?AMQPChannel $channel = null;
    protected array $exchangeArguments = [];
    protected array $queueArguments = [];
    protected array $consumerArguments = [];

    /**
     * @var callable(AbstractConnection $connection): void
     */
    protected $connectionConfigHook;

    /**
     * @var callable(AMQPChannel $channel): void
     */
    protected $channelConfigHook;

    public function __construct(
        protected readonly string $host,
        protected readonly int $port = 5672,
        protected readonly int $httpPort = 15672,
        protected readonly ?string $user = null,
        protected readonly ?string $password = null,
        protected readonly string $vhost = '/',
        protected readonly int $heartbeat = 0,
        protected readonly float $connectTimeout = 3.0,
        protected readonly float $readWriteTimeout = 3.0,
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
                    $amqpMessage->nack();
                    return;
                }

                $nextMessage['timestamp'] = (int)$nextMessage['timestamp'];
                $message = new Message($nextMessage);

                $result = $messageCallback($message);
                match (true) {
                    $result instanceof Commit => $amqpMessage->ack(true),
                    $result instanceof NoCommit => null,
                    default => $amqpMessage->ack()
                };
                $successCallback($message);
            } catch (Retryable $e) {
                $amqpMessage->nack(requeue: true);
                $errorCallback($message ?? null, $e);
            } catch (\Throwable $th) {
                $amqpMessage->nack();
                $errorCallback($message ?? null, $th);
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
        $this->channel?->stopConsume();
        $this->channel?->getConnection()?->close();
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

    public function retry(Queue $queue, ?int $limit = null): void
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
     * @param callable(AMQPChannel $channel): void $callback
     * @throws \Exception
     */
    protected function withChannel(callable $callback): void
    {
        $createChannel = function (): AMQPChannel {
            $connection = new AMQPStreamConnection(
                $this->host,
                $this->port,
                $this->user,
                $this->password,
                $this->vhost,
                connection_timeout: $this->connectTimeout,
                read_write_timeout: $this->readWriteTimeout,
                heartbeat: $this->heartbeat,
            );
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
