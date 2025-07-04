<?php

namespace Utopia\Queue\Broker;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use Utopia\Fetch\Client;
use Utopia\Queue\Consumer;
use Utopia\Queue\Error\Retryable;
use Utopia\Queue\Message;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;
use Utopia\Queue\Result\NoCommit;

class AMQP implements Publisher, Consumer
{
    /**
     * One channel per coroutine (CID => AMQPChannel).
     * Non-coroutine contexts use CID = 0.
     */
    protected array $channels = [];

    private array $exchangeArguments = [];
    private array $queueArguments = [];
    private array $consumerArguments = [];

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
        protected float $ackTimeout = 5.0,
        protected int $maxEnqueueAttempts = 3,
        protected bool $requireAck = false,
    ) {
    }

    public function getConnectionType(): string
    {
        return AMQPStreamConnection::class;
    }

    /**
     * Enable or disable waiting for publisher confirms.
     */
    public function setRequireAck(bool $require): void
    {
        $this->requireAck = $require;
    }

    public function setAckTimeout(float $timeout): void
    {
        if ($timeout <= 0) {
            throw new \InvalidArgumentException('Ack timeout must be positive');
        }
        $this->ackTimeout = $timeout;
    }

    public function setMaxEnqueueAttempts(int $maxEnqueueAttempts): void
    {
        if ($maxEnqueueAttempts < 1) {
            throw new \InvalidArgumentException('Max enqueue attempts must be at least 1');
        }
        $this->maxEnqueueAttempts = $maxEnqueueAttempts;
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
        foreach ($this->channels as $cid => $ch) {
            try {
                $ch->getConnection()?->close();
            } catch (\Throwable) {
                // ignore – connection might already be closed
            }
            unset($this->channels[$cid]);
        }
    }

    /**
     * @throws \Exception
     */
    public function enqueue(Queue $queue, array $payload): bool
    {
        $payload = [
            'pid' => \uniqid(more_entropy: true),
            'queue' => $queue->name,
            'timestamp' => time(),
            'payload' => $payload
        ];

        $message = new AMQPMessage(
            \json_encode($payload),
            [
                'content_type' => 'application/json',
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            ]
        );

        $this->withChannel(function (AMQPChannel $channel) use ($message, $queue) {
            for ($attempts = 0; $attempts < $this->maxEnqueueAttempts; $attempts++) {
                try {
                    $channel->basic_publish(
                        $message,
                        exchange: $queue->namespace,
                        routing_key: $queue->name,
                        mandatory: $this->requireAck
                    );

                    if (!$this->requireAck) {
                        // No need to wait for ack if not required
                        return;
                    }

                    // Redeclare topology, because the queue might not exist yet
                    $channel->exchange_declare($queue->namespace, AMQPExchangeType::TOPIC, durable: true, auto_delete: false, arguments: new AMQPTable($this->exchangeArguments));
                    $channel->exchange_declare("{$queue->namespace}.failed", AMQPExchangeType::TOPIC, durable: true, auto_delete: false, arguments: new AMQPTable($this->exchangeArguments));
                    $channel->queue_declare($queue->name, durable: true, auto_delete: false, arguments: new AMQPTable(array_merge($this->queueArguments, ["x-dead-letter-exchange" => "{$queue->namespace}.failed"])));
                    $channel->queue_bind($queue->name, $queue->namespace, routing_key: $queue->name);

                    // Wait for the message to be acknowledged by the broker
                    $channel->wait_for_pending_acks($this->ackTimeout);
                } catch (
                    AMQPTimeoutException |
                    AMQPConnectionClosedException |
                    AMQPChannelClosedException |
                    AMQPConnectionBlockedException $e
                ) {
                    // Retry sending the message if ack is not received or connection has issues
                    continue;
                }

                // Exit the loop if ack is received
                break;
            }
        });

        return true;
    }

    public function retry(Queue $queue, ?int $limit = null): void
    {
        // This is a no-op for AMQP
    }

    /**
     * @throws \Exception
     */
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

        return $data['messages'] ?? 0;
    }

    /**
     * @param callable(AMQPChannel $channel): void $callback
     * @throws \Exception
     */
    protected function withChannel(callable $callback): void
    {
        $cid = \class_exists('\\Swoole\\Coroutine')
            ? \Swoole\Coroutine::getCid()
            : 0;

        $createChannel = function (): AMQPChannel {
            $connection = new ($this->getConnectionType())(
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

            // Enable publisher confirms if required
            if ($this->requireAck) {
                $channel->confirm_select();
            }
            return $channel;
        };

        if (!isset($this->channels[$cid])) {
            $this->channels[$cid] = $createChannel();
        }

        try {
            $callback($this->channels[$cid]);
        } catch (\Throwable) {
            // discard broken channel for this coroutine
            unset($this->channels[$cid]);
            // create a new channel once; rethrow on second failure
            $this->channels[$cid] = $createChannel();
            $callback($this->channels[$cid]);
        }
    }
}
