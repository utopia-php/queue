# AMQPSwoole Broker

## Overview

The `AMQPSwoole` class is a specialized AMQP broker implementation designed for use in Swoole environments. It extends the base `AMQP` class and resolves compatibility issues that occur when using the standard AMQP broker in Swoole coroutines.

## Why AMQPSwoole is Needed

When using the standard `AMQP` broker in Swoole environments, you may encounter errors like:

```
Fatal error: Uncaught Swoole\Error: API must be called in the coroutine
```

This happens because:
- The standard AMQP broker uses `AMQPStreamConnection` with `StreamIO`
- `StreamIO` calls `stream_select()` which is not allowed outside Swoole coroutines
- This causes fatal errors during connection cleanup and heartbeat operations

## Solution

The `AMQPSwoole` class solves this by:
- Using `AMQPSwooleConnection` instead of `AMQPStreamConnection`
- Leveraging `SwooleIO` which is designed for Swoole environments
- Properly handling coroutine-based I/O operations

## Usage

### Basic Usage

```php
use Utopia\Queue\Broker\AMQPSwoole;
use Utopia\Queue\Queue;

// Create broker instance
$broker = new AMQPSwoole(
    host: 'localhost',
    port: 5672,
    user: 'guest',
    password: 'guest'
);

// Create queue
$queue = new Queue('my-queue');

// Enqueue a message
$broker->enqueue($queue, ['message' => 'Hello World']);
```

### Consumer Usage

```php
use Utopia\Queue\Consumer;
use Utopia\Queue\Message;

$broker->consume(
    $queue,
    function (Message $message) {
        // Process the message
        $payload = $message->getPayload();
        echo "Processing: " . json_encode($payload) . PHP_EOL;
        
        // Return result (optional)
        return new \Utopia\Queue\Result\Commit();
    },
    function (Message $message) {
        echo "Message processed successfully" . PHP_EOL;
    },
    function (?Message $message, \Throwable $error) {
        echo "Error processing message: " . $error->getMessage() . PHP_EOL;
    }
);
```

## Configuration

The `AMQPSwoole` class accepts the same configuration parameters as the base `AMQP` class:

- `host`: RabbitMQ server hostname
- `port`: RabbitMQ server port (default: 5672)
- `httpPort`: RabbitMQ management HTTP port (default: 15672)
- `user`: Username for authentication
- `password`: Password for authentication
- `vhost`: Virtual host (default: '/')
- `heartbeat`: Heartbeat interval in seconds (default: 0)
- `connectTimeout`: Connection timeout in seconds (default: 3.0)
- `readWriteTimeout`: Read/write timeout in seconds (default: 3.0)

## Testing

The AMQPSwoole broker includes comprehensive tests that verify:
- Basic message enqueueing and processing
- Concurrency handling in Swoole environments
- Error handling and retry mechanisms
- Queue size reporting

Run tests with:
```bash
composer test
```

## Migration from AMQP to AMQPSwoole

If you're currently using the `AMQP` broker in a Swoole environment and experiencing errors, migration is simple:

```php
// Before
$broker = new \Utopia\Queue\Broker\AMQP($host, $port, $httpPort, $user, $password);

// After
$broker = new \Utopia\Queue\Broker\AMQPSwoole($host, $port, $httpPort, $user, $password);
```

All other APIs remain exactly the same. 