# Utopia Queue

[![Build Status](https://travis-ci.com/utopia-php/queue.svg?branch=main)](https://travis-ci.com/utopia-php/queue)
![Total Downloads](https://img.shields.io/packagist/dt/utopia-php/queue.svg)
[![Discord](https://img.shields.io/discord/564160730845151244?label=discord)](https://appwrite.io/discord)

Utopia Queue is a powerful Queue library. This library is aiming to be as simple and easy to learn and use. This library is maintained by the [Appwrite team](https://appwrite.io).

Although this library is part of the [Utopia Framework](https://github.com/utopia-php/framework) project it is dependency free and can be used as standalone with any other PHP project or framework.

## Getting Started

Install using composer:
```bash
composer require utopia-php/queue
```

Init in your application:
```php
<?php

require_once __DIR__ . '/../../vendor/autoload.php';

// Create a worker using swoole adapter
use Utopia\Queue;
use Utopia\Queue\Message;

$connection = new Queue\Connection\RedisSwoole('redis');
$adapter = new Queue\Adapter\Swoole($connection, 12, 'swoole');
$server = new Queue\Server($adapter);

$server->job()
    ->inject('message')
    ->action(function (Message $message) {
        var_dump($message);
    });

$server
    ->error()
    ->inject('error')
    ->action(function ($error) {
        echo $error->getMessage() . PHP_EOL;
    });

$server
    ->workerStart(function () {
        echo "Worker Started" . PHP_EOL;
    })
    ->start();


// Enqueue messages to the worker using swoole adapter
$connection = new RedisSwoole('redis', 6379);
run(function () use ($connection) {
    $client = new Client('swoole', $connection);
    go(function () use ($client) {
        $client->resetStats();

        $client->enqueue([
            'type' => 'test_number',
            'value' => 123
        ]);
    });
});
```

## System Requirements

Utopia Framework requires PHP 8.0 or later. We recommend using the latest PHP version whenever possible.

## Copyright and license

The MIT License (MIT) [http://www.opensource.org/licenses/mit-license.php](http://www.opensource.org/licenses/mit-license.php)
