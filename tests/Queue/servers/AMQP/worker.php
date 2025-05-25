<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue;
use Utopia\Queue\Message;

$consumer = new Queue\Broker\AMQP(host: 'amqp', port: 5672, user: 'amqp', password: 'amqp');
$adapter = new Queue\Adapter\Swoole($consumer, 12, 'amqp');
$server = new Queue\Server($adapter);

$server->job()
    ->inject('message')
    ->action(function (Message $message) {
        handleRequest($message);
    });

$server
    ->error()
    ->inject('error')
    ->action(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    });

$server
    ->workerStart()
    ->action(function () {
        echo "Worker Started" . PHP_EOL;
    });

$server
    ->workerStop()
    ->action(function () {
        echo "Worker Stopped" . PHP_EOL;
    });

$server->start();
