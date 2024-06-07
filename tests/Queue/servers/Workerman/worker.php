<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\DI\Container;
use Utopia\Queue;
use Utopia\Queue\Message;
use Utopia\Queue\Worker;

$container = new Container();
$connection = new Queue\Connection\Redis('redis');
$adapter = new Queue\Adapter\Workerman\Server($connection, 12, 'workerman');
$server = new Worker($adapter);
$server->setContainer($container);

Worker::job()
    ->inject('message')
    ->action(function (Message $message) {
        handleRequest($message);
    });

Worker::error()
    ->inject('error')
    ->action(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    });

$server->start();
