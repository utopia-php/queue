<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue;
use Utopia\Queue\Message;

$connection = new Queue\Connection\RedisSwoole('redis');
$adapter = new Queue\Adapter\Swoole($connection, 12, 'swoole');
$server = new Queue\Server($adapter);

$server->job()
    ->inject('message')
    ->action(function (Message $message) {
        handleRequest($message);
    });

$server
    ->error(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    })
    ->workerStart(function () {
        echo "Worker Started" . PHP_EOL;
    })
    ->start();
