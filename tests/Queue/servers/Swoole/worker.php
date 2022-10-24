<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue;
use Utopia\Queue\Message;
use Utopia\Tests\Connection;


$redis = new Connection('redis', 6379);
$connection = new Queue\Connection\Redis($redis->get());
$adapter = new Queue\Adapter\Swoole($connection, 12, 'swoole');
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
    ->workerStart(function () {
        echo "Worker Started" . PHP_EOL;
    })
    ->start();
