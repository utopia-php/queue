<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue;
use Utopia\Queue\Message;

$connection = new Queue\Connection\RedisCluster(['redis-cluster-0:6379', 'redis-cluster-1:6379', 'redis-cluster-2:6379']);
$consumer = new Queue\Broker\Redis($connection);
$adapter = new Queue\Adapter\Swoole($consumer, 12, 'swoole-redis-cluster');
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
