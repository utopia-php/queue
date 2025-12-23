<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue;
use Utopia\Queue\Adapter\Workerman;
use Utopia\Queue\Connection\Redis as RedisConnection;
use Utopia\Queue\Broker\Redis;

$consumer = new Redis(new RedisConnection('redis'));
$adapter = new Workerman($consumer, 12, 'wokerman');
$server = new Queue\Server($adapter);

$server->job()->inject('message')->action(handleRequest(...));

$server
    ->error()
    ->inject('error')
    ->action(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    });

$server->workerStart()->action(function () {
    echo 'Worker Started' . PHP_EOL;
});

$server->workerStop()->action(function () {
    echo 'Worker Stopped' . PHP_EOL;
});

$server->start();
