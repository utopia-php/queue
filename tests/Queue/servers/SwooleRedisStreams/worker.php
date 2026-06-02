<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue\Server;
use Utopia\Queue\Adapter\Swoole;
use Utopia\Queue\Connection\RedisStream;
use Utopia\Queue\Broker\RedisStreams;

$connection = new RedisStream('redis');
$consumer = new RedisStreams($connection);
$adapter = new Swoole($consumer, 12, 'swoole-redis-streams');
$server = new Server($adapter);

$server->job()->inject('message')->action(handleRequest(...));

$server
    ->error()
    ->inject('error')
    ->action(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    });

$server->workerStart()->action(function () {
    echo 'Worker Started (Redis Streams)' . PHP_EOL;
});

$server->workerStop()->action(function () {
    echo 'Worker Stopped (Redis Streams)' . PHP_EOL;
});

$server->start();
