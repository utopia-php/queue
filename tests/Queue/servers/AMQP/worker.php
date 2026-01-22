<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue\Broker\AMQP;
use Utopia\Queue\Adapter\Swoole;
use Utopia\Queue\Server;

$consumer = new AMQP(host: 'amqp', port: 5672, user: 'amqp', password: 'amqp');
$adapter = new Swoole($consumer, 12, 'amqp');
$server = new Server($adapter);

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
