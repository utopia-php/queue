<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue\Broker\Redis;
use Utopia\Queue\Connection\RedisCluster;
use Utopia\Queue\Adapter\Swoole;
use Utopia\Queue\Server;
use Utopia\Validator\Text;

$consumer = new Redis(
    new RedisCluster([
        'redis-cluster-0:6379',
        'redis-cluster-1:6379',
        'redis-cluster-2:6379',
    ]),
);
$adapter = new Swoole($consumer, 12, 'swoole-redis-cluster');
$server = new Server($adapter);

$server->job()
    ->inject('message')
    ->param(
        key: 'aliasValue',
        default: '',
        validator: new Text(length: 255, min: 0),
        description: 'alias resolution test value',
        optional: true,
        aliases: ['alias_value', 'aliased'],
    )
    ->action(handleRequest(...));

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
