<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue\Server;
use Utopia\Queue\Adapter\Swoole;
use Utopia\Queue\Concurrency\Coroutine;
use Utopia\Queue\Connection\Locking;
use Utopia\Queue\Connection\Redis as RedisConnection;
use Utopia\Queue\Broker\Redis;
use Utopia\Validator\Text;

// One connection drives the blocking receive loop; a separate, locked
// connection carries the bookkeeping for the handlers fanned out across
// coroutines.
$consumer = new Redis(
    receive: new RedisConnection('redis'),
    work: new Locking(new RedisConnection('redis')),
    executor: new Coroutine(maxCoroutines: 5),
);
$adapter = new Swoole($consumer, 12, 'swoole');
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
