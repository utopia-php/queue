<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';

use Utopia\Queue;

$connection = new Queue\Connection\RedisConnection('redis');
$adapter = new Queue\Adapter\SwooleAdapter(4, $connection);
$server = new Queue\Server($adapter, 'test');
$server
    ->error(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    })
    ->onStart(function() {
        echo "Queue Server started". PHP_EOL;
    })
    ->onJob(function (array $payload) {
        var_dump($payload);
    })
    ->start();
