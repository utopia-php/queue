<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';

use Utopia\Queue;
use Utopia\Queue\Job;

$connection = new Queue\Connection\RedisConnection('redis');
$adapter = new Queue\Adapter\SwooleAdapter($connection, 12, 'test');
$server = new Queue\Server($adapter);
$server
    ->error(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    })
    ->onStart(function() {
        echo "Queue Server started". PHP_EOL;
    })
    ->onJob(function (Job $job) {
        if (array_key_exists('stop', $job->getPayload())) {
            throw new Exception("Error Processing Request", 1);
        }
    })
    ->start();
