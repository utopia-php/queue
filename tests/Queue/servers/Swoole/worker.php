<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';

use Utopia\Queue;
use Utopia\Queue\Job;

$connection = new Queue\Connection\RedisConnection('redis');
$adapter = new Queue\Adapter\SwooleAdapter(12, $connection);
$server = new Queue\Server($adapter, 'test');
$server
    ->error(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    })
    ->onStart(function() {
        echo "Queue Server started". PHP_EOL;
    })
    ->onJob(function (Job $job) {
        echo "---".PHP_EOL;
        echo "PID: {$job->getPid()}".PHP_EOL;
        echo "Queue: {$job->getQueue()}".PHP_EOL;
        echo "Timestamp: {$job->getTimestamp()}".PHP_EOL;
        echo "Payload: ". PHP_EOL;
        var_dump($job->getPayload());
    })
    ->start();
