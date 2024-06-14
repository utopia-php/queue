<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\Queue;
use Utopia\Queue\Concurrency\Manager;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Message;

class BuildsConcurrencyManager extends Manager
{
    public function getConcurrencyKey(Message $message): string
    {
        return $message['payload']['$id'] ?? throw new Exception("Concurrency key not found.");
    }
}

$connection = new Redis('redis');
$adapter = new Queue\Adapter\Swoole($connection, 12, 'swoole');
$server = new Queue\Server($adapter);

$server->setConcurrencyManager('builds', new BuildsConcurrencyManager('builds', 2, $connection));

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

$server->start();
