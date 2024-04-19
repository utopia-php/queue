<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\DI\Container;
use Utopia\Queue;
use Utopia\Queue\Message;
use Utopia\Queue\Server;

$container = new Container();
$connection = new Queue\Connection\Redis('redis');
$adapter = new Queue\Adapter\Workerman($connection, 12, 'workerman');
$server = new Server($adapter);
$server->setContainer($container);

Server::job()
    ->inject('message')
    ->action(function (Message $message) {
        handleRequest($message);
    });

Server::error()
    ->inject('error')
    ->action(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    });

$server->start();
