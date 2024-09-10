<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\DI\Container;
use Utopia\Queue;
use Utopia\Queue\Concurrency\Manager;
use Utopia\Queue\Message;
use Utopia\Queue\Worker;
use Utopia\Servers\Validator;

class Text extends Validator
{
    /**
     * Get Description
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return 'Value must be a valid number';
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return '';
    }

    /**
     * Is valid
     *
     * Validation will pass when $value is numeric.
     *
     * @param  mixed  $value
     * @return bool
     */
    public function isValid(mixed $value): bool
    {
        return true;
    }
}



$container = new Container();
$connection = new Queue\Adapter\Swoole\Redis('redis');
$adapter = new Queue\Adapter\Swoole\Server($connection, 1, 'swoole');
$server = new Queue\Worker($adapter);
$server->setConcurrencyManager(new TestConcurrencyManager($connection, 5));
$server->setContainer($container);

// Server::init()
//     ->param('id', 'default', new Text(), 'Message ID', true)
//     ->inject('message')
//     ->action(function ($id, $message) {
//         var_dump($id);
//         var_dump($message);
//         echo "Job init" . PHP_EOL;
//     });

// Server::job()
//     ->param('id', 'default', new Text(), 'Message ID', true)
//     ->inject('message')
//     ->action(function ($id, $message) {
//         var_dump($id);
//         var_dump($message);
//         echo "Job start" . PHP_EOL;
//     });

Worker::job()
    ->inject('message')
    ->action(function (Message $message) {
        handleRequest($message);
    });

Worker::error()
    ->inject('error')
    ->action(function ($th) {
        echo $th->getMessage() . PHP_EOL;
    });

$server->start();
