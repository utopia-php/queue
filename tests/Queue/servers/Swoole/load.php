<?php

require_once __DIR__ . '/../../../../vendor/autoload.php';
require_once __DIR__ . '/../tests.php';

use Utopia\CLI\Console;
use Utopia\DI\Container;
use Utopia\Queue;
use Utopia\Queue\Worker;
use Utopia\Queue\Client;
use Utopia\Queue\Connection\Redis;
use Utopia\Servers\Validator;

function formatTimeDiff($timeDiff)
{
    $hours = floor($timeDiff / 3600);
    $minutes = floor(($timeDiff % 3600) / 60);
    $seconds = $timeDiff % 60;

    return sprintf("%02d:%02d:%02d", $hours, $minutes, $seconds);
}

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
$jobs = 10000000;
$sleep = 0;
$connection = new Redis('redis', 6379);
$client = new Client('swoole', $connection);

Console::log('Started loading queue with ' . $jobs . ' jobs');

for ($i = 0; $i < $jobs; $i++) {
    $time = time();
    $client->enqueue(['id' => $i, 'time' => $time]);
    // break;
    if ($i % 10000 === 0) {
        Console::log('Loaded ' . $i . ' jobs');
    }
}

Console::log('Finished loading queue');

sleep($sleep);

$container = new Container();
// $connection = new Queue\Adapter\Swoole\Redis('redis');
$connection = new Queue\Connection\Redis('redis');
$adapter = new Queue\Adapter\Swoole\Server($connection, 1000, 'swoole');
$server = new Queue\Worker($adapter);
$server->setContainer($container);

Worker::job()
    ->param('id', 0, new Text(), 'Message ID', true)
    ->param('time', 0, new Text(), 'Message ID', true)
    ->inject('message')
    ->action(function ($id, $time, $message) use ($jobs, $sleep) {
        // usleep(100000);
        if ($time) {
            $currentTimestamp = time();
            $timeDiff = ($currentTimestamp - $time) - $sleep;
            $humanReadableDiff = formatTimeDiff($timeDiff);
            Console::warning('Time took to process until job #'.$id . '/'. $jobs . ' jobs: '.$humanReadableDiff);
        }
    });

Worker::error()
    ->inject('error')
    ->action(function ($th) {
        echo $th->getMessage() . PHP_EOL;
        exit();
    });

$server->start();
