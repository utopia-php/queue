<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Client;
use Utopia\Queue\Connection\RedisSwoole;

use function Swoole\Coroutine\go;
use function Swoole\Coroutine\run;

class SwooleTest extends TestCase
{
    public function setUp(): void
    {
    }

    public function testWorkerman(): void
    {
        $connection = new RedisSwoole('localhost', 6378);

        run(function () use ($connection) {
            $client = new Client('workerman', $connection);
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 123
                ]));
            });
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 'lorem'
                ]));
            });
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 'ispum'
                ]));
            });
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 'dolor'
                ]));
            });
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'stop' => true
                ]));
            });
        });
    }

    public function testSwoole(): void
    {
        $connection = new RedisSwoole('localhost', 6378);

        run(function () use ($connection) {
            $client = new Client('swoole', $connection);
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 123
                ]));
            });
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 'lorem'
                ]));
            });
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 'ispum'
                ]));
            });
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'value' => 'dolor'
                ]));
            });
            go(function () use ($client) {
                $this->assertTrue($client->enqueue([
                    'stop' => true
                ]));
            });
        });
    }
}
