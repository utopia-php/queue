<?php

namespace Tests\E2E\Adapter;

use Utopia\Queue\Client;
use Utopia\Queue\Connection\Redis;

use function Co\run;

class SwooleTest extends Base
{
    protected function getClient(): Client
    {
        $connection = new Redis('redis', 6379);
        $client = new Client('swoole', $connection);

        return $client;
    }

    /**
     * @depends testRetry
     */
    protected function testSwooleConcurrency(): void
    {
        $connection = new Redis('redis', 6379);

        run(function () use ($connection) {
            $client = new Client('swoole', $connection);
            go(function () use ($client) {
                $client->resetStats();

                foreach ($this->payloads as $payload) {
                    $this->assertTrue($client->enqueue($payload));
                }

                sleep(1);

                $this->assertEquals(8, $client->sumTotalJobs());
                $this->assertEquals(0, $client->sumProcessingJobs());
                $this->assertEquals(1, $client->sumFailedJobs());
                $this->assertEquals(7, $client->sumSuccessfulJobs());
            });
        });
    }
}
