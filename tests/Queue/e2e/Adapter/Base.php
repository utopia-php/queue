<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Client;

abstract class Base extends TestCase
{
    protected array $payloads;

    public function setUp(): void
    {
        $this->payloads = [];
        $this->payloads[] = [
            'type' => 'test_string',
            'value' => 'lorem ipsum'
        ];
        $this->payloads[] = [
            'type' => 'test_number',
            'value' => 123
        ];
        $this->payloads[] = [
            'type' => 'test_number',
            'value' => 123.456
        ];
        $this->payloads[] = [
            'type' => 'test_bool',
            'value' => true
        ];
        $this->payloads[] = [
            'type' => 'test_null',
            'value' => null
        ];
        $this->payloads[] = [
            'type' => 'test_array',
            'value' => [
                1,
                2,
                3
            ]
        ];
        $this->payloads[] = [
            'type' => 'test_assoc',
            'value' => [
                'string' => 'ipsum',
                'number' => 123,
                'bool' => true,
                'null' => null
            ]
        ];
        $this->payloads[] = [
            'type' => 'test_exception'
        ];
    }

    /**
     * @return Client
     */
    abstract protected function getClient(): Client;

    public function testEvents(): void
    {
        $client = $this->getClient();
        $client->resetStats();

        foreach ($this->payloads as $payload) {
            $this->assertTrue($client->enqueue($payload));
        }

        sleep(1);

        $this->assertEquals(8, $client->sumTotalJobs());
        $this->assertEquals(0, $client->getQueueSize());
        $this->assertEquals(0, $client->sumProcessingJobs());
        $this->assertEquals(1, $client->sumFailedJobs());
        $this->assertEquals(7, $client->sumSuccessfulJobs());
    }

    /**
     * @depends testEvents
     */
    public function testRetry(): void
    {
        $client = $this->getClient();
        $client->resetStats();

        $client->enqueue([
            'type' => 'test_exception',
            'id' => 1
        ]);
        $client->enqueue([
            'type' => 'test_exception',
            'id' => 2
        ]);
        $client->enqueue([
            'type' => 'test_exception',
            'id' => 3
        ]);
        $client->enqueue([
            'type' => 'test_exception',
            'id' => 4
        ]);

        sleep(1);

        $this->assertEquals(4, $client->sumTotalJobs());
        $this->assertEquals(0, $client->sumProcessingJobs());
        $this->assertEquals(4, $client->sumFailedJobs());
        $this->assertEquals(0, $client->sumSuccessfulJobs());

        $client->resetStats();

        $client->retry();

        sleep(1);

        // Retry will retry ALL failed jobs regardless of if they are still tracked in stats
        // Meaning this test has 5 failed jobs due to the previous tests.
        $this->assertEquals(5, $client->sumTotalJobs());
        $this->assertEquals(0, $client->sumProcessingJobs());
        $this->assertEquals(5, $client->sumFailedJobs());
        $this->assertEquals(0, $client->sumSuccessfulJobs());

        $client->resetStats();

        $client->retry(2);

        sleep(1);

        $this->assertEquals(2, $client->sumTotalJobs());
        $this->assertEquals(0, $client->sumProcessingJobs());
        $this->assertEquals(2, $client->sumFailedJobs());
        $this->assertEquals(0, $client->sumSuccessfulJobs());
    }
}
