<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Publisher;
use Utopia\Queue\Queue;

use function Co\run;

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
    }

    /**
     * @return Publisher
     */
    abstract protected function getPublisher(): Publisher;

    abstract protected function getQueue(): Queue;

    public function testEvents(): void
    {
        $publisher = $this->getPublisher();

        foreach ($this->payloads as $payload) {
            $this->assertTrue($publisher->enqueue($this->getQueue(), $payload));
        }

        sleep(1);
    }

    public function testConcurrency(): void
    {
        run(function () {
            $publisher = $this->getPublisher();
            go(function () use ($publisher) {
                foreach ($this->payloads as $payload) {
                    $this->assertTrue($publisher->enqueue($this->getQueue(), $payload));
                }

                sleep(1);
            });
        });
    }

    /**
     * @depends testEvents
     */
    public function testRetry(): void
    {
        $publisher = $this->getPublisher();

        $publisher->enqueue($this->getQueue(), [
            'type' => 'test_exception',
            'id' => 1
        ]);
        $publisher->enqueue($this->getQueue(), [
            'type' => 'test_exception',
            'id' => 2
        ]);
        $publisher->enqueue($this->getQueue(), [
            'type' => 'test_exception',
            'id' => 3
        ]);
        $publisher->enqueue($this->getQueue(), [
            'type' => 'test_exception',
            'id' => 4
        ]);

        sleep(1);
        $publisher->retry($this->getQueue());
        sleep(1);
        $publisher->retry($this->getQueue(), 2);
        sleep(1);
    }
}
