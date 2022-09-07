<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\Queue\Client;
use Utopia\Queue\Connection\Redis;
use Utopia\Queue\Connection\RedisSwoole;
use Utopia\Queue\Job;
use Utopia\Validator\ArrayList;
use Utopia\Validator\Boolean;
use Utopia\Validator\FloatValidator;
use Utopia\Validator\Integer;
use Utopia\Validator\Text;

use function Swoole\Coroutine\go;
use function Swoole\Coroutine\run;

class SwooleTest extends TestCase
{
    private $jobs;

    public function setUp(): void
    {
        $this->jobs = [];

        $job = new Job();
        $job->param('value', '', new Text(0));
        $job->setTimestamp(time());
        $job->setPayload(['value' => 'lorem ipsum']);
        $job->action(function ($value) {
            assert(is_string($value));
        });
        $this->jobs[] = $job;

        $job = new Job();
        $job->param('value', null, new Integer());
        $job->setTimestamp(time());
        $job->setPayload(['value' => 123]);
        $job->action(function ($value) {
            assert(is_numeric($value));
        });
        $this->jobs[] = $job;

        $job = new Job();
        $job->setTimestamp(time());
        $job->param('value', null, new FloatValidator());
        $job->setPayload(['value' => 123.456]);
        $job->action(function ($value) {
            assert(is_numeric($value));
        });
        $this->jobs[] = $job;

        $job = new Job();
        $job->setTimestamp(time());
        $job->param('value', true, new Boolean());
        $job->setPayload(['value' => true]);
        $job->action(function ($value) {
            assert(is_bool($value));
        });
        $this->jobs[] = $job;

        $job = new Job();
        $job->setTimestamp(time());
        $job->param('value', null, new Text(0), '', true);
        $job->setPayload(['value' => null]);
        $job->action(function ($value) {
            assert(is_null($value));
        });
        $this->jobs[] = $job;

        $job = new Job();
        $job->setTimestamp(time());
        $job->param('value', null, new ArrayList(new Integer()), '', true);
        $job->setPayload(['value' => [1,2,3]]);
        $job->action(function ($value) {
            assert(is_array($value));
            assert(count($value) === 3);
            assert(empty(array_diff([1,2,3], $value)));
        });
        $this->jobs[] = $job;

        $job = new Job();
        $job->setTimestamp(time());
        $job->param('value', [], new ArrayList(new Integer()), '', true);
        $job->setPayload(['value' => [
            'string' => 'ipsum',
            'number' => 123,
            'bool' => true,
            'null' => null
        ]]);
        $job->action(function ($value) {
            assert(is_array($value));
            assert(count($value) === 3);
            assert($value['string'] === 'ipsum');
            assert($value['number'] === 123);
            assert($value['bool'] === true);
            assert($value['null'] === null);
        });
        $this->jobs[] = $job;

        $job = new Job();
        $job->setTimestamp(time());
        $job->setPayload([]);
        $job->action(function () {
            assert(false);
        });
        $this->jobs[] = $job;
    }

    public function testEvents(): void
    {
        $connection = new Redis('redis', 6379);

        $client = new Client('workerman', $connection);
        $client->resetStats();

        foreach ($this->jobs as $job) { /** @var Job $job */
            $newJob = $client->job();
            $newJob->setPayload($job->getPayload());
            $newJob->setTimestamp($job->getTimestamp());
            foreach ($job->getParams() as $key => $param) {
                $newJob->param($key, $param['default'], $param['validator'], $param['description'], $param['optional'], $param['injections']);
            }
            $newJob->action($job->getAction());
            var_dump($job->asArray());
            var_dump($newJob->asArray());
            $this->assertTrue($client->enqueue($newJob));
        }

        sleep(1);

        $this->assertEquals(8, $client->sumTotalJobs());
        $this->assertEquals(0, $client->getQueueSize());
        $this->assertEquals(0, $client->sumProcessingJobs());
        $this->assertEquals(1, $client->sumFailedJobs());
        $this->assertEquals(7, $client->sumSuccessfulJobs());
    }

    public function testSwoole(): void
    {
        $connection = new RedisSwoole('redis', 6379);

        run(function () use ($connection) {
            $client = new Client('swoole', $connection);
            go(function () use ($client) {
                $client->resetStats();
                $job = $this->jobs[0];
                // foreach ($this->jobs as $job) { /** @var Job $job */
                    $newJob = $client->job();
                    $newJob->setTimestamp($job->getTimestamp());
                    $newJob->setPayload($job->getPayload());
                    foreach ($job->getParams() as $key => $param) {
                        $newJob->param($key, $param['default'], $param['validator'], $param['description'], $param['optional'], $param['injections']);
                    }
                    // var_dump($job->asArray());
                    $newJob->action($job->getAction());
                    var_dump($job->getAction());
                    // var_dump($newJob->asArray()['action']);
                    $this->assertTrue($client->enqueue($newJob));
                // }
                sleep(1);

                $this->assertEquals(8, $client->sumTotalJobs());
                $this->assertEquals(0, $client->sumProcessingJobs());
                $this->assertEquals(1, $client->sumFailedJobs());
                $this->assertEquals(7, $client->sumSuccessfulJobs());
            });
        });
    }
}
