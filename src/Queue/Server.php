<?php

namespace Utopia\Queue;

use Throwable;
use Utopia\Hook;
use Utopia\CLI\Console;
use Exception;
use Utopia\Validator;

/**
 * Utopia PHP Queue
 *
 * @package Utopia\Queue
 *
 * @link https://github.com/utopia-php/Queue
 * @author Torsten Dittmann <torsten@appwrite.io>
 * @version 1.0 RC1
 * @license The MIT License (MIT) <http://www.opensource.org/licenses/mit-license.php>
 */
class Server
{
    /**
     * Callbacks that will be executed when an error occurs
     *
     * @var array
     */
    protected array $errorCallbacks = [];
    protected Adapter $adapter;

    /**
     * @var array
     */
    protected array $resources = [
        'error' => null,
    ];

    /**
     * @var array
     */
    protected static array $resourcesCallbacks = [];

    /**
     * Creates an instance of a Queue server.
     * @param Adapter $adapter
     */
    public function __construct(Adapter $adapter)
    {
        $this->adapter = $adapter;
    }

    /**
    * If a resource has been created return it, otherwise create it and then return it
    *
    * @param string $name
    * @param bool $fresh
    * @return mixed
    * @throws Exception
    */
    public function getResource(string $name, bool $fresh = false): mixed
    {
        if ($name === 'utopia') {
            return $this;
        }

        if (!\array_key_exists($name, $this->resources) || $fresh || self::$resourcesCallbacks[$name]['reset']) {
            if (!\array_key_exists($name, self::$resourcesCallbacks)) {
                throw new Exception('Failed to find resource: "' . $name . '"');
            }

            $this->resources[$name] = \call_user_func_array(
                self::$resourcesCallbacks[$name]['callback'],
                $this->getResources(self::$resourcesCallbacks[$name]['injections'])
            );
        }

        self::$resourcesCallbacks[$name]['reset'] = false;

        return $this->resources[$name];
    }

    /**
     * Get Resources By List
     *
     * @param array $list
     * @return array
     */
    public function getResources(array $list): array
    {
        $resources = [];

        foreach ($list as $name) {
            $resources[$name] = $this->getResource($name);
        }

        return $resources;
    }

    /**
     * Set a new resource callback
     *
     * @param string $name
     * @param callable $callback
     * @param array $injections
     *
     * @throws Exception
     *
     * @return void
     */
    public static function setResource(string $name, callable $callback, array $injections = []): void
    {
        if ($name === 'utopia') {
            throw new Exception("'utopia' is a reserved keyword.", 500);
        }
        self::$resourcesCallbacks[$name] = ['callback' => $callback, 'injections' => $injections, 'reset' => true];
    }


    /**
     * Starts the Queue server.
     * @return void
     */
    public function start(): void
    {
        try {
            $this->adapter->start();
        } catch (Throwable $error) {
            foreach ($this->errorCallbacks as $errorCallback) {
                $errorCallback($error, "start");
            }
        }
    }

    /**
     * Shuts down the Queue server.
     * @return void
     */
    public function shutdown(): void
    {
        try {
            $this->adapter->shutdown();
        } catch (Throwable $error) {
            foreach ($this->errorCallbacks as $errorCallback) {
                $errorCallback($error, "shutdown");
            }
        }
    }

    /**
     * Is called when the Server starts.
     * @param callable $callback
     * @return self
     */
    public function onStart(callable $callback): self
    {
        try {
            $this->adapter->onStart(function () use ($callback) {
                Console::success("[Worker] Queue Workers are starting");
                call_user_func($callback);
            });
        } catch (Throwable $error) {
            foreach ($this->errorCallbacks as $errorCallback) {
                $errorCallback($error, "onStart");
            }
        }
        return $this;
    }

    /**
     * Is called when a Worker starts.
     * @param callable $callback
     * @return self
     */
    public function onWorkerStart(callable $callback): self
    {
        try {
            $this->adapter->onWorkerStart(function (string $workerId) use ($callback) {
                Console::success("[Worker] Worker {$workerId} is ready!");
                call_user_func($callback);
            });
        } catch (Throwable $error) {
            foreach ($this->errorCallbacks as $errorCallback) {
                $errorCallback($error, "onWorkerStart");
            }
        }

        return $this;
    }

    /**
     * Is called when a Worker receives a Job.
     * @return self
     */
    public function onJob(): self
    {
        try {
            $this->adapter->onJob(function () {
                while (true) {
                    /**
                     * Waiting for next Job.
                     */
                    $nextJob = $this->adapter->connection->rightPopArray("{$this->adapter->namespace}.queue.{$this->adapter->queue}", 5);

                    if (!$nextJob) {
                        continue;
                    }

                    $nextJob['timestamp'] = \intval($nextJob['timestamp']);

                    $job = new Job($nextJob);
                    Console::info("[Job] Received Job ({$job->getPid()}).");

                    /**
                     * Move Job to Jobs and it's PID to the processing list.
                     */
                    $this->adapter->connection->setArray("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$job->getPid()}", $nextJob);
                    $this->adapter->connection->leftPush("{$this->adapter->namespace}.processing.{$this->adapter->queue}", $job->getPid());

                    /**
                     * Increment Total Jobs Received from Stats.
                     */
                    $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.total");

                    try {
                        /**
                         * Increment Processing Jobs from Stats.
                         */
                        $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.processing");

                        \call_user_func_array($job->getAction(), $this->getArguments($job));

                        /**
                         * Remove Jobs if successful.
                         */
                        $this->adapter->connection->remove("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$job->getPid()}");

                        /**
                         * Increment Successful Jobs from Stats.
                         */
                        $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.success");

                        Console::success("[Job] ({$job->getPid()}) successfully run.");
                    } catch (\Throwable $th) {
                        /**
                         * Move failed Job to Failed list.
                         */
                        $this->adapter->connection->leftPush("{$this->adapter->namespace}.failed.{$this->adapter->queue}", $job->getPid());

                        /**
                         * Increment Failed Jobs from Stats.
                         */
                        $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.failed");

                        Console::error("[Job] ({$job->getPid()}) failed to run.");
                        Console::error("[Job] ({$job->getPid()}) {$th->getMessage()}");
                    } finally {
                        /**
                         * Remove Job from Processing.
                         */
                        $this->adapter->connection->listRemove("{$this->adapter->namespace}.processing.{$this->adapter->queue}", $job->getPid());

                        /**
                         * Decrease Processing Jobs from Stats.
                         */
                        $this->adapter->connection->decrement("{$this->adapter->namespace}.stats.{$this->adapter->queue}.processing");
                    }
                }
            });
        } catch (Throwable $error) {
            foreach ($this->errorCallbacks as $errorCallback) {
                $errorCallback($error, "onJob");
            }
        }

        return $this;
    }

    /**
     * Get Arguments
     *
     * @param Job $job
     * @return array
     * @throws Exception
     */
    protected function getArguments(Job $job): array
    {
        $arguments = [];
        $payload = $job->getPayload();
        foreach ($job->getParams() as $key => $param) { // Get value from route or request object
            $value = $payload[$key] ?? $param['default'];
            $value = ($value === '' || is_null($value)) ? $param['default'] : $value;

            $this->validate($key, $param, $value);
            $job->setParamValue($key, $value);
            $arguments[$param['order']] = $value;
        }

        foreach ($job->getInjections() as $key => $injection) {
            $arguments[$injection['order']] = $this->getResource($injection['name']);
        }

        return $arguments;
    }

    /**
     * Validate Param
     *
     * Creates an validator instance and validate given value with given rules.
     *
     * @param string $key
     * @param array $param
     * @param mixed $value
     *
     * @throws Exception
     *
     * @return void
     */
    protected function validate(string $key, array $param, mixed $value): void
    {
        if ('' !== $value && !is_null($value)) {
            $validator = $param['validator']; // checking whether the class exists

            if (\is_callable($validator)) {
                $validator = \call_user_func_array($validator, $this->getResources($param['injections']));
            }

            if (!$validator instanceof Validator) { // is the validator object an instance of the Validator class
                throw new Exception('Validator object is not an instance of the Validator class', 500);
            }

            if (!$validator->isValid($value)) {
                throw new Exception('Invalid ' .$key . ': ' . $validator->getDescription(), 400);
            }
        } elseif (!$param['optional']) {
            throw new Exception('Param "' . $key . '" is not optional.', 400);
        }
    }

    /**
     * Register callback. Will be executed when error occurs.
     * @param callable $callback
     * @return self
     */
    public function error(callable $callback): self
    {
        \array_push($this->errorCallbacks, $callback);
        return $this;
    }
}
