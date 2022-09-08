<?php

namespace Utopia\Queue;

use Throwable;
use Utopia\CLI\Console;
use Exception;
use Utopia\Validator;

class Server
{
    /**
     * Callbacks that will be executed when an error occurs
     *
     * @var array
     */
    protected array $errorCallbacks = [];
    protected Adapter $adapter;
    protected Job $job;

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

    public function job(): Job
    {
        $this->job = new Job();
        return $this->job;
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
     * Starts the Queue Server
     * @param callable $callback
     * @return self
     */
    public function start(callable $callback): self
    {
        try {
            $this->adapter->start(function () use ($callback) {
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
    public function workerStart(callable $callback): self
    {
        try {
            $this->adapter->workerStart(function (string $workerId) use ($callback) {
                Console::success("[Worker] Worker {$workerId} is ready!");
                call_user_func($callback);
                while (true) {
                    /**
                     * Waiting for next Job.
                     */
                    $nextMessage = $this->adapter->connection->rightPopArray("{$this->adapter->namespace}.queue.{$this->adapter->queue}", 5);

                    if (!$nextMessage) {
                        continue;
                    }

                    $nextMessage['timestamp'] = \intval($nextMessage['timestamp']);

                    $message = new Message($nextMessage);

                    self::setResource('message', fn () => $message);

                    Console::info("[Job] Received Job ({$message->getPid()}).");

                    /**
                     * Move Job to Jobs and it's PID to the processing list.
                     */
                    $this->adapter->connection->setArray("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$message->getPid()}", $nextMessage);
                    $this->adapter->connection->leftPush("{$this->adapter->namespace}.processing.{$this->adapter->queue}", $message->getPid());

                    /**
                     * Increment Total Jobs Received from Stats.
                     */
                    $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.total");

                    try {
                        /**
                         * Increment Processing Jobs from Stats.
                         */
                        $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.processing");

                        \call_user_func_array($this->job->getAction(), $this->getArguments($message->getPayload()));

                        /**
                         * Remove Jobs if successful.
                         */
                        $this->adapter->connection->remove("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$message->getPid()}");

                        /**
                         * Increment Successful Jobs from Stats.
                         */
                        $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.success");

                        Console::success("[Job] ({$message->getPid()}) successfully run.");
                    } catch (\Throwable $th) {
                        /**
                         * Move failed Job to Failed list.
                         */
                        $this->adapter->connection->leftPush("{$this->adapter->namespace}.failed.{$this->adapter->queue}", $message->getPid());

                        /**
                         * Increment Failed Jobs from Stats.
                         */
                        $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.failed");

                        Console::error("[Job] ({$message->getPid()}) failed to run.");
                        Console::error("[Job] ({$message->getPid()}) {$th->getMessage()}");
                    } finally {
                        /**
                         * Remove Job from Processing.
                         */
                        $this->adapter->connection->listRemove("{$this->adapter->namespace}.processing.{$this->adapter->queue}", $message->getPid());

                        /**
                         * Decrease Processing Jobs from Stats.
                         */
                        $this->adapter->connection->decrement("{$this->adapter->namespace}.stats.{$this->adapter->queue}.processing");
                    }
                }
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
    // public function onJob(): self
    // {
    //     try {
    //         $this->adapter->onJob(function () {
    //             while (true) {
    //                 /**
    //                  * Waiting for next Job.
    //                  */
    //                 $nextMessage = $this->adapter->connection->rightPopArray("{$this->adapter->namespace}.queue.{$this->adapter->queue}", 5);

    //                 if (!$nextMessage) {
    //                     continue;
    //                 }

    //                 $nextMessage['timestamp'] = \intval($nextMessage['timestamp']);

    //                 $message = new Message($nextMessage);

    //                 self::setResource('message', fn () => $message);

    //                 Console::info("[Job] Received Job ({$message->getPid()}).");

    //                 /**
    //                  * Move Job to Jobs and it's PID to the processing list.
    //                  */
    //                 $this->adapter->connection->setArray("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$message->getPid()}", $nextMessage);
    //                 $this->adapter->connection->leftPush("{$this->adapter->namespace}.processing.{$this->adapter->queue}", $message->getPid());

    //                 /**
    //                  * Increment Total Jobs Received from Stats.
    //                  */
    //                 $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.total");

    //                 try {
    //                     /**
    //                      * Increment Processing Jobs from Stats.
    //                      */
    //                     $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.processing");

    //                     \call_user_func_array($this->job->getAction(), $this->getArguments($message->getPayload()));

    //                     /**
    //                      * Remove Jobs if successful.
    //                      */
    //                     $this->adapter->connection->remove("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$message->getPid()}");

    //                     /**
    //                      * Increment Successful Jobs from Stats.
    //                      */
    //                     $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.success");

    //                     Console::success("[Job] ({$message->getPid()}) successfully run.");
    //                 } catch (\Throwable $th) {
    //                     /**
    //                      * Move failed Job to Failed list.
    //                      */
    //                     $this->adapter->connection->leftPush("{$this->adapter->namespace}.failed.{$this->adapter->queue}", $message->getPid());

    //                     /**
    //                      * Increment Failed Jobs from Stats.
    //                      */
    //                     $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.failed");

    //                     Console::error("[Job] ({$message->getPid()}) failed to run.");
    //                     Console::error("[Job] ({$message->getPid()}) {$th->getMessage()}");
    //                 } finally {
    //                     /**
    //                      * Remove Job from Processing.
    //                      */
    //                     $this->adapter->connection->listRemove("{$this->adapter->namespace}.processing.{$this->adapter->queue}", $message->getPid());

    //                     /**
    //                      * Decrease Processing Jobs from Stats.
    //                      */
    //                     $this->adapter->connection->decrement("{$this->adapter->namespace}.stats.{$this->adapter->queue}.processing");
    //                 }
    //             }
    //         });
    //     } catch (Throwable $error) {
    //         foreach ($this->errorCallbacks as $errorCallback) {
    //             $errorCallback($error, "onJob");
    //         }
    //     }

    //     return $this;
    // }

    /**
     * Get Arguments
     *
     * @param array $payload
     * @return array
     * @throws Exception
     */
    protected function getArguments(array $payload = []): array
    {
        $arguments = [];
        foreach ($this->job->getParams() as $key => $param) { // Get value from route or request object
            $value = $payload[$key] ?? $param['default'];
            $value = ($value === '' || is_null($value)) ? $param['default'] : $value;

            $this->validate($key, $param, $value);
            $this->job->setParamValue($key, $value);
            $arguments[$param['order']] = $value;
        }

        foreach ($this->job->getInjections() as $key => $injection) {
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
