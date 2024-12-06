<?php

namespace Utopia\Queue;

use Throwable;
use Utopia\CLI\Console;
use Exception;
use Utopia\Hook;
use Utopia\Telemetry\Adapter as Telemetry;
use Utopia\Telemetry\Adapter\None as NoTelemetry;
use Utopia\Telemetry\Gauge;
use Utopia\Telemetry\Histogram;
use Utopia\Validator;

class Server
{
    /**
     * Queue Adapter
     *
     * @var Adapter
     */
    protected Adapter $adapter;

    /**
     * Job
     *
     * @var Job
     */
    protected Job $job;

    /**
     * Hooks that will run when error occur
     *
     * @var array
     */
    protected array $errorHooks = [];

    /**
     * Hooks that will run before running job
     *
     * @var array
     */
    protected array $initHooks = [];

    /**
     * Hooks that will run after running job
     *
     * @var array
     */
    protected array $shutdownHooks = [];

    /**
     * Hook that is called when worker starts
     *
     * @var Hook
     */
    protected Hook $workerStartHook;

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

    private Histogram $jobWaitTime;
    private Histogram $processDuration;
    private Gauge $queueLength;

    /**
     * Creates an instance of a Queue server.
     * @param Adapter $adapter
     */
    public function __construct(Adapter $adapter)
    {
        $this->adapter = $adapter;
        $this->setTelemetry(new NoTelemetry());
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
        self::$resourcesCallbacks[$name] = ['callback' => $callback, 'injections' => $injections, 'reset' => true];
    }

    public function setTelemetry(Telemetry $telemetry)
    {
        $this->jobWaitTime = $telemetry->createHistogram(
            'messaging.process.wait.duration',
            's',
            null,
            ['ExplicitBucketBoundaries' =>  [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]]
        );

        // https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/#metric-messagingprocessduration
        $this->processDuration = $telemetry->createHistogram(
            'messaging.process.duration',
            's',
            null,
            ['ExplicitBucketBoundaries' =>  [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]]
        );

        $this->queueLength = $telemetry->createGauge('messaging.queue.length');
    }

    /**
     * Shutdown Hooks
     * @return Hook
     */
    public function shutdown(): Hook
    {
        $hook = new Hook();
        $hook->groups(['*']);
        $this->shutdownHooks[] = $hook;
        return $hook;
    }

    /**
     * Stops the Queue server.
     * @return self
     */
    public function stop(): self
    {
        try {
            $this->adapter->stop();
        } catch (Throwable $error) {
            self::setResource('error', fn () => $error);
            foreach ($this->errorHooks as $hook) {
                call_user_func_array($hook->getAction(), $this->getArguments($hook));
            }
        }
        return $this;
    }

    /**
     * Init Hooks
     *
     * @return Hook
     */
    public function init(): Hook
    {
        $hook = new Hook();
        $hook->groups(['*']);
        $this->initHooks[] = $hook;
        return $hook;
    }

    /**
     * Starts the Queue Server
     * @return self
     */
    public function start(): self
    {
        try {
            $this->adapter->workerStart(function (string $workerId) {
                Console::success("[Worker] Worker {$workerId} is ready!");
                self::setResource('workerId', fn () => $workerId);
                if (!is_null($this->workerStartHook)) {
                    call_user_func_array($this->workerStartHook->getAction(), $this->getArguments($this->workerStartHook));
                }
                while (true) {
                    $this->queueLength->record(
                        $this->adapter->connection->listSize("{$this->adapter->namespace}.queue.{$this->adapter->queue}"),
                        ['queue' => $this->adapter->queue]
                    );

                    /**
                     * Waiting for next Job.
                     */
                    $nextMessage = $this->adapter->connection->rightPopArray("{$this->adapter->namespace}.queue.{$this->adapter->queue}", 5);

                    if (!$nextMessage) {
                        continue;
                    }

                    $nextMessage['timestamp'] = (int)$nextMessage['timestamp'];

                    $message = new Message($nextMessage);

                    self::setResource('message', fn () => $message);

                    $receivedAtTimestamp = microtime(true);

                    Console::info("[Job] Received Job ({$message->getPid()}).");

                    $waitDuration = microtime(true) - $message->getTimestamp();
                    $this->jobWaitTime->record($waitDuration, ['queue' => $this->adapter->queue]);

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

                        if ($this->job->getHook()) {
                            foreach ($this->initHooks as $hook) { // Global init hooks
                                if (in_array('*', $hook->getGroups())) {
                                    $arguments = $this->getArguments($hook, $message->getPayload());
                                    \call_user_func_array($hook->getAction(), $arguments);
                                }
                            }
                        }

                        foreach ($this->job->getGroups() as $group) {
                            foreach ($this->initHooks as $hook) { // Group init hooks
                                if (in_array($group, $hook->getGroups())) {
                                    $arguments = $this->getArguments($hook, $message->getPayload());
                                    \call_user_func_array($hook->getAction(), $arguments);
                                }
                            }
                        }

                        \call_user_func_array($this->job->getAction(), $this->getArguments($this->job, $message->getPayload()));

                        /**
                         * Remove Jobs if successful.
                         */
                        $this->adapter->connection->remove("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$message->getPid()}");

                        /**
                         * Increment Successful Jobs from Stats.
                         */
                        $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.success");

                        if ($this->job->getHook()) {
                            foreach ($this->shutdownHooks as $hook) { // Global init hooks
                                if (in_array('*', $hook->getGroups())) {
                                    $arguments = $this->getArguments($hook, $message->getPayload());
                                    \call_user_func_array($hook->getAction(), $arguments);
                                }
                            }
                        }

                        foreach ($this->job->getGroups() as $group) {
                            foreach ($this->shutdownHooks as $hook) { // Group init hooks
                                if (in_array($group, $hook->getGroups())) {
                                    $arguments = $this->getArguments($hook, $message->getPayload());
                                    \call_user_func_array($hook->getAction(), $arguments);
                                }
                            }
                        }

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

                        self::setResource('error', fn () => $th);
                        foreach ($this->errorHooks as $hook) {
                            call_user_func_array($hook->getAction(), $this->getArguments($hook));
                        }
                    } finally {
                        $processDuration = microtime(true) - $receivedAtTimestamp;
                        $this->processDuration->record($processDuration, ['queue' => $this->adapter->queue]);

                        /**
                         * Remove Job from Processing.
                         */
                        $this->adapter->connection->listRemove("{$this->adapter->namespace}.processing.{$this->adapter->queue}", $message->getPid());

                        /**
                         * Decrease Processing Jobs from Stats.
                         */
                        $this->adapter->connection->decrement("{$this->adapter->namespace}.stats.{$this->adapter->queue}.processing");
                    }

                    $this->resources = [];
                }
            });

            $this->adapter->start();
        } catch (Throwable $error) {
            self::setResource('error', fn () => $error);
            foreach ($this->errorHooks as $hook) {
                call_user_func_array($hook->getAction(), $this->getArguments($hook));
            }
        }
        return $this;
    }

    /**
     * Is called when a Worker starts.
     * @return Hook
     */
    public function workerStart(): Hook
    {
        $hook = new Hook();
        $hook->groups(['*']);
        $this->workerStartHook = $hook;
        return $hook;
    }

    /**
    * Returns Worker starts hook.
    * @return Hook
    */
    public function getWorkerStart(): Hook
    {
        return $this->workerStartHook;
    }

    /**
     * Is called when a Worker stops.
     * @param callable $callback
     * @return self
     */
    public function workerStop(callable $callback = null): self
    {
        try {
            $this->adapter->workerStop(function (string $workerId) use ($callback) {
                Console::success("[Worker] Worker {$workerId} is ready!");
                if (!is_null($callback)) {
                    call_user_func($callback);
                }
            });
        } catch (Throwable $error) {
            self::setResource('error', fn () => $error);
            foreach ($this->errorHooks as $hook) {
                call_user_func_array($hook->getAction(), $this->getArguments($hook));
            }
        }

        return $this;
    }

    /**
     * Get Arguments
     *
     * @param Hook $hook
     * @param array $payload
     * @return array
     */
    protected function getArguments(Hook $hook, array $payload = []): array
    {
        $arguments = [];
        foreach ($hook->getParams() as $key => $param) { // Get value from route or request object
            $value = $payload[$key] ?? $param['default'];
            $value = ($value === '' || is_null($value)) ? $param['default'] : $value;

            $this->validate($key, $param, $value);
            $hook->setParamValue($key, $value);
            $arguments[$param['order']] = $value;
        }

        foreach ($hook->getInjections() as $key => $injection) {
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
     * Register hook. Will be executed when error occurs.
     * @return Hook
     */
    public function error(): Hook
    {
        $hook = new Hook();
        $hook->groups(['*']);
        $this->errorHooks[] = $hook;
        return $hook;
    }
}
