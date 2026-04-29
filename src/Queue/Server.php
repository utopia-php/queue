<?php

namespace Utopia\Queue;

use Exception;
use Throwable;
use Utopia\DI\Container;
use Utopia\Servers\Hook;
use Utopia\Telemetry\Adapter as Telemetry;
use Utopia\Telemetry\Adapter\None as NoTelemetry;
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
     * @var array<Hook>
     */
    protected array $errorHooks = [];

    /**
     * Hooks that will run before running job
     *
     * @var array<Hook>
     */
    protected array $initHooks = [];

    /**
     * Hooks that will run after running job
     *
     * @var array<Hook>
     */
    protected array $shutdownHooks = [];

    /**
     * Hooks that will run when worker starts
     *
     * @var array<Hook>
     */
    protected array $workerStartHooks = [];

    /**
     * Hooks that will run when worker stops
     *
     * @var array<Hook>
     */
    protected array $workerStopHooks = [];

    protected Container $container;
    protected ?Container $messageContainer = null;

    private Histogram $jobWaitTime;
    private Histogram $processDuration;

    /**
     * Creates an instance of a Queue server.
     * @param Adapter $adapter
     * @param Container|null $container
     */
    public function __construct(Adapter $adapter, ?Container $container = null)
    {
        $this->adapter = $adapter;
        $this->container = $container ?? new Container();
        $this->setTelemetry(new NoTelemetry());
    }

    public function job(): Job
    {
        $this->job = new Job();
        return $this->job;
    }

    /**
     * Set a new resource on the container
     *
     * @param string $name
     * @param callable $callback
     * @param array $injections
     *
     * @return void
     */
    public function setResource(
        string $name,
        callable $callback,
        array $injections = [],
    ): void {
        $this->container->set($name, $callback, $injections);
    }

    public function getContainer(): Container
    {
        return $this->messageContainer ?? $this->container;
    }

    public function setTelemetry(Telemetry $telemetry): void
    {
        $this->jobWaitTime = $telemetry->createHistogram(
            'messaging.process.wait.duration',
            's',
            null,
            [
                'ExplicitBucketBoundaries' => [
                    0.005,
                    0.01,
                    0.025,
                    0.05,
                    0.075,
                    0.1,
                    0.25,
                    0.5,
                    0.75,
                    1,
                    2.5,
                    5,
                    7.5,
                    10,
                ],
            ],
        );

        // https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/#metric-messagingprocessduration
        $this->processDuration = $telemetry->createHistogram(
            'messaging.process.duration',
            's',
            null,
            [
                'ExplicitBucketBoundaries' => [
                    0.005,
                    0.01,
                    0.025,
                    0.05,
                    0.075,
                    0.1,
                    0.25,
                    0.5,
                    0.75,
                    1,
                    2.5,
                    5,
                    7.5,
                    10,
                ],
            ],
        );
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
            $this->getContainer()->set('error', fn () => $error);
            foreach ($this->errorHooks as $hook) {
                $hook->getAction()(...$this->getArguments($this->getContainer(), $hook));
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
                $this->getContainer()->set('workerId', fn () => $workerId);

                foreach ($this->workerStartHooks as $hook) {
                    $hook->getAction()(...$this->getArguments($this->getContainer(), $hook));
                }

                $this->adapter->consumer->consume(
                    $this->adapter->queue,
                    function (Message $message) {
                        $this->messageContainer = new Container($this->container);

                        $receivedAtTimestamp = microtime(true);
                        try {
                            $waitDuration =
                                microtime(true) - $message->getTimestamp();
                            $this->jobWaitTime->record($waitDuration);

                            $this->getContainer()->set('message', fn () => $message);

                            if ($this->job->getHook()) {
                                foreach ($this->initHooks as $hook) {
                                    // Global init hooks
                                    if (\in_array('*', $hook->getGroups())) {
                                        $arguments = $this->getArguments(
                                            $this->getContainer(),
                                            $hook,
                                            $message->getPayload(),
                                        );
                                        $hook->getAction()(...$arguments);
                                    }
                                }
                            }

                            foreach ($this->job->getGroups() as $group) {
                                foreach ($this->initHooks as $hook) {
                                    // Group init hooks
                                    if (\in_array($group, $hook->getGroups())) {
                                        $arguments = $this->getArguments(
                                            $this->getContainer(),
                                            $hook,
                                            $message->getPayload(),
                                        );
                                        $hook->getAction()(...$arguments);
                                    }
                                }
                            }

                            return \call_user_func_array(
                                $this->job->getAction(),
                                $this->getArguments(
                                    $this->getContainer(),
                                    $this->job,
                                    $message->getPayload(),
                                ),
                            );
                        } finally {
                            $processDuration =
                                microtime(true) - $receivedAtTimestamp;
                            $this->processDuration->record($processDuration);
                        }
                    },
                    function (Message $message) {
                        $this->getContainer()->set('message', fn () => $message);

                        if ($this->job->getHook()) {
                            foreach ($this->shutdownHooks as $hook) {
                                // Global shutdown hooks
                                if (\in_array('*', $hook->getGroups())) {
                                    $arguments = $this->getArguments(
                                        $this->getContainer(),
                                        $hook,
                                        $message->getPayload(),
                                    );
                                    $hook->getAction()(...$arguments);
                                }
                            }
                        }

                        foreach ($this->job->getGroups() as $group) {
                            foreach ($this->shutdownHooks as $hook) {
                                // Group shutdown hooks
                                if (\in_array($group, $hook->getGroups())) {
                                    $arguments = $this->getArguments(
                                        $this->getContainer(),
                                        $hook,
                                        $message->getPayload(),
                                    );
                                    $hook->getAction()(...$arguments);
                                }
                            }
                        }
                    },
                    function (?Message $message, Throwable $th) {
                        $this->getContainer()->set('error', fn () => $th);
                        if ($message !== null) {
                            $this->getContainer()->set('message', fn () => $message);
                        }

                        foreach ($this->errorHooks as $hook) {
                            $hook->getAction()(...$this->getArguments($this->getContainer(), $hook));
                        }
                    },
                );
            });

            $this->adapter->workerStop(function (string $workerId) {
                $this->getContainer()->set('workerId', fn () => $workerId);

                try {
                    // Call user-defined workerStop hooks
                    foreach ($this->workerStopHooks as $hook) {
                        try {
                            $hook->getAction()(...$this->getArguments($this->getContainer(), $hook));
                        } catch (Throwable $e) {
                        }
                    }
                } finally {
                    // Always close consumer connection, even if hooks throw
                    $this->adapter->consumer->close();
                }
            });

            $this->adapter->start();
        } catch (Throwable $error) {
            $this->getContainer()->set('error', fn () => $error);
            foreach ($this->errorHooks as $hook) {
                $hook->getAction()(...$this->getArguments($this->getContainer(), $hook));
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
        $this->workerStartHooks[] = $hook;
        return $hook;
    }

    /**
     * Returns Worker starts hooks.
     * @return array
     */
    public function getWorkerStart(): array
    {
        return $this->workerStartHooks;
    }

    /**
     * Is called when a Worker stops.
     * @return Hook
     */
    public function workerStop(): Hook
    {
        $hook = new Hook();
        $hook->groups(['*']);
        $this->workerStopHooks[] = $hook;
        return $hook;
    }

    /**
     * Returns Worker stops hooks.
     * @return array
     */
    public function getWorkerStop(): array
    {
        return $this->workerStopHooks;
    }

    /**
     * Get Arguments
     *
     * @param Container $container
     * @param Hook $hook
     * @param array $payload
     * @return array
     */
    protected function getArguments(Container $container, Hook $hook, array $payload = []): array
    {
        $arguments = [];
        foreach ($hook->getParams() as $key => $param) {
            // Get value from route or request object
            $value = $payload[$key] ?? $param['default'];
            $value =
                $value === '' || $value === null ? $param['default'] : $value;

            $this->validate($key, $param, $value, $container);
            $hook->setParamValue($key, $value);
            $arguments[$param['order']] = $value;
        }

        foreach ($hook->getInjections() as $key => $injection) {
            $arguments[$injection['order']] = $container->get(
                $injection['name'],
            );
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
     * @param Container $container
     *
     * @throws Exception
     *
     * @return void
     */
    protected function validate(string $key, array $param, mixed $value, Container $container): void
    {
        if ('' !== $value && $value !== null) {
            $validator = $param['validator']; // checking whether the class exists

            if (\is_callable($validator)) {
                $validatorKey = '_validator:' . $key;
                $container->set($validatorKey, $validator, $param['injections']);
                $validator = $container->get($validatorKey);
            }

            if (!$validator instanceof Validator) {
                // is the validator object an instance of the Validator class
                throw new Exception(
                    'Validator object is not an instance of the Validator class',
                    500,
                );
            }

            if (!$validator->isValid($value)) {
                throw new Exception(
                    'Invalid ' . $key . ': ' . $validator->getDescription(),
                    400,
                );
            }
        } elseif (!$param['optional']) {
            throw new Exception("Param $key is not optional.", 400);
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
