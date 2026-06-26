<?php

namespace Utopia\Queue;

use Exception;
use Throwable;
use Utopia\DI\Container;
use Utopia\Servers\Hook;
use Utopia\Telemetry\Adapter as Telemetry;
use Utopia\Telemetry\Adapter\None as NoTelemetry;
use Utopia\Telemetry\Histogram;
use Utopia\Telemetry\ObservableGauge;
use Utopia\Validator;

class Server
{
    /**
     * Job
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

    private Histogram $jobWaitTime;
    private Histogram $processDuration;
    private ObservableGauge $queueDepth;

    /**
     * Creates an instance of a Queue server.
     */
    public function __construct(protected Adapter $adapter)
    {
        $this->setTelemetry(new NoTelemetry());
    }

    public function job(): Job
    {
        $this->job = new Job();
        return $this->job;
    }

    /**
     * Static resources container.
     *
     * Shortcut for the underlying adapter's {@see Adapter::resources()}. Use
     * `$server->resources()->set(...)` to register app-wide services that are
     * shared across every message for the lifetime of the server.
     */
    public function resources(): Container
    {
        return $this->adapter->resources();
    }

    /**
     * Per-message context container.
     *
     * Shortcut for the underlying adapter's {@see Adapter::context()}. Use
     * `$server->context()->set(...)` to register message-scoped resources and
     * `$server->context()->get(...)` to read them. Lookups fall through to the
     * static resources container, so app-wide services remain accessible.
     */
    public function context(): Container
    {
        return $this->adapter->context();
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

        $this->queueDepth = $telemetry->createObservableGauge(
            'messaging.queue.depth',
            '{message}',
            'Number of pending messages in the queue.',
        );

        $this->queueDepth->observe(function (callable $observe): void {
            if (!$this->adapter->consumer instanceof Publisher) {
                return;
            }

            try {
                $size = $this->adapter->consumer->getQueueSize($this->adapter->queue);
            } catch (Throwable) {
                return;
            }

            $observe($size, [
                'messaging.destination.name' => $this->adapter->queue->name,
                'messaging.destination.namespace' => $this->adapter->queue->namespace,
            ]);
        });
    }

    /**
     * Shutdown Hooks
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
     */
    public function stop(): self
    {
        try {
            $this->adapter->stop();
        } catch (Throwable $error) {
            $this->resources()->set('error', fn(): \Throwable => $error);
            foreach ($this->errorHooks as $hook) {
                $hook->getAction()(...$this->getArguments($this->resources(), $hook));
            }
        }
        return $this;
    }

    /**
     * Init Hooks
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
     */
    public function start(): self
    {
        try {
            $this->adapter->workerStart(function (string $workerId): void {
                $this->resources()->set('workerId', fn(): string => $workerId);

                foreach ($this->workerStartHooks as $hook) {
                    $hook->getAction()(...$this->getArguments($this->resources(), $hook));
                }

                $this->adapter->consume(
                    function (Message $message) {
                        $receivedAtTimestamp = microtime(true);
                        try {
                            $waitDuration
                                = microtime(true) - $message->getTimestamp();
                            $this->jobWaitTime->record($waitDuration);

                            $this->context()->set('message', fn(): \Utopia\Queue\Message => $message);

                            if ($this->job->getHook()) {
                                foreach ($this->initHooks as $hook) {
                                    // Global init hooks
                                    if (\in_array('*', $hook->getGroups())) {
                                        $arguments = $this->getArguments(
                                            $this->context(),
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
                                            $this->context(),
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
                                    $this->context(),
                                    $this->job,
                                    $message->getPayload(),
                                ),
                            );
                        } finally {
                            $processDuration
                                = microtime(true) - $receivedAtTimestamp;
                            $this->processDuration->record($processDuration);
                        }
                    },
                    function (Message $message): void {
                        $this->context()->set('message', fn(): \Utopia\Queue\Message => $message);

                        if ($this->job->getHook()) {
                            foreach ($this->shutdownHooks as $hook) {
                                // Global shutdown hooks
                                if (\in_array('*', $hook->getGroups())) {
                                    $arguments = $this->getArguments(
                                        $this->context(),
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
                                        $this->context(),
                                        $hook,
                                        $message->getPayload(),
                                    );
                                    $hook->getAction()(...$arguments);
                                }
                            }
                        }
                    },
                    function (?Message $message, Throwable $th): void {
                        $this->context()->set('error', fn(): \Throwable => $th);
                        if ($message instanceof \Utopia\Queue\Message) {
                            $this->context()->set('message', fn(): \Utopia\Queue\Message => $message);
                        }

                        foreach ($this->errorHooks as $hook) {
                            $hook->getAction()(...$this->getArguments($this->context(), $hook));
                        }
                    },
                );
            });

            $this->adapter->workerStop(function (string $workerId): void {
                $this->resources()->set('workerId', fn(): string => $workerId);

                try {
                    // Call user-defined workerStop hooks
                    foreach ($this->workerStopHooks as $hook) {
                        try {
                            $hook->getAction()(...$this->getArguments($this->resources(), $hook));
                        } catch (Throwable) {
                        }
                    }
                } finally {
                    // Always close consumer connection, even if hooks throw
                    $this->adapter->consumer->close();
                }
            });

            $this->adapter->start();
        } catch (Throwable $error) {
            $this->resources()->set('error', fn(): \Throwable => $error);
            foreach ($this->errorHooks as $hook) {
                $hook->getAction()(...$this->getArguments($this->resources(), $hook));
            }
        }
        return $this;
    }

    /**
     * Is called when a Worker starts.
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
     */
    public function getWorkerStart(): array
    {
        return $this->workerStartHooks;
    }

    /**
     * Is called when a Worker stops.
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
     */
    public function getWorkerStop(): array
    {
        return $this->workerStopHooks;
    }

    /**
     * Get Arguments
     */
    protected function getArguments(Container $context, Hook $hook, array $payload = []): array
    {
        $arguments = [];
        foreach ($hook->getParams() as $key => $param) {
            $payloadKey = $key;
            if (!\array_key_exists($key, $payload) && !empty($param['aliases'])) {
                foreach ($param['aliases'] as $alias) {
                    if (\array_key_exists($alias, $payload)) {
                        $payloadKey = $alias;
                        break;
                    }
                }
            }

            // Get value from route or request object
            $value = $payload[$payloadKey] ?? $param['default'];
            $value
                = $value === '' || $value === null ? $param['default'] : $value;

            $this->validate($key, $param, $value, $context);
            $hook->setParamValue($key, $value);
            $arguments[$param['order']] = $value;
        }

        foreach ($hook->getInjections() as $injection) {
            $arguments[$injection['order']] = $context->get(
                $injection['name'],
            );
        }

        // call_user_func_array passes integer keys in iteration order, not key
        // order, so sort the two-pass (params, then injections) array by key.
        ksort($arguments);

        return $arguments;
    }

    /**
     * Validate Param
     *
     * Creates an validator instance and validate given value with given rules.
     *
     *
     * @throws Exception
     *
     */
    protected function validate(string $key, array $param, mixed $value, Container $context): void
    {
        if ('' !== $value && $value !== null) {
            $validator = $param['validator']; // checking whether the class exists

            if (\is_callable($validator)) {
                $validatorKey = '_validator:' . $key;
                $context->set($validatorKey, $validator, $param['injections']);
                $validator = $context->get($validatorKey);
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
     */
    public function error(): Hook
    {
        $hook = new Hook();
        $hook->groups(['*']);
        $this->errorHooks[] = $hook;
        return $hook;
    }
}
