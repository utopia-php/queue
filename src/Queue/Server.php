<?php

namespace Utopia\Queue;

use Throwable;
use Exception;
use Utopia\CLI\Console;
use Utopia\DI\Dependency;
use Utopia\Servers\Base;
use Utopia\Servers\Hook;

class Server extends Base
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
    protected static Job $job;

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

    /**
     * Creates an instance of a Queue server.
     * @param Adapter $adapter
     */
    public function __construct(Adapter $adapter)
    {
        $this->adapter = $adapter;
    }

    /**
     * Add a job hook
     */
    public static function job(): Job
    {
        self::$job = new Job();
        return self::$job;
    }

    /**
     * Stops the Queue server.
     * @return self
     */
    public function stop(): self
    {
        try {
            $this->adapter->stop();
        } catch (Throwable $th) {
            $context = clone $this->container;

            $dependency = new Dependency();
            $context->set(
                $dependency
                    ->setName('error')
                    ->setCallback(fn () => $th)
            );

            foreach (self::$shutdown as $hook) { // Global shutdown hooks
                if (in_array('*', $hook->getGroups())) {
                    $this->prepare($context, $hook, [], [])->inject($hook, true);
                }
            }
        }
        return $this;
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

                // if (!is_null($this->workerStartHook)) {
                //     call_user_func_array($this->workerStartHook->getAction(), $this->getArguments($this->workerStartHook));
                // }

                while (true) {
                    /**
                     * Waiting for next Job.
                     */
                    $nextMessage = $this->adapter->connection->rightPopArray("{$this->adapter->namespace}.queue.{$this->adapter->queue}", 5);

                    if (!$nextMessage) {
                        continue;
                    }

                    $nextMessage['timestamp'] = (int)$nextMessage['timestamp'];

                    $context = clone $this->container;
                    $job = clone self::$job;
                    $groups = $job->getGroups();
                    $message = new Message($nextMessage);

                    $dependency = new Dependency();
                    $context->set(
                        $dependency
                            ->setName('message')
                            ->setCallback(fn () => $message)
                    );

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

                        foreach (self::$init as $hook) { // Global init hooks
                            if (in_array('*', $hook->getGroups())) {
                                $this->prepare($context, $hook, [], $message->getPayload())->inject($hook, true);
                            }
                        }

                        foreach ($groups as $group) {
                            foreach (self::$init as $hook) { // Group init hooks
                                if (in_array($group, $hook->getGroups())) {
                                    $this->prepare($context, $hook, [], $message->getPayload())->inject($hook, true);
                                }
                            }
                        }

                        $this->prepare($context, $job, [], $message->getPayload())->inject($job, true);

                        /**
                         * Remove Jobs if successful.
                         */
                        $this->adapter->connection->remove("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$message->getPid()}");

                        /**
                         * Increment Successful Jobs from Stats.
                         */
                        $this->adapter->connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.success");

                        foreach ($groups as $group) {
                            foreach (self::$shutdown as $hook) { // Group shutdown hooks
                                if (in_array($group, $hook->getGroups())) {
                                    $this->prepare($context, $hook, [], $message->getPayload())->inject($hook, true);
                                }
                            }
                        }

                        foreach (self::$shutdown as $hook) { // Global shutdown hooks
                            if (in_array('*', $hook->getGroups())) {
                                $this->prepare($context, $hook, [], $message->getPayload())->inject($hook, true);
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

                        $dependency = new Dependency();
                        $context->set(
                            $dependency
                                ->setName('error')
                                ->setCallback(fn () => $th)
                        )
                        ;

                        foreach ($groups as $group) {
                            foreach (self::$errors as $error) { // Group error hooks
                                if (in_array($group, $error->getGroups())) {
                                    try {
                                        $this->prepare($context, $error, [], $message->getPayload())->inject($error, true);
                                    } catch (\Throwable $e) {
                                        throw new Exception('Group error handler had an error: ' . $e->getMessage(). ' on: ' . $e->getFile().':'.$e->getLine(), 500, $e);
                                    }
                                }
                            }
                        }

                        foreach (self::$errors as $error) { // Global error hooks
                            if (in_array('*', $error->getGroups())) {
                                try {
                                    $this->prepare($context, $error, [], $message->getPayload())->inject($error, true);
                                } catch (\Throwable $e) {
                                    throw new Exception('Global error handler had an error: ' . $e->getMessage(). ' on: ' . $e->getFile().':'.$e->getLine(), 500, $e);
                                }
                            }
                        }
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

                    $this->resources = [];
                }
            });

            $this->adapter->start();
        } catch (Throwable $th) {
            $context = clone $this->container;

            $dependency = new Dependency();
            $context->set(
                $dependency
                    ->setName('error')
                    ->setCallback(fn () => $th)
            );

            foreach (self::$shutdown as $hook) { // Global shutdown hooks
                if (in_array('*', $hook->getGroups())) {
                    $this->prepare($context, $hook, [], [])->inject($hook, true);
                }
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
    // public function workerStop(callable $callback = null): self
    // {
    //     $container = clone $this->container;

    //     try {
    //         $this->adapter->workerStop(function (string $workerId) use ($this, $container, $callback) {
    //             Console::success("[Worker] Worker {$workerId} is ready!");
    //             if (!is_null($callback)) {
    //                 call_user_func($callback);
    //                 $this->prepare($container, $hook, [], [])->inject($hook, true);
    //             }
    //         });
    //     } catch (Throwable $error) {
    //         self::setResource('error', fn () => $error);
    //         foreach (self::$errors as $error) { // Global error hooks
    //             if (in_array('*', $error->getGroups())) {
    //                 try {
    //                     $this->prepare($container, $error, [], [])->inject($error, true);
    //                 } catch (\Throwable $e) {
    //                     throw new Exception('Error handler had an error: ' . $e->getMessage(). ' on: ' . $e->getFile().':'.$e->getLine(), 500, $e);
    //                 }
    //             }
    //         }
    //     }

    //     return $this;
    // }
}
