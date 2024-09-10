<?php

namespace Utopia\Queue;

use Throwable;
use Exception;
use Utopia\CLI\Console;
use Utopia\DI\Container;
use Utopia\DI\Dependency;
use Utopia\Queue\Concurrency\Manager;
use Utopia\Servers\Base;

class Worker extends Base
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
     * Concurrency Manager
     *
     * @var ?Manager
     */
    protected ?Manager $concurrencyManager = null;

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
     * Register a concurrency manager
     *
     * @param Manager $manager
     */
    public function setConcurrencyManager(Manager $manager): self
    {
        $this->concurrencyManager = $manager;
        return $this;
    }

    /**
     * Get a concurrency manager
     *
     * @return ?Manager
     */
    public function getConcurrencyManager(): ?Manager
    {
        return $this->concurrencyManager;
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
            $this->adapter->onWorkerStart(function (string $workerId) {
                // Check if the connection is ready
                $retryAttempts = 30;
                $retryDelay = 1; // seconds

                while (!$this->adapter->connection->ping()) {
                    if ($retryAttempts <= 0) {
                        Console::error("[Worker] connection is not ready. Exiting...");
                        return $this;
                    }

                    $retryAttempts--;

                    Console::warning("[Worker] connection is not ready. Retrying in {$retryDelay} seconds [{$retryAttempts} left] ...");

                    sleep($retryDelay);
                }

                Console::success("[Worker] Worker {$workerId} is ready!");

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
                    $message = new Message($nextMessage);

                    $dependency = new Dependency();
                    $context->set(
                        $dependency
                            ->setName('message')
                            ->setCallback(fn () => $message)
                    );

                    $this->adapter->onJob(function () use ($job, $message, $nextMessage, $context) {
                        $this->lifecycle($job, $message, $nextMessage, $context, $this->adapter->connection);
                    });
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

    protected function lifecycle(Job $job, Message $message, array $nextMessage, Container $context, Connection $connection): static
    {
        Console::info("[Job] Received Job ({$message->getPid()}).");

        $concurrencyManager = $this->getConcurrencyManager();

        $groups = $job->getGroups();

        $connection->getConnection();

        if ($concurrencyManager && !$concurrencyManager->canProcessJob($message)) {
            $this->adapter->connection->leftPushArray("{$this->adapter->namespace}.queue.{$this->adapter->queue}", $nextMessage);
            Console::info("[Job] Re-queued Job ({$message->getPid()}) due to concurrency limit.");
            return $this;
        }

        /** Increment the concurrency counter */
        if ($concurrencyManager) {
            $concurrencyManager->startJob($message);
        }

        /**
         * Move Job to Jobs and it's PID to the processing list.
         */
        $connection->setArray("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$message->getPid()}", $nextMessage);
        $connection->leftPush("{$this->adapter->namespace}.processing.{$this->adapter->queue}", $message->getPid());

        /**
         * Increment Total Jobs Received from Stats.
         */
        $connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.total");

        /**
         * Increment Processing Jobs from Stats.
         */
        $connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.processing");

        try {
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
            $connection->remove("{$this->adapter->namespace}.jobs.{$this->adapter->queue}.{$message->getPid()}");

            /**
             * Increment Successful Jobs from Stats.
             */
            $connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.success");


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
            $connection->leftPush("{$this->adapter->namespace}.failed.{$this->adapter->queue}", $message->getPid());

            /**
             * Increment Failed Jobs from Stats.
             */
            $connection->increment("{$this->adapter->namespace}.stats.{$this->adapter->queue}.failed");

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
            /** Decrement the counter so another job can be picked up again */
            if ($concurrencyManager) {
                $concurrencyManager->finishJob($message);
            }

            /**
             * Remove Job from Processing.
             */
            $connection->listRemove("{$this->adapter->namespace}.processing.{$this->adapter->queue}", $message->getPid());

            /**
             * Decrease Processing Jobs from Stats.
             */
            $connection->decrement("{$this->adapter->namespace}.stats.{$this->adapter->queue}.processing");

            $connection->putConnection();
        }

        return $this;
    }
}
