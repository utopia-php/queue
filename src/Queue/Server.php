<?php
namespace Utopia\Queue;

use Throwable;
use Utopia\Queue\Adapter;

/**
 * Utopia PHP Framework
 *
 * @package Utopia\Queue
 *
 * @link https://github.com/utopia-php/framework
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
    protected $errorCallbacks = [];
    protected Adapter $adapter;

    /**
     * Creates an instance of a Queue server.
     * @param Adapter $adapter 
     */
    public function __construct(Adapter $adapter)
    {
        $this->adapter = $adapter;
    }

    /**
     * Starts the Queue server.
     * @return void 
     */
    public function start(): void
    {
        try {
            $this->adapter->start();
        } catch(Throwable $error) {
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
        } catch(Throwable $error) {
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
            $this->adapter->onStart($callback);
        } catch(Throwable $error) {
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
            $this->adapter->onWorkerStart($callback);
        } catch(Throwable $error) {
            foreach ($this->errorCallbacks as $errorCallback) {
                $errorCallback($error, "onWorkerStart");
            }
        }

        return $this;
    }

    /**
     * Is called when a Worker receives a Job.
     * @param callable $callback 
     * @return self 
     */
    public function onJob(callable $callback): self
    {
        try {
            $this->adapter->onJob($callback);
        } catch(Throwable $error) {
            foreach ($this->errorCallbacks as $errorCallback) {
                $errorCallback($error, "onJob");
            }
        }

        return $this;
    }

    /**
     * Register callback. Will be executed when error occurs.
     * @param callable $callback
     * @param Throwable $error
     * @return self
     */
    public function error(callable $callback): self
    {
        \array_push($this->errorCallbacks, $callback);
        return $this;
    }
}
