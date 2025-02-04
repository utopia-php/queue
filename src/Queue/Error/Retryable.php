<?php

namespace Utopia\Queue\Error;

/**
 * A Retryable error signals the queue consumer to re-queue the message instead of putting it into failed queue.
 */
class Retryable extends \RuntimeException
{
}
