<?php

namespace Utopia\Queue;

use Utopia\Http\Hook;

class Job extends Hook
{
    /**
     * Whether to use hook
     *
     * @var bool
     */
    protected bool $hook = true;

    /**
     * Set hook status
     * When set false, hooks for this route will be skipped.
     *
     * @param boolean $hook
     *
     * @return static
     */
    public function hook(bool $hook = true): static
    {
        $this->hook = $hook;

        return $this;
    }

    /**
     * Get hook status
     *
     * @return bool
     */
    public function getHook(): bool
    {
        return $this->hook;
    }
}
