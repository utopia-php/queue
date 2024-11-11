<?php

namespace Utopia\Queue;

use Utopia\Telemetry\Adapter as Telemetry;
use Utopia\Telemetry\Histogram;

class Metrics
{
    public Histogram $jobWaitTime;
    public Histogram $processDuration;

    public function __construct(Telemetry $telemetry)
    {
        // Duration in seconds from when the job was created till it got picked up by a worker.
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
    }
}
