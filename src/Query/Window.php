<?php

namespace Fledge\Pulse\Elasticsearch\Query;

use Carbon\CarbonImmutable;
use Carbon\CarbonInterval;

/**
 * The time boundaries upstream DatabaseStorage reads a window with.
 *
 * Upstream splits a window into a "tail" of raw entries and the buckets it
 * pre-aggregated on write. With `period = interval / 60` (the bucket width in
 * seconds, e.g. 60 for an hour):
 *
 * - windowStart  = now - interval + 1
 * - currentBucket = floor(now / period) * period
 * - oldestBucket = currentBucket - interval + period
 * - tail: raw entries with windowStart <= timestamp <= oldestBucket - 1
 * - buckets: every bucket >= oldestBucket, which (buckets being aligned to
 *   multiples of the period) is every entry with timestamp >= oldestBucket,
 *   without an upper bound.
 *
 * Raw entries in Elasticsearch are filtered on exactly these boundaries, so
 * reads match upstream at the edges.
 */
final readonly class Window
{
    public function __construct(
        public int $windowStart,
        public int $oldestBucket,
        public int $currentBucket,
        public float $period,
    ) {}

    public static function for(CarbonInterval $interval, ?CarbonImmutable $now = null): self
    {
        $now ??= CarbonImmutable::now();

        $seconds = $interval->totalSeconds;
        $period = $seconds / 60;
        $timestamp = $now->getTimestamp();

        $windowStart = (int) ($timestamp - $seconds + 1);
        $currentBucket = (int) (floor($timestamp / $period) * $period);
        $oldestBucket = $currentBucket - $seconds + $period;

        return new self($windowStart, (int) ceil($oldestBucket), $currentBucket, $period);
    }

    /**
     * The `fixed_interval` of a date histogram whose buckets are upstream's
     * buckets of this window (epoch aligned, `period` seconds wide).
     */
    public function fixedInterval(): string
    {
        $milliseconds = max(1, (int) round($this->period * 1000));

        return $milliseconds % 1000 === 0
            ? intdiv($milliseconds, 1000).'s'
            : $milliseconds.'ms';
    }
}
