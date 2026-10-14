<?php

namespace Fledge\Pulse\Elasticsearch\Query;

/**
 * One side of an upstream aggregate read for one group (a key, or a type):
 * either the tail (raw entries) or the buckets of one aggregation.
 *
 * Built from a filter aggregation holding an optional `s` stats sub-agg and,
 * for bucket averages, an optional `m` avg_bucket (the mean of the per-bucket
 * means).
 */
final readonly class Part
{
    public function __construct(
        public int $docs = 0,
        public int $values = 0,
        public ?float $min = null,
        public ?float $max = null,
        public float $sum = 0.0,
        public ?float $avg = null,
    ) {}

    /**
     * @param  array<string, mixed>|null  $aggregation
     */
    public static function from(?array $aggregation): self
    {
        if ($aggregation === null) {
            return new self;
        }

        $stats = $aggregation['s'] ?? [];

        $avg = array_key_exists('m', $aggregation)
            ? $aggregation['m']['value'] ?? null
            : $stats['avg'] ?? null;

        return new self(
            docs: (int) ($aggregation['doc_count'] ?? 0),
            values: (int) ($stats['count'] ?? 0),
            min: isset($stats['min']) ? (float) $stats['min'] : null,
            max: isset($stats['max']) ? (float) $stats['max'] : null,
            sum: (float) ($stats['sum'] ?? 0),
            avg: $avg === null ? null : (float) $avg,
        );
    }

    public function withAvg(?float $avg): self
    {
        return new self($this->docs, $this->values, $this->min, $this->max, $this->sum, $avg);
    }

    /**
     * Combine a tail and a bucket part the way upstream's outer query does.
     *
     * Upstream unions one tail row (only when the group has tail entries) and
     * one bucket row (only when it has buckets of the aggregation), then
     * applies the aggregation over both rows, ignoring nulls:
     *
     * - count: the tail's count(*) (0 when the tail row exists but holds none
     *   of the column's type) plus the sum of the bucket counts.
     * - min / max / sum: over the tail values and the bucket values.
     * - avg: the plain mean of the tail average and the mean of the bucket
     *   averages, not a mean weighted by the number of values.
     *
     * @param  bool  $tailRow  whether upstream has a tail row for the group
     */
    public static function merge(string $aggregate, bool $tailRow, self $tail, self $bucket): int|float|null
    {
        $tailValue = match ($aggregate) {
            'count' => $tailRow ? $tail->docs : null,
            'min' => $tail->values > 0 ? $tail->min : null,
            'max' => $tail->values > 0 ? $tail->max : null,
            'sum' => $tail->values > 0 ? $tail->sum : null,
            'avg' => $tail->values > 0 ? $tail->avg : null,
        };

        $bucketValue = $bucket->docs === 0 ? null : match ($aggregate) {
            'count' => $bucket->docs,
            'min' => $bucket->values > 0 ? $bucket->min : null,
            'max' => $bucket->values > 0 ? $bucket->max : null,
            // Upstream pre-aggregates a missing value as 0.
            'sum' => $bucket->sum,
            'avg' => $bucket->avg,
        };

        $parts = array_values(array_filter([$tailValue, $bucketValue], fn ($value) => $value !== null));

        if ($parts === []) {
            return null;
        }

        return match ($aggregate) {
            'count', 'sum' => (int) round(array_sum($parts)),
            'min' => (int) round(min($parts)),
            'max' => (int) round(max($parts)),
            'avg' => array_sum($parts) / count($parts),
        };
    }
}
