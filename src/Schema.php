<?php

namespace Fledge\Pulse\Elasticsearch;

use Carbon\CarbonInterval;
use Illuminate\Contracts\Config\Repository;

/**
 * Names, mappings and settings of the Elasticsearch resources Pulse uses.
 *
 * - `{prefix}-entries`: a data stream (index template of the same name)
 *   holding every Entry, including onlyBuckets() ones. Fields: `@timestamp`
 *   (epoch seconds accepted), `type`, `key`, `value`, `aggregations`,
 *   `only_buckets`.
 * - `{prefix}-values`: a regular index holding the latest Value per
 *   type and key. Fields: `timestamp` (epoch seconds), `type`, `key`,
 *   `value` (stored only, never searched).
 * - `{prefix}-policy`: the ILM policy of the data stream, only with `ilm`.
 *
 * Both the install command and the storage read names from here, so they
 * cannot drift apart.
 */
class Schema
{
    /**
     * Longest key (in characters) that is still indexed. Lucene rejects terms
     * over 32766 bytes; 8191 characters of 4 byte UTF-8 stay below that.
     * Longer keys are kept in `_source` but are invisible to queries and
     * aggregations.
     */
    public const int KEY_IGNORE_ABOVE = 8191;

    /**
     * Upstream DatabaseStorage::trim() never keeps more than 7 days.
     */
    public const int MAX_KEEP_SECONDS = 7 * 24 * 60 * 60;

    public const int ROLLOVER_MAX_AGE_SECONDS = 24 * 60 * 60;

    public const string ROLLOVER_MAX_PRIMARY_SHARD_SIZE = '50gb';

    public function __construct(private readonly Repository $config) {}

    public function prefix(): string
    {
        return (string) ($this->option('prefix') ?: 'pulse');
    }

    /**
     * The entries data stream, also the name of its index template.
     */
    public function entries(): string
    {
        return $this->prefix().'-entries';
    }

    public function values(): string
    {
        return $this->prefix().'-values';
    }

    public function policy(): string
    {
        return $this->prefix().'-policy';
    }

    public function ilm(): bool
    {
        return (bool) $this->option('ilm', true);
    }

    public function shards(): int
    {
        return max(1, (int) $this->option('shards', 1));
    }

    public function replicas(): int
    {
        return max(0, (int) $this->option('replicas', 0));
    }

    public function bulkChunk(): int
    {
        return max(1, (int) $this->option('bulk_chunk', 1000));
    }

    /**
     * How often documents rejected with 429 are resent before store() throws.
     */
    public function bulkRetries(): int
    {
        return max(0, (int) $this->option('bulk_retries', 3));
    }

    /**
     * The wait before the first resend, in milliseconds; doubled per retry.
     */
    public function bulkBackoffMs(): int
    {
        return max(0, (int) $this->option('bulk_backoff_ms', 500));
    }

    public function deduplicateEntries(): bool
    {
        return (bool) $this->option('deduplicate_entries', true);
    }

    /**
     * Candidate keys per shard for an ordered key list of `limit` keys:
     * `terms_shard_size`, or max(10000, limit * 50), never below the limit.
     */
    public function termsShardSize(int $limit): int
    {
        $configured = $this->option('terms_shard_size');

        $size = $configured === null || $configured === ''
            ? max(10_000, $limit * 50)
            : (int) $configured;

        return max($size, $limit, 1);
    }

    /**
     * Retention in seconds, parsed like upstream DatabaseStorage::trim():
     * `pulse.storage.trim.keep` (default "7 days") through
     * CarbonInterval::fromString(), capped at 7 days.
     */
    public function keepSeconds(): int
    {
        $keep = $this->config->get('pulse.storage.trim.keep') ?? '7 days';

        $seconds = (int) floor(CarbonInterval::fromString($keep)->totalMilliseconds / 1000);

        return min(max($seconds, 1), self::MAX_KEEP_SECONDS);
    }

    /**
     * The ILM policy body: roll over daily (sooner when the retention is
     * shorter than a day) and delete a backing index `keep` after its
     * rollover. Data therefore lives between `keep` and `keep` plus one
     * rollover period; reads filter on the window anyway.
     *
     * @return array<string, mixed>
     */
    public function policyBody(): array
    {
        $keep = $this->keepSeconds();

        return [
            'policy' => [
                '_meta' => ['managed_by' => 'webpatser/fledge-pulse-elasticsearch'],
                'phases' => [
                    'hot' => [
                        'min_age' => '0ms',
                        'actions' => [
                            'rollover' => [
                                'max_age' => min($keep, self::ROLLOVER_MAX_AGE_SECONDS).'s',
                                'max_primary_shard_size' => self::ROLLOVER_MAX_PRIMARY_SHARD_SIZE,
                            ],
                        ],
                    ],
                    'delete' => [
                        'min_age' => $keep.'s',
                        'actions' => [
                            'delete' => (object) [],
                        ],
                    ],
                ],
            ],
        ];
    }

    /**
     * The composable index template that turns `{prefix}-entries` into a
     * data stream.
     *
     * @return array<string, mixed>
     */
    public function entriesTemplateBody(): array
    {
        $settings = [
            'number_of_shards' => $this->shards(),
            'number_of_replicas' => $this->replicas(),
        ];

        if ($this->ilm()) {
            $settings['index.lifecycle.name'] = $this->policy();
        }

        return [
            'index_patterns' => [$this->entries().'*'],
            'data_stream' => (object) [],
            'priority' => 200,
            '_meta' => ['managed_by' => 'webpatser/fledge-pulse-elasticsearch'],
            'template' => [
                'settings' => $settings,
                'mappings' => $this->entriesMappings(),
            ],
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public function entriesMappings(): array
    {
        return [
            'dynamic' => 'strict',
            'properties' => [
                '@timestamp' => ['type' => 'date', 'format' => 'epoch_second||strict_date_optional_time'],
                'type' => ['type' => 'keyword'],
                // keyword has doc_values on by default, which the terms aggregations need.
                'key' => ['type' => 'keyword', 'ignore_above' => self::KEY_IGNORE_ABOVE],
                'value' => ['type' => 'long'],
                // The aggregations the entry was recorded for (count, min,
                // max, sum, avg). Upstream only pre-aggregates an entry into
                // the buckets of these, so bucket reads filter on it.
                'aggregations' => ['type' => 'keyword'],
                // onlyBuckets() entries count in buckets but never in the raw
                // "tail" of a window, nor as the source of a key.
                'only_buckets' => ['type' => 'boolean'],
            ],
        ];
    }

    /**
     * The values index body (settings and mappings).
     *
     * @return array<string, mixed>
     */
    public function valuesIndexBody(): array
    {
        return [
            'settings' => [
                'number_of_shards' => $this->shards(),
                'number_of_replicas' => $this->replicas(),
            ],
            'mappings' => $this->valuesMappings(),
        ];
    }

    /**
     * Values are read by `_id` (sha1 of type and key) or by type, and their
     * payload is returned from `_source` as the string upstream stores, so
     * `value` is neither indexed nor aggregatable.
     *
     * @return array<string, mixed>
     */
    public function valuesMappings(): array
    {
        return [
            'dynamic' => 'strict',
            'properties' => [
                'timestamp' => ['type' => 'date', 'format' => 'epoch_second'],
                'type' => ['type' => 'keyword'],
                'key' => ['type' => 'keyword', 'ignore_above' => self::KEY_IGNORE_ABOVE],
                'value' => ['type' => 'text', 'index' => false],
            ],
        ];
    }

    protected function option(string $key, mixed $default = null): mixed
    {
        return $this->config->get("pulse.storage.elasticsearch.{$key}", $default);
    }
}
