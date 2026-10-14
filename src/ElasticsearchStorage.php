<?php

namespace Fledge\Pulse\Elasticsearch;

use Carbon\CarbonImmutable;
use Carbon\CarbonInterval;
use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\Exception\ClientResponseException;
use Fledge\Pulse\Elasticsearch\Query\Dsl;
use Fledge\Pulse\Elasticsearch\Query\Part;
use Fledge\Pulse\Elasticsearch\Query\Window;
use Fledge\Pulse\Elasticsearch\Support\BulkException;
use Generator;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Support\Collection;
use InvalidArgumentException;
use Laravel\Pulse\Contracts\Storage;
use Laravel\Pulse\Entry;
use Laravel\Pulse\Value;
use RuntimeException;
use stdClass;

use function Fledge\Async\delay;

/**
 * Pulse storage on Elasticsearch: entries in the `{prefix}-entries` data
 * stream, values in the `{prefix}-values` index (see Schema).
 *
 * The client is injected by the service provider (contextual binding to
 * ClientFactory::client()), so requests run on the fiber-aware transport and
 * a read suspends only the fiber that issued it.
 *
 * Upstream DatabaseStorage pre-aggregates entries into buckets on write; this
 * storage keeps raw entries (each with the aggregations it was recorded for
 * and its onlyBuckets() flag) and aggregates at read time with upstream's
 * window math (see Window and Part), so every read returns what upstream
 * returns for the same input:
 *
 * - the tail of a window counts raw entries only, buckets count every entry
 *   recorded for the aggregation, onlyBuckets() ones included;
 * - a bucket average is the mean of the entries in one `period` wide bucket
 *   (a date histogram), and avg is the plain mean of the tail average and
 *   the mean of those bucket averages;
 * - aggregate()/aggregateTypes() take a key from the raw entries, so a key
 *   that only has onlyBuckets() entries comes back as null.
 *
 * Ordering of aggregate() and aggregateTypes() follows upstream on MySQL:
 * nulls last on desc and first on asc. Ties order by key ascending, where
 * upstream leaves them to the database. When Elasticsearch picks the
 * candidate keys (count, sum and max orderings), a key without values for
 * a sum or count ranks as 0 there, so among more than `limit * 2` keys tied
 * at 0 or null the cut may differ from upstream's.
 *
 * An interval that is not one of upstream's bucket periods (1 hour, 6
 * hours, 24 hours, 7 days) is read from raw entries, where upstream finds
 * no buckets for it.
 */
class ElasticsearchStorage implements Storage
{
    /**
     * Buckets per composite aggregation page.
     */
    protected const int PAGE = 1000;

    /**
     * Keys per `terms` query when resolving keys.
     */
    protected const int TERMS_CHUNK = 10_000;

    /**
     * Seconds purge() waits for its entry deletion task.
     */
    protected const float PURGE_TIMEOUT = 600.0;

    /**
     * @var list<string>
     */
    protected const array AGGREGATES = ['count', 'min', 'max', 'sum', 'avg'];

    protected Schema $schema;

    /**
     * Background `delete_by_query` tasks on the entries not yet seen done.
     *
     * @var array<int, string>
     */
    protected array $pendingDeletes = [];

    public function __construct(
        protected Client $client,
        protected Repository $config,
    ) {
        $this->schema = new Schema($config);
    }

    /**
     * Store the items: entries into the data stream, values into the values
     * index, one `_bulk` request per `bulk_chunk` documents. Entries and
     * values never share a request: entry requests carry
     * `require_data_stream`, so a missing data stream fails with a hint to
     * run the install command instead of auto-creating a plain index with
     * dynamic mappings.
     *
     * Entries get a deterministic `_id` (see entryIds()) and are written
     * with `create`, so storing a batch again after a failed store() (the
     * Redis ingest keeps the batch and retries it) hits version conflicts,
     * which are ignored, instead of counting twice. A retried batch whose
     * entries land in a newer backing index (after a rollover in between)
     * is not deduplicated: ids are unique per backing index only.
     *
     * Values are written with the value's timestamp as external version, so
     * the newest value per type and key wins whatever order writes arrive in;
     * a write older than the stored value is a version conflict and ignored.
     * `external_gte` lets a later write with the same timestamp win, as an
     * upstream upsert does.
     *
     * @param  Collection<int, Entry|Value>  $items
     */
    public function store(Collection $items): void
    {
        if ($items->isEmpty()) {
            return;
        }

        [$entries, $values] = $items->partition(fn (Entry|Value $item) => $item instanceof Entry);

        $documents = [];

        foreach ($entries as $entry) {
            $aggregations = array_values(array_unique($entry->aggregations()));

            // Upstream stores these nowhere: no raw row and no bucket.
            if ($entry->isOnlyBuckets() && $aggregations === []) {
                continue;
            }

            $documents[] = [
                '@timestamp' => $entry->timestamp,
                'type' => $entry->type,
                'key' => $entry->key,
                'value' => $entry->value,
                'aggregations' => $aggregations,
                'only_buckets' => $entry->isOnlyBuckets(),
            ];
        }

        $ids = $this->schema->deduplicateEntries() ? $this->entryIds($documents) : [];

        $operations = [];

        foreach ($documents as $i => $document) {
            $action = ['_index' => $this->schema->entries()];

            if (isset($ids[$i])) {
                $action['_id'] = $ids[$i];
            }

            $operations[] = [['create' => $action], $document];
        }

        foreach (array_chunk($operations, $this->schema->bulkChunk()) as $chunk) {
            $this->bulk($chunk, entries: true);
        }

        $operations = [];

        // Within one call the last value per type and key wins, as upstream.
        $latest = $values->reverse()->unique(fn (Value $value) => [$value->key, $value->type]);

        foreach ($latest as $value) {
            $operations[] = [
                ['index' => [
                    '_index' => $this->schema->values(),
                    '_id' => $this->valueId($value->type, $value->key),
                    'version' => $value->timestamp,
                    'version_type' => 'external_gte',
                ]],
                [
                    'timestamp' => $value->timestamp,
                    'type' => $value->type,
                    'key' => $value->key,
                    'value' => $value->value,
                ],
            ];
        }

        foreach (array_chunk($operations, $this->schema->bulkChunk()) as $chunk) {
            $this->bulk($chunk, entries: false);
        }
    }

    /**
     * Deterministic entry ids: a hash chain over the entries of the batch,
     * in order. An entry's id covers its own content and everything before
     * it, so identical entries within one batch stay distinct, a batch
     * stored again (even with more entries appended, as the Redis ingest
     * reads a grown stream) gets the same ids for the same leading entries,
     * and two batches only share ids for an identical leading run.
     *
     * @param  list<array<string, mixed>>  $documents
     * @return list<string>
     */
    protected function entryIds(array $documents): array
    {
        $ids = [];
        $chain = '';

        foreach ($documents as $document) {
            $chain = sha1($chain.'|'.json_encode([
                $document['type'],
                $document['key'],
                $document['@timestamp'],
                $document['value'],
                $document['aggregations'],
                $document['only_buckets'],
            ], JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE));

            $ids[] = $chain;
        }

        return $ids;
    }

    /**
     * Trim the storage.
     *
     * With ILM the policy deletes old entries, so only values (a regular
     * index without a policy) are trimmed here; without ILM entries are
     * deleted by query too. The cut-off is upstream's: `trim.keep`, at most
     * 7 days.
     */
    public function trim(): void
    {
        $now = CarbonImmutable::now();

        $keep = $this->config->get('pulse.storage.trim.keep') ?? '7 days';

        $before = $now->subMilliseconds(
            (int) CarbonInterval::fromString($keep)->totalMilliseconds
        );

        if ($now->subDays(7)->isAfter($before)) {
            $before = $now->subDays(7);
        }

        $this->deleteByQuery($this->schema->values(), Dsl::until($before->getTimestamp(), 'timestamp'));

        if (! $this->schema->ilm()) {
            $this->deleteEntriesInBackground(Dsl::until($before->getTimestamp()));
        }
    }

    /**
     * Purge the storage: every document, or those of the given types.
     *
     * The indices are refreshed first, so documents written just before are
     * purged too. Entries are deleted by a background task that is polled
     * until it finishes (no single request has to outlast the HTTP timeout,
     * and writes right after purge() cannot race the deletion); values are
     * deleted with one request. The purge is visible on return.
     *
     * @param  list<string>|null  $types
     */
    public function purge(?array $types = null): void
    {
        $query = $types === null ? Dsl::matchAll() : Dsl::terms('type', $types);

        $this->client->indices()->refresh([
            'index' => $this->schema->entries().','.$this->schema->values(),
            'ignore_unavailable' => true,
            'allow_no_indices' => true,
        ]);

        $this->deleteEntriesInBackground($query);

        $this->deleteByQuery($this->schema->values(), $query, refresh: true);

        $this->awaitDeletes(self::PURGE_TIMEOUT);
    }

    /**
     * Wait for the background entry deletions started by trim() and purge()
     * and refresh the entries, so their result is visible. Polls the tasks
     * API, so the wait suspends only the calling fiber.
     *
     * @param  float  $timeout  seconds to wait at most
     */
    public function awaitDeletes(float $timeout = 60.0): void
    {
        $deadline = microtime(true) + $timeout;

        while ($this->pendingDeletes !== []) {
            foreach ($this->pendingDeletes as $i => $task) {
                if ($this->taskCompleted($task)) {
                    unset($this->pendingDeletes[$i]);
                }
            }

            if ($this->pendingDeletes === []) {
                break;
            }

            if (microtime(true) >= $deadline) {
                throw new RuntimeException('Pulse entry deletion task(s) ['.implode(', ', $this->pendingDeletes).'] did not finish within '.$timeout.' seconds.');
            }

            delay(0.05);
        }

        $this->client->indices()->refresh([
            'index' => $this->schema->entries(),
            'ignore_unavailable' => true,
            'allow_no_indices' => true,
        ]);
    }

    /**
     * Start a `delete_by_query` on the entries without waiting for it: on a
     * week of raw entries it can run far longer than the HTTP timeout.
     * Documents changed while it runs are skipped (`conflicts=proceed`).
     *
     * @param  array<string, mixed>  $query
     */
    protected function deleteEntriesInBackground(array $query): void
    {
        $response = $this->client->deleteByQuery([
            'index' => $this->schema->entries(),
            'body' => ['query' => $query],
            'conflicts' => 'proceed',
            'ignore_unavailable' => true,
            'allow_no_indices' => true,
            'wait_for_completion' => false,
        ])->asArray();

        if (isset($response['task'])) {
            $this->pendingDeletes[] = (string) $response['task'];
        }
    }

    protected function taskCompleted(string $task): bool
    {
        try {
            return (bool) ($this->client->tasks()->get(['task_id' => $task])->asArray()['completed'] ?? true);
        } catch (ClientResponseException $e) {
            // A task that is gone (and left no result) is finished.
            if ($e->getCode() === 404) {
                return true;
            }

            throw $e;
        }
    }

    /**
     * Retrieve values for the given type.
     *
     * @param  list<string>|null  $keys
     * @return Collection<string, object{timestamp: int, key: string, value: string}>
     */
    public function values(string $type, ?array $keys = null): Collection
    {
        $filter = [Dsl::term('type', $type)];

        // Upstream skips the key filter for an empty list too.
        if ($keys) {
            $filter[] = ['ids' => ['values' => array_values(array_unique(array_map(
                fn (string $key) => $this->valueId($type, $key),
                $keys,
            )))]];
        }

        $rows = [];
        $after = null;

        do {
            $body = [
                'size' => self::PAGE,
                'track_total_hits' => false,
                'query' => Dsl::bool(filter: $filter),
                '_source' => ['timestamp', 'key', 'value'],
                'sort' => [['key' => 'asc'], ['timestamp' => 'asc']],
            ];

            if ($after !== null) {
                $body['search_after'] = $after;
            }

            $hits = $this->search($this->schema->values(), $body)['hits']['hits'] ?? [];

            foreach ($hits as $hit) {
                $rows[] = (object) [
                    'timestamp' => (int) $hit['_source']['timestamp'],
                    'key' => (string) $hit['_source']['key'],
                    'value' => (string) $hit['_source']['value'],
                ];

                $after = $hit['sort'];
            }
        } while (count($hits) === self::PAGE);

        return (new Collection($rows))->keyBy('key');
    }

    /**
     * Retrieve aggregate values for plotting on a graph.
     *
     * @param  list<string>  $types
     * @param  'count'|'min'|'max'|'sum'|'avg'  $aggregate
     * @return Collection<string, Collection<string, Collection<string, int|float|null>>>
     */
    public function graph(array $types, string $aggregate, CarbonInterval $interval): Collection
    {
        $this->validateAggregate($aggregate);

        // Upstream's bucket and padding math, verbatim.
        $now = CarbonImmutable::now();
        $maxDataPoints = 60;
        $secondsPerPeriod = ($interval->totalSeconds / $maxDataPoints);
        $currentBucket = (int) (floor($now->getTimestamp() / $secondsPerPeriod) * $secondsPerPeriod);
        $firstBucket = $currentBucket - (($maxDataPoints - 1) * $secondsPerPeriod);

        $padding = collect()
            ->range(0, 59)
            ->mapWithKeys(fn ($i) => [CarbonImmutable::createFromTimestamp($firstBucket + $i * $secondsPerPeriod)->toDateTimeString() => null]);

        $structure = collect($types)->mapWithKeys(fn ($type) => [$type => $padding]);

        if ($types === []) {
            return new Collection;
        }

        $window = Window::for($interval, $now);

        $query = Dsl::bool(filter: [
            Dsl::terms('type', $types),
            Dsl::term('aggregations', $aggregate),
            Dsl::since((int) ceil($firstBucket)),
        ]);

        $sources = [
            ['k' => ['terms' => ['field' => 'key']]],
            ['t' => ['terms' => ['field' => 'type']]],
            ['h' => ['date_histogram' => ['field' => '@timestamp', 'fixed_interval' => $window->fixedInterval()]]],
        ];

        /** @var array<string, array<string, array<string, int|float|null>>> $readings */
        $readings = [];

        foreach ($this->composite($this->schema->entries(), $query, $sources, ['s' => Dsl::stats()]) as $bucket) {
            $bucketKey = CarbonImmutable::createFromTimestamp(intdiv((int) $bucket['key']['h'], 1000))->toDateTimeString();

            $readings[$bucket['key']['k']][$bucket['key']['t']][$bucketKey] = $this->graphValue($aggregate, $bucket);
        }

        return (new Collection($readings))
            ->sortKeys()
            ->map(fn (array $byType) => $structure->merge(
                (new Collection($byType))->map(fn (array $points) => $padding->merge($points))
            ));
    }

    /**
     * Retrieve aggregate values for the given type.
     *
     * @param  'count'|'min'|'max'|'sum'|'avg'|list<'count'|'min'|'max'|'sum'|'avg'>  $aggregates
     * @return Collection<int, object{key: string|null, min?: int, max?: int, sum?: int, avg?: float, count?: int}>
     */
    public function aggregate(
        string $type,
        string|array $aggregates,
        CarbonInterval $interval,
        ?string $orderBy = null,
        string $direction = 'desc',
        int $limit = 101,
    ): Collection {
        $aggregates = is_array($aggregates) ? array_values($aggregates) : [$aggregates];

        if ($invalid = array_diff($aggregates, self::AGGREGATES)) {
            throw new InvalidArgumentException('Invalid aggregate type(s) ['.implode(', ', $invalid).'], allowed types: ['.implode(', ', self::AGGREGATES).'].');
        }

        $orderBy ??= $aggregates[0];
        $direction = $this->validateDirection($direction);

        if (! in_array($orderBy, $aggregates, true)) {
            throw new InvalidArgumentException("Cannot order by [{$orderBy}]: it is not one of the requested aggregates.");
        }

        if ($limit === 0) {
            return new Collection;
        }

        $window = Window::for($interval);
        $unique = array_values(array_unique($aggregates));

        $query = Dsl::bool(
            filter: [Dsl::term('type', $type), Dsl::since($window->windowStart)],
            should: $this->tailOrBuckets($window, Dsl::terms('aggregations', $unique)),
        );

        $aggs = [
            'tail' => ['filter' => Dsl::before($window->oldestBucket), 'aggs' => ['s' => Dsl::stats()]],
            'raw' => ['filter' => Dsl::raw()],
        ];

        foreach ($unique as $aggregate) {
            $aggs["b_{$aggregate}"] = $this->bucketAggregation(
                [Dsl::term('aggregations', $aggregate), Dsl::since($window->oldestBucket)],
                $aggregate,
                $window,
            );
        }

        $ordering = $this->orderedInPhp($orderBy) ? null : [
            'filter' => Dsl::bool(should: [
                Dsl::before($window->oldestBucket),
                Dsl::bool(filter: [Dsl::term('aggregations', $orderBy), Dsl::since($window->oldestBucket)]),
            ]),
            ...$this->orderMetric($orderBy),
        ];

        $rows = [];

        foreach ($this->keyBuckets($query, $aggs, $ordering, $direction, $limit) as $bucket) {
            $tail = Part::from($bucket['tail']);
            $values = [];

            foreach ($aggregates as $aggregate) {
                $values[$aggregate] = Part::merge($aggregate, $tail->docs > 0, $tail, Part::from($bucket["b_{$aggregate}"]));
            }

            $rows[] = ['key' => (string) $bucket['key'], 'raw' => $bucket['raw']['doc_count'] > 0, 'values' => $values];
        }

        return $this->finishRows($rows, $orderBy, $direction, $limit);
    }

    /**
     * Retrieve aggregate values for the given types.
     *
     * @param  string|list<string>  $types
     * @param  'count'|'min'|'max'|'sum'|'avg'  $aggregate
     * @return Collection<int, object>
     */
    public function aggregateTypes(
        string|array $types,
        string $aggregate,
        CarbonInterval $interval,
        ?string $orderBy = null,
        string $direction = 'desc',
        int $limit = 101,
    ): Collection {
        $this->validateAggregate($aggregate);

        $types = is_array($types) ? array_values($types) : [$types];
        $orderBy ??= $types[0];
        $direction = $this->validateDirection($direction);

        if (! in_array($orderBy, $types, true)) {
            throw new InvalidArgumentException("Cannot order by [{$orderBy}]: it is not one of the requested types.");
        }

        if ($limit === 0) {
            return new Collection;
        }

        $window = Window::for($interval);

        $query = Dsl::bool(
            filter: [Dsl::terms('type', $types), Dsl::since($window->windowStart)],
            should: $this->tailOrBuckets($window, Dsl::term('aggregations', $aggregate)),
        );

        $aggs = [
            'tail' => ['filter' => Dsl::before($window->oldestBucket)],
            'raw' => ['filter' => Dsl::raw()],
        ];

        foreach ($types as $i => $type) {
            $aggs["t{$i}"] = [
                'filter' => Dsl::bool(filter: [Dsl::term('type', $type), Dsl::before($window->oldestBucket)]),
                'aggs' => ['s' => Dsl::stats()],
            ];

            $aggs["b{$i}"] = $this->bucketAggregation(
                [Dsl::term('type', $type), Dsl::since($window->oldestBucket)],
                $aggregate,
                $window,
            );
        }

        // The query already limits tail documents to raw ones and bucket
        // documents to the aggregation, so the type alone selects them.
        $ordering = $this->orderedInPhp($aggregate) ? null : [
            'filter' => Dsl::term('type', $orderBy),
            ...$this->orderMetric($aggregate),
        ];

        $rows = [];

        foreach ($this->keyBuckets($query, $aggs, $ordering, $direction, $limit) as $bucket) {
            $tailRow = $bucket['tail']['doc_count'] > 0;
            $values = [];

            foreach ($types as $i => $type) {
                $values[$type] = Part::merge($aggregate, $tailRow, Part::from($bucket["t{$i}"]), Part::from($bucket["b{$i}"]));
            }

            $rows[] = ['key' => (string) $bucket['key'], 'raw' => $bucket['raw']['doc_count'] > 0, 'values' => $values];
        }

        return $this->finishRows($rows, $orderBy, $direction, $limit);
    }

    /**
     * Retrieve an aggregate total for the given types.
     *
     * @param  string|list<string>  $types
     * @param  'count'|'min'|'max'|'sum'|'avg'  $aggregate
     * @return float|Collection<string, int|float|null>
     */
    public function aggregateTotal(
        array|string $types,
        string $aggregate,
        CarbonInterval $interval,
    ): float|Collection {
        $this->validateAggregate($aggregate);

        $list = is_array($types) ? array_values($types) : [$types];

        if ($list === []) {
            return new Collection;
        }

        $window = Window::for($interval);

        $query = Dsl::bool(
            filter: [Dsl::terms('type', $list), Dsl::since($window->windowStart)],
            should: $this->tailOrBuckets($window, Dsl::term('aggregations', $aggregate)),
        );

        $response = $this->search($this->schema->entries(), [
            'size' => 0,
            'track_total_hits' => false,
            'query' => $query,
            'aggs' => [
                't' => [
                    'terms' => ['field' => 'type', 'size' => count(array_unique($list))],
                    'aggs' => [
                        'tail' => ['filter' => Dsl::before($window->oldestBucket), 'aggs' => ['s' => Dsl::stats()]],
                        'b' => ['filter' => Dsl::since($window->oldestBucket), 'aggs' => ['s' => Dsl::stats()]],
                    ],
                ],
            ],
        ]);

        $means = $aggregate === 'avg' ? $this->bucketMeansPerType($list, $window) : [];

        $totals = [];

        foreach ($response['aggregations']['t']['buckets'] ?? [] as $bucket) {
            $type = (string) $bucket['key'];
            $tail = Part::from($bucket['tail']);
            $buckets = Part::from($bucket['b']);

            if ($aggregate === 'avg') {
                $buckets = $buckets->withAvg($means[$type] ?? null);
            }

            $totals[$type] = Part::merge($aggregate, $tail->docs > 0, $tail, $buckets);
        }

        if (is_array($types)) {
            return new Collection($totals);
        }

        return (float) ($totals[$types] ?? 0);
    }

    /**
     * The two halves of a window as `should` clauses: raw entries in the
     * tail, and entries recorded for the aggregation in the buckets.
     *
     * @param  array<string, mixed>  $aggregations  the bucket side's aggregations filter
     * @return list<array<string, mixed>>
     */
    protected function tailOrBuckets(Window $window, array $aggregations): array
    {
        return [
            Dsl::bool(filter: [Dsl::before($window->oldestBucket)], mustNot: [Dsl::onlyBuckets()]),
            Dsl::bool(filter: [Dsl::since($window->oldestBucket), $aggregations]),
        ];
    }

    /**
     * A filter aggregation over the bucket side of one aggregation. Averages
     * get a date histogram of upstream's buckets and the mean of the bucket
     * means (`m`); the rest get value stats (`s`).
     *
     * @param  list<array<string, mixed>>  $filter
     * @return array<string, mixed>
     */
    protected function bucketAggregation(array $filter, string $aggregate, Window $window): array
    {
        $aggregation = ['filter' => Dsl::bool(filter: $filter)];

        if ($aggregate === 'avg') {
            $aggregation['aggs'] = [
                'h' => [
                    'date_histogram' => [
                        'field' => '@timestamp',
                        'fixed_interval' => $window->fixedInterval(),
                        'min_doc_count' => 1,
                    ],
                    'aggs' => ['v' => ['avg' => ['field' => 'value']]],
                ],
                'm' => ['avg_bucket' => ['buckets_path' => 'h>v']],
            ];
        } elseif ($aggregate !== 'count') {
            $aggregation['aggs'] = ['s' => Dsl::stats()];
        }

        return $aggregation;
    }

    /**
     * The metric under the ordering filter; a count orders by its doc count.
     *
     * @return array<string, mixed>
     */
    protected function orderMetric(string $aggregate): array
    {
        return $aggregate === 'count'
            ? []
            : ['aggs' => ['m' => [$aggregate => ['field' => 'value']]]];
    }

    /**
     * Whether keys ordered by this aggregate are ordered in PHP over every
     * key rather than chosen by Elasticsearch.
     *
     * - avg: upstream's average (tail mean and mean of bucket means) is no
     *   single metric Elasticsearch can order by.
     * - min: Elasticsearch ranks a key without values as +infinity, so it
     *   would pick those first on desc and last on asc, the opposite of
     *   upstream's NULL ordering (MySQL: nulls last on desc, first on asc).
     *
     * max ranks a key without values as -infinity, which is where MySQL puts
     * NULL; count and sum rank it as 0.
     */
    protected function orderedInPhp(string $aggregate): bool
    {
        return $aggregate === 'avg' || $aggregate === 'min';
    }

    /**
     * The per-key buckets of an aggregate read.
     *
     * With an ordering filter the keys are chosen in two passes:
     *
     * 1. candidates: an ordered `terms` aggregation with a large
     *    `shard_size` (`terms_shard_size`). Entries spread over one backing
     *    index per day, each its own shard, and an ordered `terms` merges
     *    per-shard top lists, so with the default `shard_size` the top keys
     *    and their values would be approximate. Each shard returns up to
     *    `shard_size` keys; with no more keys per backing index than that,
     *    the candidates are exact.
     * 2. exact values: the aggregations recomputed for the candidate keys
     *    only (a `terms` filter on them and a `size` of their count, so
     *    every shard returns every candidate).
     *
     * finishRows() then orders and limits by the exact values.
     *
     * Without an ordering filter (see orderedInPhp()) or without a limit,
     * every key is paged through and ordered in PHP.
     *
     * @param  array<string, mixed>  $query
     * @param  array<string, mixed>  $aggs
     * @param  array<string, mixed>|null  $ordering
     * @return iterable<int, array<string, mixed>>
     */
    protected function keyBuckets(array $query, array $aggs, ?array $ordering, string $direction, int $limit): iterable
    {
        if ($ordering === null || $limit < 0) {
            $sources = [['k' => ['terms' => ['field' => 'key']]]];

            foreach ($this->composite($this->schema->entries(), $query, $sources, $aggs, $this->pageSizeFor($aggs)) as $bucket) {
                yield [...$bucket, 'key' => $bucket['key']['k']];
            }

            return;
        }

        $path = isset($ordering['aggs']) ? 'ord>m' : 'ord';
        $shardSize = $this->schema->termsShardSize($limit);

        // A few spare candidates cost little and cover ties at the cut.
        $size = min($shardSize, $limit * 2);

        $response = $this->search($this->schema->entries(), [
            'size' => 0,
            'track_total_hits' => false,
            'query' => $query,
            'aggs' => [
                'k' => [
                    'terms' => [
                        'field' => 'key',
                        'size' => $size,
                        'shard_size' => $shardSize,
                        'order' => [[$path => $direction], ['_key' => 'asc']],
                    ],
                    'aggs' => ['ord' => $ordering],
                ],
            ],
        ]);

        $keys = array_map(
            fn (array $bucket): string => (string) $bucket['key'],
            $response['aggregations']['k']['buckets'] ?? [],
        );

        foreach (array_chunk($keys, self::TERMS_CHUNK) as $chunk) {
            $exact = $this->search($this->schema->entries(), [
                'size' => 0,
                'track_total_hits' => false,
                'query' => Dsl::bool(filter: [$query, Dsl::terms('key', $chunk)]),
                'aggs' => [
                    'k' => [
                        'terms' => ['field' => 'key', 'size' => count($chunk)],
                        'aggs' => $aggs,
                    ],
                ],
            ]);

            yield from $exact['aggregations']['k']['buckets'] ?? [];
        }
    }

    /**
     * Composite page size for per-key buckets carrying these aggregations,
     * so one page stays well under `search.max_buckets` (65536 by default):
     * every filter is one bucket per key and every average a date histogram
     * of up to 61 more (aggregateTypes() by avg has one per type).
     *
     * @param  array<string, mixed>  $aggs
     */
    protected function pageSizeFor(array $aggs): int
    {
        $perKey = 1;

        foreach ($aggs as $aggregation) {
            $perKey += 1 + (isset($aggregation['aggs']['h']) ? 61 : 0);
        }

        return max(10, min(self::PAGE, intdiv(30_000, $perKey)));
    }

    /**
     * Order, limit, resolve keys and build the result rows.
     *
     * Nulls order as upstream on MySQL: last on desc, first on asc; ties
     * order by key ascending.
     *
     * @param  list<array{key: string, raw: bool, values: array<string, int|float|null>}>  $rows
     * @return Collection<int, stdClass>
     */
    protected function finishRows(array $rows, string $orderBy, string $direction, int $limit): Collection
    {
        usort($rows, function (array $a, array $b) use ($orderBy, $direction): int {
            $x = $a['values'][$orderBy];
            $y = $b['values'][$orderBy];

            $result = match (true) {
                $x === null && $y === null => 0,
                $x === null => -1,
                $y === null => 1,
                default => $x <=> $y,
            };

            if ($direction === 'desc') {
                $result = -$result;
            }

            return $result !== 0 ? $result : strcmp($a['key'], $b['key']);
        });

        if ($limit > 0) {
            $rows = array_slice($rows, 0, $limit);
        }

        $known = $this->keysWithRawEntries(array_values(array_map(
            fn (array $row) => $row['key'],
            array_filter($rows, fn (array $row) => ! $row['raw']),
        )));

        return new Collection(array_map(
            fn (array $row) => (object) [
                'key' => $row['raw'] || isset($known[$row['key']]) ? $row['key'] : null,
                ...$row['values'],
            ],
            $rows,
        ));
    }

    /**
     * Which of the keys have a raw entry of any type at any time. Upstream
     * looks a key up in its raw entries table, so a key that only has
     * onlyBuckets() entries is null.
     *
     * @param  list<string>  $keys
     * @return array<string, true>
     */
    protected function keysWithRawEntries(array $keys): array
    {
        $known = [];

        foreach (array_chunk(array_values(array_unique($keys)), self::TERMS_CHUNK) as $chunk) {
            $response = $this->search($this->schema->entries(), [
                'size' => 0,
                'track_total_hits' => false,
                'query' => Dsl::bool(filter: [Dsl::terms('key', $chunk)], mustNot: [Dsl::onlyBuckets()]),
                'aggs' => ['k' => ['terms' => ['field' => 'key', 'size' => count($chunk)]]],
            ]);

            foreach ($response['aggregations']['k']['buckets'] ?? [] as $bucket) {
                $known[(string) $bucket['key']] = true;
            }
        }

        return $known;
    }

    /**
     * Per type, the mean of the bucket averages over every key and bucket:
     * upstream's `avg(value)` over the avg rows of its aggregates table.
     *
     * @param  list<string>  $types
     * @return array<string, float>
     */
    protected function bucketMeansPerType(array $types, Window $window): array
    {
        $query = Dsl::bool(filter: [
            Dsl::terms('type', $types),
            Dsl::term('aggregations', 'avg'),
            Dsl::since($window->oldestBucket),
        ]);

        $sources = [
            ['t' => ['terms' => ['field' => 'type']]],
            ['k' => ['terms' => ['field' => 'key']]],
            ['h' => ['date_histogram' => ['field' => '@timestamp', 'fixed_interval' => $window->fixedInterval()]]],
        ];

        $sums = [];
        $counts = [];

        foreach ($this->composite($this->schema->entries(), $query, $sources, ['v' => ['avg' => ['field' => 'value']]]) as $bucket) {
            $value = $bucket['v']['value'] ?? null;

            if ($value === null) {
                continue;
            }

            $type = (string) $bucket['key']['t'];
            $sums[$type] = ($sums[$type] ?? 0.0) + $value;
            $counts[$type] = ($counts[$type] ?? 0) + 1;
        }

        $means = [];

        foreach ($sums as $type => $sum) {
            $means[(string) $type] = $sum / $counts[$type];
        }

        return $means;
    }

    /**
     * The value of one graph bucket, as upstream stores it in its bucket.
     *
     * @param  array<string, mixed>  $bucket
     */
    protected function graphValue(string $aggregate, array $bucket): int|float|null
    {
        $stats = $bucket['s'] ?? [];
        $hasValues = ($stats['count'] ?? 0) > 0;

        return match ($aggregate) {
            'count' => (int) $bucket['doc_count'],
            'min' => $hasValues ? (int) round($stats['min']) : null,
            'max' => $hasValues ? (int) round($stats['max']) : null,
            // Upstream pre-aggregates a missing value as 0.
            'sum' => (int) round($stats['sum'] ?? 0),
            'avg' => $hasValues ? (float) $stats['avg'] : null,
        };
    }

    /**
     * Page through a composite aggregation.
     *
     * @param  array<string, mixed>  $query
     * @param  list<array<string, mixed>>  $sources
     * @param  array<string, mixed>  $aggs
     * @return Generator<int, array<string, mixed>>
     */
    protected function composite(string $index, array $query, array $sources, array $aggs = [], int $page = self::PAGE): Generator
    {
        $after = null;

        do {
            $composite = ['size' => $page, 'sources' => $sources];

            if ($after !== null) {
                $composite['after'] = $after;
            }

            $aggregation = ['composite' => $composite];

            if ($aggs !== []) {
                $aggregation['aggs'] = $aggs;
            }

            $response = $this->search($index, [
                'size' => 0,
                'track_total_hits' => false,
                'query' => $query,
                'aggs' => ['c' => $aggregation],
            ]);

            $buckets = $response['aggregations']['c']['buckets'] ?? [];

            foreach ($buckets as $bucket) {
                yield $bucket;
            }

            $after = $response['aggregations']['c']['after_key'] ?? null;
        } while ($after !== null && count($buckets) === $page);
    }

    /**
     * Send one `_bulk` request of entries or of values.
     *
     * Version conflicts are expected and ignored: on values an older value
     * arriving after a newer one, on entries a document stored before (a
     * retried batch). Documents rejected with 429 (a full write queue) are
     * resent, alone, up to `bulk_retries` times with a doubling backoff that
     * suspends only the calling fiber; a whole request answered with 429 is
     * resent the same way. Any other item error throws with its reason.
     *
     * @param  list<array{0: array<string, mixed>, 1: array<string, mixed>}>  $operations
     */
    protected function bulk(array $operations, bool $entries): void
    {
        $retries = $this->schema->bulkRetries();

        for ($attempt = 0; ; $attempt++) {
            $rejected = [];
            $lastRejection = null;

            try {
                $response = $this->sendBulk($operations, $entries);
            } catch (ClientResponseException $e) {
                if ($e->getCode() !== 429 || $attempt >= $retries) {
                    throw $entries && $e->getCode() === 404
                        ? BulkException::missingDataStream($this->schema->entries(), previous: $e)
                        : $e;
                }

                $this->backoff($attempt);

                continue;
            }

            if (! ($response['errors'] ?? false)) {
                return;
            }

            foreach ($response['items'] ?? [] as $i => $item) {
                $operation = (string) array_key_first($item);
                $result = $item[$operation];
                $error = $result['error'] ?? null;

                if ($error === null) {
                    continue;
                }

                $type = $error['type'] ?? null;

                if ($type === 'version_conflict_engine_exception') {
                    continue;
                }

                if (($result['status'] ?? null) === 429 || $type === 'es_rejected_execution_exception') {
                    $rejected[] = $operations[$i];
                    $lastRejection = [$operation, (string) ($result['_index'] ?? '?'), (array) $error];

                    continue;
                }

                if ($entries && $this->isMissingDataStream((array) $error)) {
                    throw BulkException::missingDataStream($this->schema->entries(), (array) $error);
                }

                throw BulkException::fromItem($operation, (string) ($result['_index'] ?? '?'), (array) $error);
            }

            if ($rejected === []) {
                return;
            }

            if ($attempt >= $retries) {
                throw BulkException::stillRejected($retries, ...$lastRejection);
            }

            $operations = $rejected;

            $this->backoff($attempt);
        }
    }

    /**
     * @param  list<array{0: array<string, mixed>, 1: array<string, mixed>}>  $operations
     * @return array<string, mixed>
     */
    protected function sendBulk(array $operations, bool $entries): array
    {
        $body = [];

        foreach ($operations as [$action, $document]) {
            $body[] = $action;
            $body[] = $document;
        }

        $params = ['body' => $body];

        if ($entries) {
            $params['require_data_stream'] = true;
        }

        return $this->client->bulk($params)->asArray();
    }

    /**
     * Whether an entry was refused because `{prefix}-entries` is no data
     * stream and no index template would create one.
     *
     * @param  array<string, mixed>  $error
     */
    protected function isMissingDataStream(array $error): bool
    {
        $type = (string) ($error['type'] ?? '');
        $reason = (string) ($error['reason'] ?? '');

        return $type === 'index_not_found_exception'
            || str_contains($reason, 'require_data_stream')
            || str_contains($reason, 'data stream');
    }

    /**
     * Wait before resend `attempt` (0-based): `bulk_backoff_ms`, doubled
     * per attempt.
     */
    protected function backoff(int $attempt): void
    {
        $milliseconds = $this->schema->bulkBackoffMs() * (2 ** $attempt);

        if ($milliseconds > 0) {
            delay($milliseconds / 1000);
        }
    }

    /**
     * @param  array<string, mixed>  $query
     */
    protected function deleteByQuery(string $index, array $query, bool $refresh = false): void
    {
        $this->client->deleteByQuery([
            'index' => $index,
            'body' => ['query' => $query],
            'conflicts' => 'proceed',
            'ignore_unavailable' => true,
            'allow_no_indices' => true,
            'refresh' => $refresh,
        ]);
    }

    /**
     * @param  array<string, mixed>  $body
     * @return array<string, mixed>
     */
    protected function search(string $index, array $body): array
    {
        return $this->client->search(['index' => $index, 'body' => $body])->asArray();
    }

    /**
     * The values document id of a type and key.
     */
    protected function valueId(string $type, string $key): string
    {
        return sha1($type.'|'.$key);
    }

    protected function validateAggregate(string $aggregate): void
    {
        if (! in_array($aggregate, self::AGGREGATES, true)) {
            throw new InvalidArgumentException("Invalid aggregate type [$aggregate], allowed types: [".implode(', ', self::AGGREGATES).'].');
        }
    }

    /**
     * As the query builder's orderBy().
     */
    protected function validateDirection(string $direction): string
    {
        $direction = strtolower($direction);

        if (! in_array($direction, ['asc', 'desc'], true)) {
            throw new InvalidArgumentException('Order direction must be "asc" or "desc".');
        }

        return $direction;
    }
}
