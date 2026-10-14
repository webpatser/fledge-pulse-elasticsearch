<?php

use Carbon\Carbon;
use Carbon\CarbonImmutable;
use Carbon\CarbonInterval;
use Elastic\Elasticsearch\ClientBuilder;
use Fledge\Pulse\Elasticsearch\ElasticsearchStorage;
use Fledge\Pulse\Elasticsearch\Installer;
use Fledge\Pulse\Elasticsearch\Schema;
use Fledge\Pulse\Elasticsearch\Support\BulkException;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use Laravel\Pulse\Entry;
use Laravel\Pulse\Tests\Contract\StorageContract;
use Laravel\Pulse\Value;

/*
| Every test runs at 2000-01-01 13:00:00 UTC (StorageContract::NOW_TIMESTAMP).
| For a 1 hour window the tail is 12:00:01 - 12:00:59 and the buckets start at
| 12:01:00; offsets below are seconds relative to now.
*/

beforeEach(function () {
    skipUnlessElasticsearch();

    Carbon::setTestNow(CarbonImmutable::createFromTimestamp(StorageContract::NOW_TIMESTAMP, 'UTC'));

    app(Installer::class)->install();
});

afterEach(function () {
    Carbon::setTestNow();

    cleanupElasticsearch();
});

function esStorage(): ElasticsearchStorage
{
    return app(ElasticsearchStorage::class);
}

function esRefresh(): void
{
    $schema = app(Schema::class);

    elasticsearch()->indices()->refresh(['index' => $schema->entries().','.$schema->values()]);
}

/**
 * @param  list<Entry|Value>  $items
 */
function esStore(array $items): ElasticsearchStorage
{
    $storage = esStorage();
    $storage->store(collect($items));

    esRefresh();

    return $storage;
}

function esAt(int $offset): int
{
    return StorageContract::NOW_TIMESTAMP + $offset;
}

/**
 * An entry recorded for every aggregation.
 */
function esTiming(int $offset, string $key, int $value, string $type = 'timing'): Entry
{
    return (new Entry(esAt($offset), $type, $key, $value))->count()->min()->max()->sum()->avg();
}

function esCount(int $offset, string $type, string $key): Entry
{
    return (new Entry(esAt($offset), $type, $key))->count();
}

function esCountOf(string $index): int
{
    return elasticsearch()->count(['index' => $index])->asArray()['count'];
}

/**
 * @return list<array<string, mixed>>
 */
function esRows(iterable $rows): array
{
    return collect($rows)->map(fn (object $row) => (array) $row)->values()->all();
}

it('stores values and keeps the newest one per type and key', function () {
    $storage = esStorage();

    $storage->store(collect([
        new Value(esAt(-60), 'system', 'web-1', 'old'),
        new Value(esAt(0), 'system', 'web-1', 'new'),
        new Value(esAt(0), 'system', 'web-2', 'two'),
        new Value(esAt(0), 'other', 'web-1', 'other type'),
    ]));

    // An older value arriving later is a version conflict and ignored.
    $storage->store(collect([new Value(esAt(-30), 'system', 'web-1', 'stale')]));

    // A later write with the same timestamp wins, as an upstream upsert.
    $storage->store(collect([new Value(esAt(0), 'system', 'web-2', 'two again')]));

    esRefresh();

    $values = $storage->values('system');

    expect($values->keys()->sort()->values()->all())->toBe(['web-1', 'web-2'])
        ->and($values['web-1']->timestamp)->toBe(esAt(0))
        ->and($values['web-1']->key)->toBe('web-1')
        ->and($values['web-1']->value)->toBe('new')
        ->and(array_keys(get_object_vars($values['web-1'])))->toBe(['timestamp', 'key', 'value'])
        ->and($values['web-2']->value)->toBe('two again')
        ->and($storage->values('system', ['web-2'])->keys()->all())->toBe(['web-2'])
        ->and($storage->values('system', []))->toHaveCount(2)
        ->and($storage->values('system', ['missing']))->toBeEmpty()
        ->and($storage->values('unknown'))->toBeEmpty()
        ->and($storage->values('other')['web-1']->value)->toBe('other type');
});

it('stores every entry with its aggregations and bucket-only flag', function () {
    esStore([
        esCount(0, 'hit', 'raw'),
        (new Entry(esAt(0), 'cpu', 'web-1', 42))->avg()->max()->onlyBuckets(),
        new Entry(esAt(0), 'plain', 'no-aggregations', 7),
        // Upstream stores this nowhere: no raw row, no bucket.
        (new Entry(esAt(0), 'dropped', 'nothing'))->onlyBuckets(),
    ]);

    $hits = elasticsearch()->search([
        'index' => app(Schema::class)->entries(),
        'body' => ['size' => 10, 'sort' => [['type' => 'asc']]],
    ])->asArray()['hits']['hits'];

    expect(array_map(fn (array $hit) => $hit['_source'], $hits))->toBe([
        ['@timestamp' => esAt(0), 'type' => 'cpu', 'key' => 'web-1', 'value' => 42, 'aggregations' => ['avg', 'max'], 'only_buckets' => true],
        ['@timestamp' => esAt(0), 'type' => 'hit', 'key' => 'raw', 'value' => null, 'aggregations' => ['count'], 'only_buckets' => false],
        ['@timestamp' => esAt(0), 'type' => 'plain', 'key' => 'no-aggregations', 'value' => 7, 'aggregations' => [], 'only_buckets' => false],
    ]);
});

it('plots a graph with upstream bucket keys, padded types and sorted keys', function () {
    $storage = esStore([
        esCount(-600, 'hit', 'b'),
        esCount(-600, 'hit', 'b'),
        esCount(0, 'hit', 'a'),
        esCount(-30, 'hit', 'a'),
        // Before the first bucket (12:01:00): not plotted.
        esCount(-3599, 'hit', 'a'),
        // Upstream has no upper bound: a future bucket is appended.
        esCount(60, 'hit', 'c'),
    ]);

    $graph = $storage->graph(['hit', 'miss'], 'count', CarbonInterval::hour());

    expect($graph->keys()->all())->toBe(['a', 'b', 'c'])
        ->and($graph['a']->keys()->all())->toBe(['hit', 'miss'])
        ->and($graph['a']['hit'])->toHaveCount(60)
        ->and($graph['a']['hit']->keys()->first())->toBe('2000-01-01 12:01:00')
        ->and($graph['a']['hit']->keys()->last())->toBe('2000-01-01 13:00:00')
        ->and($graph['a']['hit']->filter()->all())->toBe(['2000-01-01 12:59:00' => 1, '2000-01-01 13:00:00' => 1])
        ->and($graph['a']['miss']->filter()->all())->toBe([])
        ->and($graph['a']['miss'])->toHaveCount(60)
        ->and($graph['b']['hit']->filter()->all())->toBe(['2000-01-01 12:50:00' => 2])
        ->and($graph['c']['hit'])->toHaveCount(61)
        ->and($graph['c']['hit']->keys()->last())->toBe('2000-01-01 13:01:00')
        ->and($graph['c']['hit']['2000-01-01 13:01:00'])->toBe(1)
        ->and($storage->graph(['unknown'], 'count', CarbonInterval::hour()))->toBeEmpty();
});

it('plots each aggregation per bucket', function (string $aggregate, int|float $expected) {
    $storage = esStore([
        esTiming(-600, 'web', 10),
        esTiming(-590, 'web', 30),
    ]);

    $series = $storage->graph(['timing'], $aggregate, CarbonInterval::hour())['web']['timing'];

    expect($series->filter(fn ($value) => $value !== null)->all())->toEqual(['2000-01-01 12:50:00' => $expected]);
})->with([
    'count' => ['count', 2],
    'min' => ['min', 10],
    'max' => ['max', 30],
    'sum' => ['sum', 40],
    'avg' => ['avg', 20.0],
]);

it('plots six hours in six-minute buckets', function () {
    $storage = esStore([
        esTiming(-3540, 'web', 10),
        esTiming(-3500, 'web', 30),
    ]);

    $series = $storage->graph(['timing'], 'avg', CarbonInterval::hours(6))['web']['timing'];

    expect($series->keys()->first())->toBe('2000-01-01 07:06:00')
        ->and($series->filter()->all())->toEqual(['2000-01-01 12:00:00' => 20.0]);
});

it('aggregates each aggregation over the tail and the buckets', function (string $aggregate, int|float $a, int|float $b) {
    $storage = esStore([
        esTiming(-3600, 'a', 9999), // outside the window
        esTiming(-3599, 'a', 100),  // tail
        esTiming(-600, 'a', 200),   // bucket 12:50
        esTiming(0, 'a', 400),      // bucket 13:00
        esTiming(0, 'a', 600),
        esTiming(0, 'b', 50),
    ]);

    expect(esRows($storage->aggregate('timing', $aggregate, CarbonInterval::hour())))->toEqual([
        ['key' => 'a', $aggregate => $a],
        ['key' => 'b', $aggregate => $b],
    ]);
})->with([
    'count' => ['count', 4, 1],
    'min' => ['min', 100, 50],
    'max' => ['max', 600, 50],
    'sum' => ['sum', 1300, 50],
    // (tail 100 + mean of the bucket means (200, 500) = 350) / 2
    'avg' => ['avg', 225.0, 50.0],
]);

it('aggregates several aggregations in the requested order', function () {
    $storage = esStore([
        esTiming(-3599, 'a', 100),
        esTiming(0, 'a', 300),
        esTiming(0, '42', 5),
    ]);

    expect(esRows($storage->aggregate('timing', ['sum', 'count', 'avg'], CarbonInterval::hour())))->toEqual([
        ['key' => 'a', 'sum' => 400, 'count' => 2, 'avg' => 200.0],
        ['key' => '42', 'sum' => 5, 'count' => 1, 'avg' => 5.0],
    ]);

    expect($storage->aggregate('timing', 'count', CarbonInterval::hour())[1]->key)->toBe('42');
});

it('orders and limits aggregates in Elasticsearch and, for averages, in PHP', function () {
    $storage = esStore([
        esTiming(0, 'a', 100),
        esTiming(0, 'a', 300),
        esTiming(0, 'b', 50),
        esTiming(0, 'c', 1000),
    ]);

    $hour = CarbonInterval::hour();

    expect(esRows($storage->aggregate('timing', ['count', 'min'], $hour, 'min', 'asc', 2)))->toEqual([
        ['key' => 'b', 'count' => 1, 'min' => 50],
        ['key' => 'a', 'count' => 2, 'min' => 100],
    ])
        ->and(esRows($storage->aggregate('timing', 'avg', $hour, null, 'asc', 2)))->toEqual([
            ['key' => 'b', 'avg' => 50.0],
            ['key' => 'a', 'avg' => 200.0],
        ])
        ->and(esRows($storage->aggregate('timing', 'avg', $hour, null, 'desc', 1)))->toEqual([
            ['key' => 'c', 'avg' => 1000.0],
        ])
        ->and($storage->aggregate('timing', 'count', $hour, null, 'desc', 0))->toBeEmpty()
        ->and($storage->aggregate('timing', 'count', $hour, null, 'desc', -1))->toHaveCount(3)
        ->and($storage->aggregate('unknown', 'count', $hour))->toBeEmpty();
});

it('rejects invalid aggregates, orderings and directions', function (Closure $read) {
    $read(esStorage());
})->with([
    'graph aggregate' => [fn (ElasticsearchStorage $storage) => $storage->graph(['a'], 'median', CarbonInterval::hour())],
    'aggregate aggregate' => [fn (ElasticsearchStorage $storage) => $storage->aggregate('a', ['count', 'median'], CarbonInterval::hour())],
    'aggregate order' => [fn (ElasticsearchStorage $storage) => $storage->aggregate('a', 'count', CarbonInterval::hour(), 'max')],
    'aggregate direction' => [fn (ElasticsearchStorage $storage) => $storage->aggregate('a', 'count', CarbonInterval::hour(), null, 'sideways')],
    'aggregateTypes aggregate' => [fn (ElasticsearchStorage $storage) => $storage->aggregateTypes('a', 'median', CarbonInterval::hour())],
    'aggregateTypes order' => [fn (ElasticsearchStorage $storage) => $storage->aggregateTypes(['a', 'b'], 'count', CarbonInterval::hour(), 'c')],
    'aggregateTotal aggregate' => [fn (ElasticsearchStorage $storage) => $storage->aggregateTotal('a', 'median', CarbonInterval::hour())],
])->throws(InvalidArgumentException::class);

it('aggregates types with zero or null for absent types', function () {
    $storage = esStore([
        esCount(-3599, 'hit', 'foo'),
        esCount(0, 'hit', 'foo'),
        esCount(0, 'hit', 'foo'),
        esCount(0, 'miss', 'foo'),
        esCount(-3599, 'hit', 'bar'),
        esCount(0, 'miss', 'baz'),
    ]);

    $hour = CarbonInterval::hour();

    expect(esRows($storage->aggregateTypes(['hit', 'miss'], 'count', $hour)))->toEqual([
        ['key' => 'foo', 'hit' => 3, 'miss' => 1],
        // bar has tail entries, so its absent type counts 0 ...
        ['key' => 'bar', 'hit' => 1, 'miss' => 0],
        // ... baz has none, so its absent type is null.
        ['key' => 'baz', 'hit' => null, 'miss' => 1],
    ])
        ->and($storage->aggregateTypes(['hit', 'miss'], 'count', $hour)[1]->miss)->toBe(0)
        ->and($storage->aggregateTypes(['hit', 'miss'], 'count', $hour)[2]->hit)->toBeNull()
        ->and(esRows($storage->aggregateTypes(['hit', 'miss'], 'count', $hour, 'miss', 'asc', 1)))->toEqual([
            ['key' => 'bar', 'hit' => 1, 'miss' => 0],
        ])
        ->and(esRows($storage->aggregateTypes('hit', 'count', $hour)))->toEqual([
            ['key' => 'foo', 'hit' => 3],
            ['key' => 'bar', 'hit' => 1],
        ]);
});

it('aggregates types by average, ordered in PHP', function () {
    $storage = esStore([
        esTiming(0, 'web-1', 10, 'cpu'),
        esTiming(0, 'web-1', 30, 'cpu'),
        esTiming(0, 'web-2', 70, 'cpu'),
        esTiming(0, 'web-2', 500, 'mem'),
    ]);

    expect(esRows($storage->aggregateTypes(['cpu', 'mem'], 'avg', CarbonInterval::hour())))->toEqual([
        ['key' => 'web-2', 'cpu' => 70.0, 'mem' => 500.0],
        ['key' => 'web-1', 'cpu' => 20.0, 'mem' => null],
    ])
        ->and($storage->aggregateTypes(['cpu', 'mem'], 'avg', CarbonInterval::hour())[1]->mem)->toBeNull();
});

it('totals single and multiple types', function () {
    $storage = esStore([
        esCount(-3599, 'hit', 'foo'),
        esCount(0, 'hit', 'foo'),
        esCount(0, 'hit', 'bar'),
        esCount(0, 'miss', 'foo'),
        esTiming(-3599, 'a', 100),
        esTiming(0, 'a', 5),
        esTiming(0, 'b', 700),
    ]);

    $hour = CarbonInterval::hour();

    $totals = $storage->aggregateTotal(['hit', 'miss', 'unknown'], 'count', $hour);

    expect($totals->sortKeys()->all())->toBe(['hit' => 3, 'miss' => 1])
        ->and($storage->aggregateTotal('hit', 'count', $hour))->toBe(3.0)
        ->and($storage->aggregateTotal('unknown', 'count', $hour))->toBe(0.0)
        ->and($storage->aggregateTotal('timing', 'min', $hour))->toBe(5.0)
        ->and($storage->aggregateTotal('timing', 'max', $hour))->toBe(700.0)
        ->and($storage->aggregateTotal('timing', 'sum', $hour))->toBe(805.0);
});

it('totals averages as the mean of the tail average and the bucket means', function () {
    $storage = esStore([
        (new Entry(esAt(-3599), 'cpu', 'web-1', 100))->avg(),
        (new Entry(esAt(-600), 'cpu', 'web-1', 10))->avg()->onlyBuckets(),
        (new Entry(esAt(-600), 'cpu', 'web-1', 30))->avg()->onlyBuckets(),
        (new Entry(esAt(0), 'cpu', 'web-1', 40))->avg()->onlyBuckets(),
        (new Entry(esAt(0), 'cpu', 'web-2', 70))->avg()->onlyBuckets(),
    ]);

    // Bucket means 20, 40 and 70 average 43.33; with the tail (100): 71.67.
    expect(round($storage->aggregateTotal('cpu', 'avg', CarbonInterval::hour()), 2))->toBe(71.67)
        ->and(round($storage->aggregateTotal(['cpu'], 'avg', CarbonInterval::hour())['cpu'], 2))->toBe(71.67);
});

it('keeps bucket-only entries out of the tail and out of key lookups', function () {
    $storage = esStore([
        esCount(-3599, 'hit', 'foo'),
        esCount(-3599, 'hit', 'foo')->onlyBuckets(), // tail: invisible
        esCount(0, 'hit', 'foo')->onlyBuckets(),     // bucket: counted
        esCount(0, 'hit', 'ghost')->onlyBuckets(),
        esCount(0, 'hit', 'ghost')->onlyBuckets(),
        esCount(0, 'hit', 'shared')->onlyBuckets(),
        // A raw entry of another type, outside the window, still names the key.
        esCount(-7200, 'other', 'shared'),
    ]);

    $hour = CarbonInterval::hour();

    expect(esRows($storage->aggregate('hit', 'count', $hour)))->toBe([
        ['key' => 'foo', 'count' => 2],
        ['key' => null, 'count' => 2],
        ['key' => 'shared', 'count' => 1],
    ])
        ->and(esRows($storage->aggregate('hit', 'count', $hour, null, 'desc', -1)))->toBe([
            ['key' => 'foo', 'count' => 2],
            ['key' => null, 'count' => 2],
            ['key' => 'shared', 'count' => 1],
        ])
        ->and($storage->aggregateTotal('hit', 'count', $hour))->toBe(5.0);

    $graph = $storage->graph(['hit'], 'count', $hour);

    expect($graph['ghost']['hit']['2000-01-01 13:00:00'])->toBe(2)
        ->and($graph['foo']['hit']->filter()->all())->toBe(['2000-01-01 13:00:00' => 1]);
});

it('purges given types or everything', function () {
    $storage = esStore([
        esCount(0, 'a', 'x'),
        esCount(0, 'b', 'x'),
        new Value(esAt(0), 'a', 'x', 'a'),
        new Value(esAt(0), 'b', 'x', 'b'),
    ]);

    $schema = app(Schema::class);

    $storage->purge(['a']);

    // purge() waits for the background entry deletion.
    expect(esCountOf($schema->entries()))->toBe(1)
        ->and(esCountOf($schema->values()))->toBe(1)
        ->and($storage->values('b'))->toHaveCount(1);

    // Unrefreshed writes are purged too.
    $storage->store(collect([esCount(0, 'c', 'x')]));
    $storage->purge();

    expect(esCountOf($schema->entries()))->toBe(0)
        ->and(esCountOf($schema->values()))->toBe(0);
});

it('trims entries by query without ILM and always trims values', function (bool $ilm, int $entriesLeft) {
    config(['pulse.storage.elasticsearch.ilm' => $ilm]);

    $storage = esStore([
        esCount(-8 * 86400, 'hit', 'old'),
        esCount(-86400, 'hit', 'recent'),
        new Value(esAt(-8 * 86400), 'system', 'old', 'old'),
        new Value(esAt(0), 'system', 'new', 'new'),
    ]);

    $storage->trim();
    $storage->awaitDeletes();
    esRefresh();

    $schema = app(Schema::class);

    expect(esCountOf($schema->entries()))->toBe($entriesLeft)
        ->and($storage->values('system')->keys()->all())->toBe(['new']);
})->with([
    'ilm off' => [false, 1],
    'ilm on' => [true, 2],
]);

it('trims at pulse.storage.trim.keep', function () {
    config(['pulse.storage.elasticsearch.ilm' => false, 'pulse.storage.trim.keep' => '1 hour']);

    $storage = esStore([
        esCount(-3600, 'hit', 'edge'),  // <= cut-off: trimmed, as upstream
        esCount(-3599, 'hit', 'kept'),
    ]);

    $storage->trim();
    $storage->awaitDeletes();
    esRefresh();

    expect($storage->aggregate('hit', 'count', CarbonInterval::hours(6))->pluck('key')->all())->toBe(['kept']);
});

it('sends one bulk request per bulk_chunk documents', function () {
    config(['pulse.storage.elasticsearch.bulk_chunk' => 2]);

    $history = [];
    $stack = HandlerStack::create();
    $stack->push(Middleware::history($history));

    $connection = elasticsearchConnectionConfig();
    $builder = ClientBuilder::create()
        ->setHosts($connection['hosts'])
        ->setHttpClient(new GuzzleClient(['handler' => $stack, 'verify' => false]));

    if ($connection['username'] !== '') {
        $builder->setBasicAuthentication($connection['username'], $connection['password']);
    }

    $storage = new ElasticsearchStorage($builder->build(), app('config'));

    $storage->store(collect([
        esCount(0, 'hit', 'a'),
        esCount(0, 'hit', 'b'),
        esCount(0, 'hit', 'c'),
        esCount(0, 'hit', 'd'),
        new Value(esAt(0), 'system', 'web-1', 'v'),
    ]));

    esRefresh();

    $bulks = array_filter($history, fn (array $transaction) => str_ends_with($transaction['request']->getUri()->getPath(), '/_bulk'));

    expect($bulks)->toHaveCount(3)
        ->and(esCountOf(app(Schema::class)->entries()))->toBe(4)
        ->and($storage->values('system'))->toHaveCount(1);
});

it('throws when Elasticsearch rejects a document', function () {
    elasticsearch()->indices()->close(['index' => app(Schema::class)->values()]);

    esStorage()->store(collect([new Value(esAt(0), 'system', 'web-1', 'v')]));
})->throws(BulkException::class, 'index_closed_exception');
