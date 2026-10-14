<?php

use Carbon\Carbon;
use Carbon\CarbonImmutable;
use Carbon\CarbonInterval;
use Fledge\Pulse\Elasticsearch\ElasticsearchStorage;
use Fledge\Pulse\Elasticsearch\Installer;
use Fledge\Pulse\Elasticsearch\Schema;
use Fledge\Pulse\Elasticsearch\Support\BulkException;
use Laravel\Pulse\Entry;
use Laravel\Pulse\Tests\Contract\StorageContract;

/*
| Regression tests for the 5d review findings, against a real Elasticsearch.
| Time is frozen at 2000-01-01 13:00:00 UTC (StorageContract::NOW_TIMESTAMP).
*/

beforeEach(function () {
    skipUnlessElasticsearch();

    Carbon::setTestNow(CarbonImmutable::createFromTimestamp(StorageContract::NOW_TIMESTAMP, 'UTC'));
});

afterEach(function () {
    Carbon::setTestNow();

    cleanupElasticsearch();
});

function reviewStorage(): ElasticsearchStorage
{
    return app(ElasticsearchStorage::class);
}

function reviewRefresh(): void
{
    $schema = app(Schema::class);

    elasticsearch()->indices()->refresh(['index' => $schema->entries().','.$schema->values(), 'ignore_unavailable' => true]);
}

function reviewHit(string $key, int $offset = 0): Entry
{
    return (new Entry(StorageContract::NOW_TIMESTAMP + $offset, 'hit', $key))->count();
}

/**
 * Twenty keys with ten entries each, plus `x` the given number of times.
 *
 * @return list<Entry>
 */
function reviewBatch(string $prefix, int $x): array
{
    $entries = [];

    foreach (range(1, 20) as $k) {
        foreach (range(1, 10) as $i) {
            $entries[] = reviewHit("{$prefix}{$k}");
        }
    }

    foreach (range(1, $x) as $i) {
        $entries[] = reviewHit('x');
    }

    return $entries;
}

/**
 * @return list<array<string, mixed>>
 */
function reviewRows(iterable $rows): array
{
    return collect($rows)->map(fn (object $row) => (array) $row)->values()->all();
}

it('orders keys exactly across several backing indices', function () {
    app(Installer::class)->install();

    $schema = app(Schema::class);
    $storage = reviewStorage();

    // x has 5 entries in the first backing index and 6 in the second: 11 in
    // total, the most of any key, yet in neither index's top 11 on its own.
    $storage->store(collect(reviewBatch('a', 5)));
    elasticsearch()->indices()->rollover(['alias' => $schema->entries()]);
    $storage->store(collect(reviewBatch('b', 6)));
    reviewRefresh();

    $backing = elasticsearch()->indices()->getDataStream(['name' => $schema->entries()])->asArray()['data_streams'][0]['indices'];

    expect($backing)->toHaveCount(2)
        ->and(reviewRows($storage->aggregate('hit', 'count', CarbonInterval::hour(), null, 'desc', 1)))->toBe([
            ['key' => 'x', 'count' => 11],
        ])
        ->and(reviewRows($storage->aggregate('hit', 'count', CarbonInterval::hour(), null, 'desc', 3)))->toBe([
            ['key' => 'x', 'count' => 11],
            ['key' => 'a1', 'count' => 10],
            ['key' => 'a10', 'count' => 10],
        ])
        ->and(reviewRows($storage->aggregateTypes('hit', 'count', CarbonInterval::hour(), null, 'desc', 1)))->toBe([
            ['key' => 'x', 'hit' => 11],
        ]);
});

it('stays exact with a tiny terms_shard_size thanks to the exact second pass', function () {
    config(['pulse.storage.elasticsearch.terms_shard_size' => 1]);

    app(Installer::class)->install();

    $schema = app(Schema::class);
    $storage = reviewStorage();

    $storage->store(collect(reviewBatch('a', 5)));
    elasticsearch()->indices()->rollover(['alias' => $schema->entries()]);
    $storage->store(collect(reviewBatch('b', 6)));
    reviewRefresh();

    // Candidates may be off with a shard size this small, but every value
    // returned is recomputed exactly.
    $rows = reviewRows($storage->aggregate('hit', 'count', CarbonInterval::hour(), null, 'desc', 5));

    expect($rows)->toHaveCount(5)
        ->and(collect($rows)->every(fn (array $row) => $row['count'] === ($row['key'] === 'x' ? 11 : 10)))->toBeTrue();
});

it('does not count a batch twice when it is stored again', function () {
    app(Installer::class)->install();

    $storage = reviewStorage();

    $batch = [reviewHit('a'), reviewHit('a'), reviewHit('b')];

    $storage->store(collect($batch));
    $storage->store(collect($batch));

    // The ingest re-reads a grown stream: the leading entries dedupe, the
    // new one is stored.
    $storage->store(collect([...$batch, reviewHit('c')]));
    reviewRefresh();

    expect(reviewRows($storage->aggregate('hit', 'count', CarbonInterval::hour())))->toBe([
        ['key' => 'a', 'count' => 2],
        ['key' => 'b', 'count' => 1],
        ['key' => 'c', 'count' => 1],
    ]);
});

it('stores every copy without entry deduplication', function () {
    config(['pulse.storage.elasticsearch.deduplicate_entries' => false]);

    app(Installer::class)->install();

    $storage = reviewStorage();

    $storage->store(collect([reviewHit('a')]));
    $storage->store(collect([reviewHit('a')]));
    reviewRefresh();

    expect(reviewRows($storage->aggregate('hit', 'count', CarbonInterval::hour())))->toBe([
        ['key' => 'a', 'count' => 2],
    ]);
});

it('refuses entries before the install instead of creating a plain index', function () {
    $schema = app(Schema::class);

    expect(fn () => reviewStorage()->store(collect([reviewHit('a')])))
        ->toThrow(BulkException::class, 'pulse:elasticsearch:install');

    expect(elasticsearch()->indices()->exists(['index' => $schema->entries()])->asBool())->toBeFalse();
});

it('orders nulls last on desc and first on asc', function (string $aggregate) {
    app(Installer::class)->install();

    $storage = reviewStorage();

    $at = StorageContract::NOW_TIMESTAMP;

    $storage->store(collect([
        (new Entry($at, 'timing', 'a', 100))->count()->min()->max(),
        (new Entry($at, 'timing', 'b', 50))->count()->min()->max(),
        // Only counted: no min or max for c.
        (new Entry($at, 'timing', 'c', 70))->count(),
    ]));
    reviewRefresh();

    $hour = CarbonInterval::hour();
    $read = fn (string $direction, int $limit = 101) => array_column(
        reviewRows($storage->aggregate('timing', ['count', $aggregate], $hour, $aggregate, $direction, $limit)),
        $aggregate,
        'key',
    );

    expect($read('desc'))->toBe(['a' => 100, 'b' => 50, 'c' => null])
        ->and($read('asc'))->toBe(['c' => null, 'b' => 50, 'a' => 100])
        ->and($read('asc', 1))->toBe(['c' => null])
        ->and($read('desc', 1))->toBe(['a' => 100]);
})->with(['max', 'min']);

it('runs a trim deletion in the background and a purge to completion', function () {
    config(['pulse.storage.elasticsearch.ilm' => false]);

    app(Installer::class)->install();

    $schema = app(Schema::class);
    $storage = reviewStorage();

    $storage->store(collect([reviewHit('old', -8 * 86400), reviewHit('new')]));
    reviewRefresh();

    $storage->trim();
    $storage->awaitDeletes();

    expect(elasticsearch()->count(['index' => $schema->entries()])->asArray()['count'])->toBe(1);

    $storage->purge();

    expect(elasticsearch()->count(['index' => $schema->entries()])->asArray()['count'])->toBe(0);
});
