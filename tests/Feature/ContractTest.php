<?php

use Carbon\Carbon;
use Fledge\Pulse\Elasticsearch\ElasticsearchStorage;
use Fledge\Pulse\Elasticsearch\Installer;
use Fledge\Pulse\Elasticsearch\Schema;
use Laravel\Pulse\Tests\Contract\StorageContract;

/*
| The core storage contract (webpatser/fledge-pulse, tests/Contract) run
| against ElasticsearchStorage on a real Elasticsearch, one test per case so a
| mismatch names the read that differs from upstream DatabaseStorage.
|
| Every test gets its own resource prefix (see TestCase), installs the
| resources, seeds the contract data and deletes everything afterwards.
*/

beforeEach(function () {
    skipUnlessElasticsearch();

    app(Installer::class)->install();
});

afterEach(function () {
    Carbon::setTestNow();

    cleanupElasticsearch();
});

it('honours the storage contract', function (string $case) {
    $schema = app(Schema::class);

    StorageContract::assertCase(
        app(ElasticsearchStorage::class),
        $case,
        fn () => elasticsearch()->indices()->refresh(['index' => $schema->entries().','.$schema->values()]),
    );
})->with(StorageContract::dataset());
