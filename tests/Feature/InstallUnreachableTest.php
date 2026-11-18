<?php

/*
| The install command against a host nothing listens on: it must fail with a
| readable message and a non-zero exit code, and must not hang. No
| Elasticsearch is needed (nor wanted) for these tests.
*/

it('fails with a clear error when Elasticsearch is unreachable', function () {
    config(['database.connections.elasticsearch.hosts' => ['http://127.0.0.1:1']]);

    $started = hrtime(true);

    $this->artisan('pulse:elasticsearch:install')
        ->expectsOutputToContain('Installing the Pulse Elasticsearch resources failed')
        ->assertFailed();

    $seconds = (hrtime(true) - $started) / 1e9;

    expect($seconds)->toBeLessThan(15.0);
});

it('warns when entry deduplication is forced on with the storage ingest', function () {
    config([
        'database.connections.elasticsearch.hosts' => ['http://127.0.0.1:1'],
        'pulse.ingest.driver' => 'storage',
        'pulse.storage.elasticsearch.deduplicate_entries' => true,
    ]);

    $this->artisan('pulse:elasticsearch:install')
        ->expectsOutputToContain('deduplicate_entries')
        ->assertFailed();
});

it('does not warn about entry deduplication by default', function (string $ingest) {
    config([
        'database.connections.elasticsearch.hosts' => ['http://127.0.0.1:1'],
        'pulse.ingest.driver' => $ingest,
    ]);

    $this->artisan('pulse:elasticsearch:install')
        ->doesntExpectOutputToContain('deduplicate_entries')
        ->assertFailed();
})->with(['storage', 'redis']);

it('does not report any resource as installed when Elasticsearch is unreachable', function () {
    config(['database.connections.elasticsearch.hosts' => ['http://127.0.0.1:1']]);

    $this->artisan('pulse:elasticsearch:install')
        ->doesntExpectOutputToContain('created')
        ->doesntExpectOutputToContain('updated')
        ->assertExitCode(1);
});
