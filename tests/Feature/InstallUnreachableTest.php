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

it('does not report any resource as installed when Elasticsearch is unreachable', function () {
    config(['database.connections.elasticsearch.hosts' => ['http://127.0.0.1:1']]);

    $this->artisan('pulse:elasticsearch:install')
        ->doesntExpectOutputToContain('created')
        ->doesntExpectOutputToContain('updated')
        ->assertExitCode(1);
});
