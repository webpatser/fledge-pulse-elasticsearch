<?php

use Fledge\Fiber\Http\FledgeHandler;
use Fledge\Pulse\Elasticsearch\ClientFactory;
use Fledge\Pulse\Elasticsearch\ElasticsearchStorage;
use Fledge\Pulse\Elasticsearch\Schema;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\HandlerStack;
use Laravel\Pulse\Contracts\Storage;
use Laravel\Pulse\Fiber\Storage\FiberDatabaseStorage;
use Laravel\Pulse\Fiber\Storage\StorageManager;

it('resolves the elasticsearch driver through the storage manager', function () {
    config(['pulse.storage.driver' => 'elasticsearch']);

    expect(app(Storage::class))->toBeInstanceOf(ElasticsearchStorage::class)
        ->and(app(StorageManager::class)->driver('elasticsearch'))->toBe(app(Storage::class));
});

it('leaves the default database driver in place', function () {
    expect(config('pulse.storage.driver'))->toBe('database')
        ->and(app(Storage::class))->toBeInstanceOf(FiberDatabaseStorage::class);
});

it('fills pulse.storage.elasticsearch with defaults without overriding the application', function () {
    expect(config('pulse.storage.elasticsearch'))->toBe([
        'connection' => 'elasticsearch',
        'prefix' => $this->prefix,
        'shards' => 1,
        'replicas' => 0,
        'ilm' => true,
        'bulk_chunk' => 1000,
        'bulk_retries' => 3,
        'bulk_backoff_ms' => 500,
        'deduplicate_entries' => true,
        'terms_shard_size' => null,
        'timeout' => 10.0,
        'connect_timeout' => 2.0,
        'retries' => null,
    ])->and(config('pulse.storage.trim.keep'))->toBe('7 days');

    $schema = app(Schema::class);

    expect($schema->entries())->toBe("{$this->prefix}-entries")
        ->and($schema->values())->toBe("{$this->prefix}-values")
        ->and($schema->policy())->toBe("{$this->prefix}-policy")
        ->and($schema->keepSeconds())->toBe(604_800);
});

it('reuses one client per process', function () {
    expect(app(ClientFactory::class)->client())->toBe(app(ClientFactory::class)->client());
});

it('builds the client on the Fledge fiber transport', function () {
    $http = app(ClientFactory::class)->client()->getTransport()->getClient();

    expect($http)->toBeInstanceOf(GuzzleClient::class);

    if (! method_exists($http, 'getConfig')) {
        $this->markTestSkipped('This Guzzle version does not expose its handler.');
    }

    $stack = $http->getConfig('handler');

    expect($stack)->toBeInstanceOf(HandlerStack::class)
        ->and((new ReflectionProperty(HandlerStack::class, 'handler'))->getValue($stack))->toBeInstanceOf(FledgeHandler::class);
});

it('fails with a clear message when the connection is not configured', function () {
    config(['pulse.storage.elasticsearch.connection' => 'missing']);

    app(ClientFactory::class)->connectionConfig();
})->throws(InvalidArgumentException::class, 'Elasticsearch connection [missing] is not configured');
