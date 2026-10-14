<?php

use Elastic\Elasticsearch\Exception\ClientResponseException;
use Fledge\Pulse\Elasticsearch\Schema;

beforeEach(function () {
    skipUnlessElasticsearch();
});

afterEach(function () {
    cleanupElasticsearch();
});

/**
 * The ILM policy names of every backing index of the entries data stream.
 *
 * @return list<string|null>
 */
function backingIndexPolicies(Schema $schema): array
{
    $settings = elasticsearch()->indices()->getSettings(['index' => $schema->entries()])->asArray();

    return array_values(array_map(
        fn (array $index): ?string => $index['settings']['index']['lifecycle']['name'] ?? null,
        $settings,
    ));
}

it('installs every resource and runs again without error', function () {
    $this->artisan('pulse:elasticsearch:install')
        ->expectsOutputToContain('created')
        ->assertSuccessful();

    $this->artisan('pulse:elasticsearch:install')
        ->expectsOutputToContain('updated')
        ->assertSuccessful();

    $schema = app(Schema::class);
    $client = elasticsearch();

    $policy = $client->ilm()->getLifecycle(['policy' => $schema->policy()])->asArray()[$schema->policy()]['policy'];
    expect($policy['phases']['hot']['actions']['rollover'])->toMatchArray(['max_age' => '86400s', 'max_primary_shard_size' => '50gb'])
        ->and($policy['phases']['delete']['min_age'])->toBe('604800s')
        ->and($policy['phases']['delete']['actions'])->toHaveKey('delete');

    $template = $client->indices()->getIndexTemplate(['name' => $schema->entries()])->asArray()['index_templates'][0]['index_template'];
    expect($template['index_patterns'])->toBe([$schema->entries().'*'])
        ->and($template)->toHaveKey('data_stream')
        ->and($template['template']['settings']['index'])->toMatchArray([
            'number_of_shards' => '1',
            'number_of_replicas' => '0',
            'lifecycle' => ['name' => $schema->policy()],
        ])
        ->and($template['template']['mappings']['properties'])->toMatchArray([
            'type' => ['type' => 'keyword'],
            'key' => ['type' => 'keyword', 'ignore_above' => Schema::KEY_IGNORE_ABOVE],
            'value' => ['type' => 'long'],
        ])
        ->and($template['template']['mappings']['properties']['@timestamp']['type'])->toBe('date');

    $streams = $client->indices()->getDataStream(['name' => $schema->entries()])->asArray()['data_streams'];
    expect($streams)->toHaveCount(1)
        ->and($streams[0]['name'])->toBe($schema->entries())
        ->and($streams[0]['template'])->toBe($schema->entries())
        ->and(backingIndexPolicies($schema))->each->toBe($schema->policy());

    $values = $client->indices()->getMapping(['index' => $schema->values()])->asArray()[$schema->values()]['mappings'];
    expect($values['dynamic'])->toBe('strict')
        ->and($values['properties'])->toMatchArray([
            'timestamp' => ['type' => 'date', 'format' => 'epoch_second'],
            'type' => ['type' => 'keyword'],
            'key' => ['type' => 'keyword', 'ignore_above' => Schema::KEY_IGNORE_ABOVE],
            'value' => ['type' => 'text', 'index' => false],
        ]);
});

it('accepts documents in the shape the storage writes', function () {
    $this->artisan('pulse:elasticsearch:install')->assertSuccessful();

    $schema = app(Schema::class);
    $client = elasticsearch();

    $client->index([
        'index' => $schema->entries(),
        'op_type' => 'create',
        'refresh' => 'true',
        'body' => ['@timestamp' => 946_731_600, 'type' => 'slow_request', 'key' => '["GET","/"]', 'value' => 1234],
    ]);

    $client->index([
        'index' => $schema->entries(),
        'op_type' => 'create',
        'refresh' => 'true',
        'body' => ['@timestamp' => 946_731_601, 'type' => 'user_request', 'key' => '1', 'value' => null],
    ]);

    $client->index([
        'index' => $schema->values(),
        'id' => sha1('system|server-1'),
        'version' => 946_731_600,
        'version_type' => 'external',
        'refresh' => 'true',
        'body' => ['timestamp' => 946_731_600, 'type' => 'system', 'key' => 'server-1', 'value' => json_encode(['cpu' => 12, 'name' => 'server-1'])],
    ]);

    expect($client->count(['index' => $schema->entries()])->asArray()['count'])->toBe(2);

    $value = $client->get(['index' => $schema->values(), 'id' => sha1('system|server-1')])->asArray()['_source'];
    expect($value['timestamp'])->toBe(946_731_600)
        ->and($value['value'])->toBe('{"cpu":12,"name":"server-1"}');

    // The mappings are strict: a field the storage does not write is rejected.
    expect(fn () => $client->index([
        'index' => $schema->entries(),
        'op_type' => 'create',
        'body' => ['@timestamp' => 946_731_600, 'type' => 'x', 'key' => 'y', 'value' => 1, 'unexpected' => true],
    ]))->toThrow(ClientResponseException::class);
});

it('skips the ILM policy when ilm is disabled', function () {
    config(['pulse.storage.elasticsearch.ilm' => false]);

    $this->artisan('pulse:elasticsearch:install')
        ->expectsOutputToContain('skipped')
        ->assertSuccessful();

    $this->artisan('pulse:elasticsearch:install')->assertSuccessful();

    $schema = app(Schema::class);
    $client = elasticsearch();

    expect(fn () => $client->ilm()->getLifecycle(['policy' => $schema->policy()]))
        ->toThrow(ClientResponseException::class);

    $template = $client->indices()->getIndexTemplate(['name' => $schema->entries()])->asArray()['index_templates'][0]['index_template'];
    expect($template['template']['settings']['index'])->not->toHaveKey('lifecycle');

    expect(backingIndexPolicies($schema))->each->toBeNull()
        ->and($client->indices()->exists(['index' => $schema->values()])->asBool())->toBeTrue();
});

it('applies the policy to an existing data stream when ilm is switched on', function () {
    config(['pulse.storage.elasticsearch.ilm' => false]);
    $this->artisan('pulse:elasticsearch:install')->assertSuccessful();

    $schema = app(Schema::class);
    expect(backingIndexPolicies($schema))->each->toBeNull();

    config(['pulse.storage.elasticsearch.ilm' => true]);
    $this->artisan('pulse:elasticsearch:install')->assertSuccessful();

    expect(backingIndexPolicies($schema))->each->toBe($schema->policy());
});

it('derives retention from pulse.storage.trim.keep, capped at 7 days', function (string $keep, string $deleteAfter, string $rollover) {
    config(['pulse.storage.trim.keep' => $keep]);

    $this->artisan('pulse:elasticsearch:install')->assertSuccessful();

    $schema = app(Schema::class);
    $phases = elasticsearch()->ilm()->getLifecycle(['policy' => $schema->policy()])->asArray()[$schema->policy()]['policy']['phases'];

    expect($phases['delete']['min_age'])->toBe($deleteAfter)
        ->and($phases['hot']['actions']['rollover']['max_age'])->toBe($rollover);
})->with([
    '2 days' => ['2 days', '172800s', '86400s'],
    '6 hours' => ['6 hours', '21600s', '21600s'],
    '30 days' => ['30 days', '604800s', '86400s'],
]);

it('fails cleanly when Elasticsearch rejects the request', function () {
    // An existing regular index under the data stream name blocks the stream.
    $schema = app(Schema::class);
    elasticsearch()->indices()->create(['index' => $schema->entries()]);

    try {
        $this->artisan('pulse:elasticsearch:install')
            ->expectsOutputToContain('Installing the Pulse Elasticsearch resources failed')
            ->assertFailed();
    } finally {
        rescue(fn () => elasticsearch()->indices()->delete(['index' => $schema->entries()]), report: false);
    }
});
