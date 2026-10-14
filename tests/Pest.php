<?php

use Elastic\Elasticsearch\Client;
use Fledge\Pulse\Elasticsearch\ClientFactory;
use Fledge\Pulse\Elasticsearch\Schema;
use Fledge\Pulse\Elasticsearch\Tests\TestCase;

uses(TestCase::class)->in('Feature');

/*
|--------------------------------------------------------------------------
| Elasticsearch helpers
|--------------------------------------------------------------------------
|
| Integration tests run against a real Elasticsearch at ES_HOST (default
| http://localhost:9200, optional ES_USERNAME / ES_PASSWORD) and skip when
| it does not answer.
|
*/

/**
 * @return array<string, mixed>
 */
function elasticsearchConnectionConfig(): array
{
    return [
        'driver' => 'elasticsearch',
        'auth_type' => 'http',
        'hosts' => [getenv('ES_HOST') ?: 'http://localhost:9200'],
        'username' => getenv('ES_USERNAME') ?: '',
        'password' => getenv('ES_PASSWORD') ?: '',
        'options' => [
            'ssl_verification' => false,
        ],
    ];
}

function elasticsearchAvailable(): bool
{
    static $available = null;

    if ($available !== null) {
        return $available;
    }

    $config = elasticsearchConnectionConfig();

    $headers = $config['username'] !== ''
        ? 'Authorization: Basic '.base64_encode($config['username'].':'.$config['password'])
        : '';

    $context = stream_context_create([
        'http' => ['timeout' => 2, 'ignore_errors' => true, 'header' => $headers],
        'ssl' => ['verify_peer' => false, 'verify_peer_name' => false],
    ]);

    $body = @file_get_contents($config['hosts'][0], false, $context);

    return $available = is_string($body) && str_contains($body, '"number"');
}

function skipUnlessElasticsearch(): void
{
    if (! elasticsearchAvailable()) {
        test()->markTestSkipped('Elasticsearch is not reachable at '.elasticsearchConnectionConfig()['hosts'][0].'.');
    }
}

function elasticsearch(): Client
{
    return app(ClientFactory::class)->client();
}

/**
 * Delete every resource of the current test prefix; missing ones are fine.
 */
function cleanupElasticsearch(): void
{
    if (! elasticsearchAvailable()) {
        return;
    }

    $schema = app(Schema::class);
    $client = elasticsearch();

    $calls = [
        fn () => $client->indices()->deleteDataStream(['name' => $schema->entries()]),
        fn () => $client->indices()->deleteIndexTemplate(['name' => $schema->entries()]),
        fn () => $client->ilm()->deleteLifecycle(['policy' => $schema->policy()]),
        fn () => $client->indices()->delete(['index' => $schema->values()]),
    ];

    foreach ($calls as $call) {
        rescue($call, report: false);
    }
}
