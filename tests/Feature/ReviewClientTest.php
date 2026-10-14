<?php

use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\ClientBuilder;
use Elastic\Transport\Exception\NoNodeAvailableException;
use Elastic\Transport\NodePool\Node;
use Fledge\Pulse\Elasticsearch\ClientFactory;
use Fledge\Pulse\Elasticsearch\ElasticsearchServiceProvider;
use Fledge\Pulse\Elasticsearch\ElasticsearchStorage;
use Fledge\Pulse\Elasticsearch\Support\BulkException;
use Fledge\Pulse\Elasticsearch\Support\ResurrectingNodePool;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Laravel\Pulse\Entry;
use Psr\Http\Message\RequestInterface;

/*
| Regression tests for the 5d review findings that need no Elasticsearch:
| the client's node pool, timeouts and retries, 429 handling in `_bulk`, and
| the config merge. The Guzzle MockHandler is an external dependency.
*/

/**
 * @param  array<string, mixed>  $body
 */
function reviewEsResponse(array $body, int $status = 200): Response
{
    return new Response(
        $status,
        ['X-Elastic-Product' => 'Elasticsearch', 'Content-Type' => 'application/json'],
        json_encode($body, JSON_THROW_ON_ERROR),
    );
}

/**
 * A storage on a mocked transport.
 *
 * @param  list<Response|Throwable>  $responses
 */
function reviewMockedStorage(array $responses, mixed &$history = null): ElasticsearchStorage
{
    $history = [];

    $stack = HandlerStack::create(new MockHandler($responses));
    $stack->push(Middleware::history($history));

    $client = ClientBuilder::create()
        ->setHosts(['http://es.test:9200'])
        ->setHttpClient(new GuzzleClient(['handler' => $stack]))
        ->build();

    return new ElasticsearchStorage($client, app('config'));
}

function reviewEntry(string $key): Entry
{
    return (new Entry(946_731_600, 'hit', $key))->count();
}

/**
 * The action lines of a recorded `_bulk` request.
 *
 * @param  array{request: RequestInterface}  $transaction
 * @return list<array<string, mixed>>
 */
function reviewActions(array $transaction): array
{
    $lines = array_values(array_filter(explode("\n", (string) $transaction['request']->getBody())));

    return array_values(array_map(
        fn (string $line) => json_decode($line, true),
        array_filter($lines, fn (int $i) => $i % 2 === 0, ARRAY_FILTER_USE_KEY),
    ));
}

it('resends only the documents rejected with 429, then succeeds', function () {
    config(['pulse.storage.elasticsearch.bulk_backoff_ms' => 1]);

    $storage = reviewMockedStorage([
        reviewEsResponse(['took' => 1, 'errors' => true, 'items' => [
            ['create' => ['_index' => 'e', 'status' => 201]],
            ['create' => ['_index' => 'e', 'status' => 429, 'error' => ['type' => 'es_rejected_execution_exception', 'reason' => 'queue full']]],
        ]]),
        reviewEsResponse(['took' => 1, 'errors' => false, 'items' => [
            ['create' => ['_index' => 'e', 'status' => 201]],
        ]]),
    ], $history);

    $storage->store(collect([reviewEntry('a'), reviewEntry('b')]));

    $first = reviewActions($history[0]);
    $second = reviewActions($history[1]);

    expect($history)->toHaveCount(2)
        ->and($first)->toHaveCount(2)
        ->and($second)->toHaveCount(1)
        ->and($second[0]['create']['_id'])->toBe($first[1]['create']['_id']);
});

it('throws once 429 rejections outlast bulk_retries', function () {
    config(['pulse.storage.elasticsearch.bulk_backoff_ms' => 1, 'pulse.storage.elasticsearch.bulk_retries' => 2]);

    $rejected = fn () => reviewEsResponse(['took' => 1, 'errors' => true, 'items' => [
        ['create' => ['_index' => 'e', 'status' => 429, 'error' => ['type' => 'es_rejected_execution_exception', 'reason' => 'queue full']]],
    ]]);

    $storage = reviewMockedStorage([$rejected(), $rejected(), $rejected()], $history);

    expect(fn () => $storage->store(collect([reviewEntry('a')])))
        ->toThrow(BulkException::class, 'still rejected after 2 retries');

    expect($history)->toHaveCount(3);
});

it('resends a whole request answered with 429', function () {
    config(['pulse.storage.elasticsearch.bulk_backoff_ms' => 1]);

    $storage = reviewMockedStorage([
        reviewEsResponse(['error' => ['type' => 'es_rejected_execution_exception', 'reason' => 'queue full'], 'status' => 429], 429),
        reviewEsResponse(['took' => 1, 'errors' => false, 'items' => [['create' => ['_index' => 'e', 'status' => 201]]]]),
    ], $history);

    $storage->store(collect([reviewEntry('a')]));

    expect($history)->toHaveCount(2);
});

it('passes timeouts, the resurrecting node pool and retries to the client', function () {
    config([
        'pulse.storage.elasticsearch.timeout' => 7,
        'pulse.storage.elasticsearch.connect_timeout' => 1.5,
    ]);

    $client = app(ClientFactory::class)->make(['hosts' => ['http://es.test:9200']]);
    $transport = $client->getTransport();

    expect($transport->getClient()->getConfig('timeout'))->toBe(7.0)
        ->and($transport->getClient()->getConfig('connect_timeout'))->toBe(1.5)
        ->and($transport->getClient()->getConfig('handler'))->toBeInstanceOf(HandlerStack::class)
        ->and($transport->getNodePool())->toBeInstanceOf(ResurrectingNodePool::class)
        ->and($transport->getRetries())->toBe(0)
        ->and(app(ClientFactory::class)->make(['hosts' => ['http://a:9200', 'http://b:9200']])->getTransport()->getRetries())->toBe(1);

    config(['pulse.storage.elasticsearch.retries' => 2]);

    expect(app(ClientFactory::class)->make(['hosts' => ['http://es.test:9200']])->getTransport()->getRetries())->toBe(2);
});

it('defaults to a 10 second timeout and a 2 second connect timeout', function () {
    expect(app(ClientFactory::class)->httpOptions())->toBe(['timeout' => 10.0, 'connect_timeout' => 2.0]);
});

/** A clock the test moves by hand. */
final class ReviewClock
{
    public float $now = 1000.0;

    public function __invoke(): float
    {
        return $this->now;
    }
}

/** Guzzle handler standing in for the cluster: records every attempt and times out while told to. */
final class ReviewFlakyCluster
{
    /** @var list<string> */
    public array $attempts = [];

    public function __construct(public int $failures = 0) {}

    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $this->attempts[] = $request->getUri()->getHost();

        if ($this->failures > 0) {
            $this->failures--;

            return Create::rejectionFor(new ConnectException('Connection timed out', $request));
        }

        return Create::promiseFor(reviewEsResponse(['version' => ['number' => '8.19.19']]));
    }
}

/**
 * A client on the flaky cluster with a ResurrectingNodePool (5s window) on the test clock.
 *
 * @param  list<string>  $hosts
 */
function reviewPoolClient(ReviewFlakyCluster $cluster, ReviewClock $clock, array $hosts, int $retries): Client
{
    $client = ClientBuilder::create()
        ->setHosts($hosts)
        ->setHttpClient(new GuzzleClient(['handler' => HandlerStack::create($cluster)]))
        ->setNodePool(new ResurrectingNodePool(ResurrectingNodePool::DEFAULT_RESURRECT_AFTER, Closure::fromCallable($clock)))
        ->build();

    // ClientBuilder treats 0 as unset, so the count goes on the transport.
    $client->getTransport()->setRetries($retries);

    return $client;
}

it('is left dead for good by the transport default pool after a network error on its only host', function () {
    $stack = HandlerStack::create(new MockHandler([
        new ConnectException('Connection timed out', new Request('GET', '/')),
        reviewEsResponse(['version' => ['number' => '8.19.19']]),
    ]));

    $default = ClientBuilder::create()
        ->setHosts(['http://es.test:9200'])
        ->setHttpClient(new GuzzleClient(['handler' => $stack]))
        ->build();

    expect(fn () => $default->info())->toThrow(NoNodeAvailableException::class)
        ->and(fn () => $default->info())->toThrow(NoNodeAvailableException::class, 'No alive nodes');
});

it('makes one HTTP attempt on a failing single host even with retries, then fails fast', function () {
    $cluster = new ReviewFlakyCluster(failures: 1);
    $client = reviewPoolClient($cluster, new ReviewClock, ['http://es.test:9200'], retries: 3);

    expect(fn () => $client->info())->toThrow(NoNodeAvailableException::class)
        ->and($cluster->attempts)->toBe(['es.test']);
});

it('fails fast without an HTTP attempt while the dead host is inside its window', function () {
    $cluster = new ReviewFlakyCluster(failures: 1);
    $clock = new ReviewClock;
    $client = reviewPoolClient($cluster, $clock, ['http://es.test:9200'], retries: 3);

    expect(fn () => $client->info())->toThrow(NoNodeAvailableException::class);

    $clock->now += 4.9;

    expect(fn () => $client->info())->toThrow(NoNodeAvailableException::class, 'No alive nodes')
        ->and(fn () => $client->info())->toThrow(NoNodeAvailableException::class, 'No alive nodes')
        ->and($cluster->attempts)->toBe(['es.test']);
});

it('retries the dead host after its window and recovers', function () {
    $cluster = new ReviewFlakyCluster(failures: 1);
    $clock = new ReviewClock;
    $client = reviewPoolClient($cluster, $clock, ['http://es.test:9200'], retries: 3);

    expect(fn () => $client->info())->toThrow(NoNodeAvailableException::class);

    $clock->now += 5.1;

    expect($client->info()->asArray()['version']['number'])->toBe('8.19.19')
        ->and($client->info()->asArray()['version']['number'])->toBe('8.19.19')
        ->and($cluster->attempts)->toBe(['es.test', 'es.test', 'es.test']);
});

it('marks the host dead again when the attempt after the window fails too', function () {
    $cluster = new ReviewFlakyCluster(failures: 2);
    $clock = new ReviewClock;
    $client = reviewPoolClient($cluster, $clock, ['http://es.test:9200'], retries: 3);

    expect(fn () => $client->info())->toThrow(NoNodeAvailableException::class);

    $clock->now += 5.1;

    expect(fn () => $client->info())->toThrow(NoNodeAvailableException::class)
        ->and($cluster->attempts)->toHaveCount(2);

    $clock->now += 4.9;

    expect(fn () => $client->info())->toThrow(NoNodeAvailableException::class, 'No alive nodes')
        ->and($cluster->attempts)->toHaveCount(2);

    $clock->now += 0.2;

    expect($client->info()->asArray()['version']['number'])->toBe('8.19.19')
        ->and($cluster->attempts)->toHaveCount(3);
});

it('fails over to the alive host within the same request', function () {
    $cluster = new ReviewFlakyCluster(failures: 1);
    $clock = new ReviewClock;
    $client = reviewPoolClient($cluster, $clock, ['http://es-a.test:9200', 'http://es-b.test:9200'], retries: 1);

    expect($client->info()->asArray()['version']['number'])->toBe('8.19.19')
        ->and($cluster->attempts)->toHaveCount(2)
        ->and($cluster->attempts[1])->not->toBe($cluster->attempts[0]);

    $dead = $cluster->attempts[0];

    for ($i = 0; $i < 4; $i++) {
        $client->info();
    }

    expect(array_slice($cluster->attempts, 2))->each->not->toBe($dead);

    $clock->now += 5.1;
    $client->info();

    expect(end($cluster->attempts))->toBe($dead);
});

it('fails fast when every host is dead and none is past its window', function () {
    $cluster = new ReviewFlakyCluster(failures: 2);
    $clock = new ReviewClock;
    $client = reviewPoolClient($cluster, $clock, ['http://es-a.test:9200', 'http://es-b.test:9200'], retries: 3);

    expect(fn () => $client->info())->toThrow(NoNodeAvailableException::class)
        ->and($cluster->attempts)->toHaveCount(2)
        ->and(fn () => $client->info())->toThrow(NoNodeAvailableException::class, 'No alive nodes')
        ->and($cluster->attempts)->toHaveCount(2);

    $clock->now += 5.1;

    expect($client->info()->asArray()['version']['number'])->toBe('8.19.19')
        ->and($cluster->attempts)->toHaveCount(3);
});

it('keeps a dead host out until its window has passed on the pool clock', function () {
    $clock = new ReviewClock;

    $single = (new ResurrectingNodePool(5.0, Closure::fromCallable($clock)))->setHosts(['http://a:9200']);
    $node = $single->nextNode();
    $node->markAlive(false);

    $clock->now += 4.9;

    expect(fn () => $single->nextNode())->toThrow(NoNodeAvailableException::class, 'No alive nodes');

    $clock->now += 0.1;

    expect($single->nextNode())->toBe($node)
        ->and($node->isAlive())->toBeTrue();

    $pool = (new ResurrectingNodePool(60.0, Closure::fromCallable($clock)))->setHosts(['http://a:9200', 'http://b:9200']);
    $dead = $pool->nextNode();
    $dead->markAlive(false);

    $picked = array_map(fn () => $pool->nextNode(), range(1, 4));

    expect(array_filter($picked, fn (Node $n) => $n === $dead))->toBeEmpty();

    $immediate = (new ResurrectingNodePool(0.0, Closure::fromCallable($clock)))->setHosts(['http://a:9200']);
    $only = $immediate->nextNode();
    $only->markAlive(false);

    expect($immediate->nextNode())->toBe($only);
});

it('throws when no host is configured', function () {
    (new ResurrectingNodePool)->setHosts([])->nextNode();
})->throws(NoNodeAvailableException::class, 'No Elasticsearch hosts');

it('keeps package defaults for null config values', function () {
    config(['pulse.storage.elasticsearch' => ['ilm' => null, 'bulk_chunk' => null, 'prefix' => 'custom']]);

    (new ElasticsearchServiceProvider(app()))->boot();

    expect(config('pulse.storage.elasticsearch.ilm'))->toBeTrue()
        ->and(config('pulse.storage.elasticsearch.bulk_chunk'))->toBe(1000)
        ->and(config('pulse.storage.elasticsearch.prefix'))->toBe('custom');
});
