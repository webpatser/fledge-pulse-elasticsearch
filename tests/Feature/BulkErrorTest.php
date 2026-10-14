<?php

use Elastic\Elasticsearch\ClientBuilder;
use Fledge\Pulse\Elasticsearch\ElasticsearchStorage;
use Fledge\Pulse\Elasticsearch\Schema;
use Fledge\Pulse\Elasticsearch\Support\BulkException;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use Laravel\Pulse\Entry;
use Laravel\Pulse\Value;
use Psr\Http\Message\RequestInterface;

/*
| `_bulk` handling without an Elasticsearch: the client's Guzzle handler is a
| MockHandler (an external dependency) that replays canned responses and the
| history middleware records the requests the storage sent. The client
| refuses responses without the `X-Elastic-Product` header.
*/

/**
 * A storage on a client whose transport replays the given responses.
 *
 * @param  list<Response>  $responses
 * @param  array<int, array{request: RequestInterface}>|null  $history  filled with the transactions; an undefined caller variable arrives as null
 */
function bulkStorage(array $responses, mixed &$history = null, ?MockHandler &$mock = null): ElasticsearchStorage
{
    $history = [];
    $mock = new MockHandler($responses);

    $stack = HandlerStack::create($mock);
    $stack->push(Middleware::history($history));

    $client = ClientBuilder::create()
        ->setHosts(['http://es.test:9200'])
        ->setHttpClient(new GuzzleClient(['handler' => $stack]))
        ->build();

    return new ElasticsearchStorage($client, app('config'));
}

/**
 * @param  array<string, mixed>  $body
 */
function bulkResponse(array $body): Response
{
    return new Response(
        200,
        ['X-Elastic-Product' => 'Elasticsearch', 'Content-Type' => 'application/json'],
        json_encode($body, JSON_THROW_ON_ERROR),
    );
}

function bulkOk(int $items = 0): Response
{
    return bulkResponse([
        'took' => 1,
        'errors' => false,
        'items' => array_fill(0, $items, ['create' => ['status' => 201]]),
    ]);
}

/**
 * @return array{string: array<string, mixed>}
 */
function bulkItem(string $operation, int $status, ?string $type = null, ?string $reason = null): array
{
    $item = ['_index' => 'pulse-test', 'status' => $status];

    if ($type !== null) {
        $item['error'] = ['type' => $type, 'reason' => $reason];
    }

    return [$operation => $item];
}

function bulkEntry(string $key): Entry
{
    return (new Entry(946_731_600, 'hit', $key))->count();
}

function bulkValue(string $key): Value
{
    return new Value(946_731_600, 'system', $key, 'v');
}

it('throws the first reason when a document is rejected', function () {
    $storage = bulkStorage([
        bulkResponse([
            'took' => 1,
            'errors' => true,
            'items' => [
                bulkItem('create', 201),
                bulkItem('create', 400, 'mapper_parsing_exception', 'failed to parse field [value] of type [long]'),
                bulkItem('create', 400, 'strict_dynamic_mapping_exception', 'second failure'),
            ],
        ]),
    ]);

    try {
        $storage->store(collect([bulkEntry('a'), bulkEntry('b'), bulkEntry('c')]));

        $this->fail('Expected a BulkException.');
    } catch (BulkException $e) {
        expect($e->getMessage())
            ->toContain('mapper_parsing_exception')
            ->toContain('failed to parse field [value] of type [long]')
            ->toContain('[create]')
            ->toContain('pulse-test')
            ->not->toContain('second failure');
    }
});

it('ignores version conflicts on values', function () {
    $storage = bulkStorage([
        bulkResponse([
            'took' => 1,
            'errors' => true,
            'items' => [
                bulkItem('index', 409, 'version_conflict_engine_exception', 'current version [10] is higher'),
                bulkItem('index', 200),
                bulkItem('index', 409, 'version_conflict_engine_exception', 'current version [10] is higher'),
            ],
        ]),
    ], $history, $mock);

    $storage->store(collect([bulkValue('a'), bulkValue('b'), bulkValue('c')]));

    expect($history)->toHaveCount(1)
        ->and($mock->count())->toBe(0);
});

it('throws when a version conflict is mixed with a real error', function () {
    $storage = bulkStorage([
        bulkResponse([
            'took' => 1,
            'errors' => true,
            'items' => [
                bulkItem('index', 409, 'version_conflict_engine_exception', 'current version [10] is higher'),
                bulkItem('index', 403, 'cluster_block_exception', 'index [pulse-values] blocked by: [FORBIDDEN/8/index write (api)]'),
            ],
        ]),
    ]);

    expect(fn () => $storage->store(collect([bulkValue('a'), bulkValue('b')])))
        ->toThrow(BulkException::class, 'cluster_block_exception');
});

it('ignores a version conflict on an entry stored before', function () {
    // Entries carry a deterministic _id, so a retried batch conflicts on the
    // entries that already made it in.
    $storage = bulkStorage([
        bulkResponse([
            'took' => 1,
            'errors' => true,
            'items' => [
                bulkItem('create', 409, 'version_conflict_engine_exception', 'document already exists'),
                bulkItem('create', 201),
            ],
        ]),
    ], $history, $mock);

    $storage->store(collect([bulkEntry('a'), bulkEntry('b')]));

    expect($history)->toHaveCount(1)
        ->and($mock->count())->toBe(0);
});

it('does not send the remaining chunks after a rejected one', function () {
    config(['pulse.storage.elasticsearch.bulk_chunk' => 2]);

    $storage = bulkStorage([
        bulkResponse([
            'took' => 1,
            'errors' => true,
            'items' => [bulkItem('create', 400, 'mapper_parsing_exception', 'boom'), bulkItem('create', 201)],
        ]),
        bulkOk(2),
    ], $history, $mock);

    expect(fn () => $storage->store(collect([bulkEntry('a'), bulkEntry('b'), bulkEntry('c'), bulkEntry('d')])))
        ->toThrow(BulkException::class, 'boom');

    expect($history)->toHaveCount(1)
        ->and($mock->count())->toBe(1);
});

it('sends one bulk request per bulk_chunk operations', function (int $chunk, int $entries, int $values, array $perRequest) {
    config(['pulse.storage.elasticsearch.bulk_chunk' => $chunk]);

    $storage = bulkStorage(array_map(fn () => bulkOk(), $perRequest), $history, $mock);

    $items = [];

    foreach (range(1, $entries) as $i) {
        $items[] = bulkEntry("entry-{$i}");
    }

    foreach (range(1, $values) as $i) {
        $items[] = bulkValue("value-{$i}");
    }

    $storage->store(collect($items));

    $schema = app(Schema::class);

    expect($history)->toHaveCount(count($perRequest))
        ->and($mock->count())->toBe(0);

    foreach ($history as $i => $transaction) {
        $request = $transaction['request'];

        // Every operation is an action line followed by its document line.
        $lines = array_values(array_filter(explode("\n", (string) $request->getBody())));

        expect($request->getUri()->getPath())->toBe('/_bulk')
            ->and($request->getMethod())->toBe('POST')
            ->and(count($lines))->toBe($perRequest[$i] * 2);
    }

    // Entry requests require the data stream; value requests do not.
    $entryRequests = (int) ceil($entries / $chunk);

    foreach ($history as $i => $transaction) {
        parse_str($transaction['request']->getUri()->getQuery(), $query);

        expect($query['require_data_stream'] ?? null)->toBe($i < $entryRequests ? 'true' : null);
    }

    // Entries are sent first, then values, in order across the chunks.
    $actions = collect($history)
        ->flatMap(fn (array $transaction) => explode("\n", (string) $transaction['request']->getBody()))
        ->filter()
        ->values()
        ->filter(fn (string $line, int $index) => $index % 2 === 0)
        ->map(fn (string $line) => json_decode($line, true))
        ->values();

    expect($actions)->toHaveCount($entries + $values)
        ->and($actions->take($entries)->every(fn (array $action) => ($action['create']['_index'] ?? null) === $schema->entries()))->toBeTrue()
        ->and($actions->take($entries)->every(fn (array $action) => strlen($action['create']['_id'] ?? '') === 40))->toBeTrue()
        ->and($actions->take($entries)->pluck('create._id')->unique())->toHaveCount($entries)
        ->and($actions->skip($entries)->every(fn (array $action) => ($action['index']['_index'] ?? null) === $schema->values()))->toBeTrue();
})->with([
    // Entries and values never share a request; each is chunked on its own.
    'five operations in chunks of two' => [2, 3, 2, [2, 1, 2]],
    'two entries and two values in chunks of four' => [4, 2, 2, [2, 2]],
    'one operation per request' => [1, 2, 1, [1, 1, 1]],
    'everything fits one chunk' => [1000, 5, 5, [5, 5]],
]);

it('sends nothing for an empty batch', function () {
    $storage = bulkStorage([], $history, $mock);

    $storage->store(collect());

    expect($history)->toBeEmpty();
});
