# Fledge Pulse Elasticsearch

Elasticsearch storage driver for [Fledge Pulse](https://github.com/webpatser/fledge-pulse). Entries go to a data stream with ILM retention, values to a regular index. Reads and bulk writes run on Fledge's fiber-aware HTTP transport, so parallel dashboard cards query concurrently.

## Requirements

- PHP 8.5
- `webpatser/fledge-pulse`
- `webpatser/fledge-fiber` ^13.36.0.3 (`Fledge\Fiber\Search\ElasticClientFactory`)
- Elasticsearch 8.17 or newer (`pdphilip/elasticsearch` is not required)

## Installation

```bash
composer require webpatser/fledge-pulse-elasticsearch
php artisan pulse:elasticsearch:install
```

Select the driver:

```dotenv
PULSE_STORAGE_DRIVER=elasticsearch
```

The install command is idempotent: run it again after an upgrade or a config change (for example from a deploy step or a migration) to update the ILM policy, the index template and the mappings.

## Configuration

Options live under `pulse.storage.elasticsearch` in `config/pulse.php`. Only list the keys you change; the defaults are:

```php
'storage' => [
    'driver' => env('PULSE_STORAGE_DRIVER', 'database'),

    'elasticsearch' => [
        'connection' => env('PULSE_ES_CONNECTION', 'elasticsearch'), // database.connections.<name>
        'prefix' => env('PULSE_ES_PREFIX', 'pulse'),
        'shards' => (int) env('PULSE_ES_SHARDS', 1),
        'replicas' => (int) env('PULSE_ES_REPLICAS', 0),
        'ilm' => (bool) env('PULSE_ES_ILM', true),
        'bulk_chunk' => (int) env('PULSE_ES_BULK_CHUNK', 1000),
        'bulk_retries' => (int) env('PULSE_ES_BULK_RETRIES', 3),
        'bulk_backoff_ms' => (int) env('PULSE_ES_BULK_BACKOFF_MS', 500),
        'deduplicate_entries' => (bool) env('PULSE_ES_DEDUPLICATE_ENTRIES', true),
        'terms_shard_size' => env('PULSE_ES_TERMS_SHARD_SIZE'), // null: max(10000, limit * 50)
        'timeout' => (float) env('PULSE_ES_TIMEOUT', 10),
        'connect_timeout' => (float) env('PULSE_ES_CONNECT_TIMEOUT', 2),
        'retries' => env('PULSE_ES_RETRIES'), // null: one per extra host
    ],
],
```

A key set to `null` in your config (for example `env('PULSE_ES_ILM')` with the variable unset) keeps the package default.

The connection block uses the familiar `database.connections.elasticsearch` shape: `hosts`, `username`/`password`, `api_key`/`api_id`, `auth_type` + `cloud_id`, `ssl_cert` and `options.ssl_verification`.

### Timeouts and failed hosts

Every request has a `timeout` (whole request and inactivity, default 10 seconds) and a `connect_timeout` (TCP connect and TLS handshake, default 2 seconds), so an unreachable cluster cannot hold a PHP-FPM worker, a queue worker or `pulse:work` for long.

The client uses its own node pool: a host that failed (a timeout, a refused connection) is skipped for 5 seconds and then tried again, and when every host has failed the one that failed first is retried. The `elasticsearch/elasticsearch` default (also what `Fledge\Fiber\Search\ElasticClientFactory::builder()` gives you) marks a host dead on the first network error and never tries it again, so a single-host process would refuse every request with "No alive nodes" until it restarts. `retries` is the number of times a request that failed on the network is resent to another host (default: one per extra host, none for a single host).

### Writes

Entries and values are sent in separate `_bulk` requests of at most `bulk_chunk` documents. Entry requests set `require_data_stream`, so writing before `pulse:elasticsearch:install` fails with a message that says to run it, instead of creating a plain `{prefix}-entries` index with dynamic mappings. (A values-only batch before the install still auto-creates `{prefix}-values`; delete it and run the install command.)

Documents rejected with 429 (`es_rejected_execution_exception`, the cluster's write queue is full) are resent up to `bulk_retries` times, waiting `bulk_backoff_ms` and then twice as long each time, before `store()` throws. The wait suspends only the current fiber.

With `deduplicate_entries` every entry gets a deterministic `_id` (a hash chain over the batch: its own content and every entry before it) and is written with `create`. When `store()` throws halfway, the Redis ingest keeps the batch and stores it again later; the entries that already made it in come back as version conflicts and are skipped, so nothing counts twice. Residual risks:

- ids are unique per backing index, so a retried batch that lands in a newer backing index (after a daily rollover in between) is not deduplicated;
- two separately stored batches that start with an identical run of entries in the same second (two identical requests, each digested on its own) share those ids, and the second run is dropped. Set `deduplicate_entries` to `false` if that matters more to you than retries; Elasticsearch then generates the ids.

## Resources

| Resource | Name | Notes |
|---|---|---|
| Data stream + index template | `{prefix}-entries` | `@timestamp`, `type`, `key`, `value`, `aggregations`, `only_buckets`; `shards` primaries per backing index |
| Index | `{prefix}-values` | latest value per type and key |
| ILM policy | `{prefix}-policy` | rolls over daily, deletes after `pulse.storage.trim.keep` (capped at 7 days, like upstream) |

With `ilm` disabled (clusters without ILM, such as serverless) no policy is installed and `trim()` deletes old entries by query instead. Entry deletions (`trim()` without ILM, `purge()`) run as background `delete_by_query` tasks with `conflicts=proceed`, because a week of raw entries can take far longer than the request timeout; values are deleted with one request. `trim()` does not wait for its task; `purge()` polls the tasks API until its task is done (up to 10 minutes), so `pulse:clear` is complete on return. `ElasticsearchStorage::awaitDeletes()` waits for pending tasks and refreshes the entries.

Keys longer than 8191 characters are stored but not indexed (`ignore_above`), so they are invisible to every read: such entries are missing from the dashboard.

## Reads and their cost

Upstream pre-aggregates entries into buckets on write; this driver keeps raw entries and aggregates at read time with upstream's window math, so results match `DatabaseStorage`. The price is that a 7-day card aggregates a week of raw entries on every read, which is heavier than upstream's bucket reads. On busy applications sample the high-volume recorders, for example the cache interactions recorder's `sample_rate` in `config/pulse.php`.

Ordered key lists (`aggregate()`, `aggregateTypes()`) are exact, even though the entries of a 7-day window live in about eight backing indices, one shard each:

1. Elasticsearch picks candidate keys with an ordered `terms` aggregation whose `shard_size` is `terms_shard_size` (default `max(10000, limit * 50)`). With no more distinct keys per backing index than that, the candidates are exact.
2. The aggregates are recomputed for the candidates only, exactly, and the final order and limit come from those values.

Ordering by `avg` (and by `min`, see below) cannot be expressed as one Elasticsearch metric, so those reads page through every key in the window with a composite aggregation and order in PHP. That costs more with many distinct keys; the composite page size shrinks for averages of several types so one page stays under `search.max_buckets`.

Nulls order as upstream does on MySQL: last on `desc`, first on `asc`, ties by key ascending. Ordering by `min` is done in PHP because Elasticsearch ranks a key without values as +infinity for `min`, the opposite end. When Elasticsearch picks candidates for `count` or `sum`, a key without values ranks as 0, so with more than `limit * 2` keys tied at 0 or null the cut can differ from MySQL's.

## Testing

The tests run against a real Elasticsearch at `ES_HOST` (default `http://localhost:9200`, optional `ES_USERNAME` / `ES_PASSWORD`) and skip when it does not answer. Each test uses its own resource prefix and deletes its resources afterwards.

```bash
composer test
```

## License

MIT
