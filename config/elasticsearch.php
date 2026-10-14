<?php

/*
|--------------------------------------------------------------------------
| Pulse Elasticsearch Storage
|--------------------------------------------------------------------------
|
| Defaults for `pulse.storage.elasticsearch`. The service provider merges
| them under any values the application sets in its own config/pulse.php,
| so an application only lists the keys it wants to change:
|
|     'storage' => [
|         'driver' => env('PULSE_STORAGE_DRIVER', 'elasticsearch'),
|         'elasticsearch' => [
|             'prefix' => 'myapp-pulse',
|         ],
|     ],
|
*/

return [

    // The `database.connections.<name>` block holding hosts, credentials
    // and SSL settings (the shape pdphilip/elasticsearch uses; that package
    // is not required).
    'connection' => env('PULSE_ES_CONNECTION', 'elasticsearch'),

    // Resources are named `{prefix}-entries` (data stream and index
    // template), `{prefix}-values` (index) and `{prefix}-policy` (ILM).
    'prefix' => env('PULSE_ES_PREFIX', 'pulse'),

    // Primary shards per backing index. Reads stay exact with any count:
    // daily rollover spreads a 7 day window over ~8 backing indices anyway,
    // so ordered key lists pick candidates with `terms_shard_size` and then
    // recompute the chosen keys exactly.
    'shards' => (int) env('PULSE_ES_SHARDS', 1),

    'replicas' => (int) env('PULSE_ES_REPLICAS', 0),

    // Retention through an ILM policy (daily rollover, delete after
    // `pulse.storage.trim.keep`). Disable on clusters without ILM
    // (serverless); trim() then deletes by query instead.
    'ilm' => (bool) env('PULSE_ES_ILM', true),

    // Maximum number of documents per `_bulk` request.
    'bulk_chunk' => (int) env('PULSE_ES_BULK_CHUNK', 1000),

    // Documents rejected with 429 (es_rejected_execution_exception, a full
    // write queue) are resent up to this many times, waiting
    // `bulk_backoff_ms` and then twice as long each time, before store()
    // throws.
    'bulk_retries' => (int) env('PULSE_ES_BULK_RETRIES', 3),

    'bulk_backoff_ms' => (int) env('PULSE_ES_BULK_BACKOFF_MS', 500),

    // Give every entry a deterministic `_id` (its content plus its position
    // in the batch), so a batch stored again after a failed store() (the
    // Redis ingest keeps it and retries) does not count twice. Trade-off:
    // two separately stored batches whose leading entries are identical,
    // in the same second, collapse into one. Disable to let Elasticsearch
    // generate ids (no deduplication, no collapsing).
    'deduplicate_entries' => (bool) env('PULSE_ES_DEDUPLICATE_ENTRIES', true),

    // Candidate keys per shard for ordered key lists (aggregate(),
    // aggregateTypes()). null means max(10000, limit * 50). The chosen keys
    // are recomputed exactly; this only bounds how far down each backing
    // index looks for them.
    'terms_shard_size' => env('PULSE_ES_TERMS_SHARD_SIZE'),

    // HTTP timeouts in seconds: `timeout` for the whole request (and
    // inactivity), `connect_timeout` for TCP connect and TLS handshake.
    'timeout' => (float) env('PULSE_ES_TIMEOUT', 10),

    'connect_timeout' => (float) env('PULSE_ES_CONNECT_TIMEOUT', 2),

    // Retries of a request that failed on the network; null means one per
    // extra host (none for a single host). A failed host is retried by the
    // next request either way.
    'retries' => env('PULSE_ES_RETRIES'),

];
