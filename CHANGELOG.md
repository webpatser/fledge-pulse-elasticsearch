# Changelog

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0] - 2026-11-18

### Changed

- Behaviour change: `deduplicate_entries` now defaults to `null` (it was `true`). Entries get deterministic ids only when `pulse.ingest.driver` is `redis`, the ingest that stores a failed batch again. With the storage ingest, Elasticsearch generates the ids, so identical requests stored in the same second no longer collapse into one. `true` or `false` still force it either way.
- Trade-off: the Elasticsearch transport resends a `_bulk` request that timed out when several hosts are configured or the client's `retries` option is set, and without ids such a resend can count entries twice. If your setup retries, or a custom ingest driver redelivers batches, set `PULSE_ES_DEDUPLICATE_ENTRIES=true`.
- `pulse:elasticsearch:install` warns when deduplication is forced on with the storage ingest.

### Upgrade

- No reindex or migration is needed; mappings and the index template are unchanged, and stored entries stay readable. Entries that were collapsed before the upgrade stay collapsed.
- If you published the config, `pulse.storage.elasticsearch.deduplicate_entries` still holds the old default `(bool) env('PULSE_ES_DEDUPLICATE_ENTRIES', true)`, which forces deduplication on for every ingest. Change it to `env('PULSE_ES_DEDUPLICATE_ENTRIES')` to get the new default, or set `PULSE_ES_DEDUPLICATE_ENTRIES=true` to keep the old behaviour.

## [0.1.2] - 2026-10-24

### Security

- Composer `conflict` with `livewire/livewire` below `4.4.8` (GHSA-mhf7-xp6r-6qrc).

### Removed

- The Dependabot config.

## [0.1.1] - 2026-10-22

### Changed

- The `fledge-fiber` floor is raised to `^13.38`.

### Added

- Dependabot for Composer and GitHub Actions (removed again in 0.1.2).

## [0.1.0] - 2026-10-14

### Added

- Initial release of fledge-pulse-elasticsearch, an Elasticsearch storage driver for fledge-pulse.
