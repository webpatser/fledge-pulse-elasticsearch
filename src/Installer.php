<?php

namespace Fledge\Pulse\Elasticsearch;

use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\Exception\ClientResponseException;

/**
 * Creates or updates every Elasticsearch resource the storage needs.
 *
 * Idempotent: the ILM policy and the index template are upserts, the data
 * stream and the values index are created when missing and get their
 * mappings put (and, with ILM, the policy setting) when they exist. A
 * concurrent install that creates a resource first is not an error.
 *
 * Shard and replica counts of an existing data stream change with its next
 * backing index (the template applies them at rollover); the values index
 * keeps the counts it was created with.
 */
class Installer
{
    public function __construct(
        protected ClientFactory $clients,
        protected Schema $schema,
    ) {}

    /**
     * Install the resources.
     *
     * @return array<string, string> Resource description => what happened.
     */
    public function install(): array
    {
        $client = $this->clients->client();
        $steps = [];

        $policy = $this->schema->policy();

        if ($this->schema->ilm()) {
            $client->ilm()->putLifecycle(['policy' => $policy, 'body' => $this->schema->policyBody()]);
            $steps["ILM policy [{$policy}]"] = 'put';
        } else {
            $steps["ILM policy [{$policy}]"] = 'skipped (ilm disabled)';
        }

        $entries = $this->schema->entries();

        $client->indices()->putIndexTemplate(['name' => $entries, 'body' => $this->schema->entriesTemplateBody()]);
        $steps["Index template [{$entries}]"] = 'put';

        $steps["Data stream [{$entries}]"] = $this->installEntries($client, $entries);

        $values = $this->schema->values();

        $steps["Index [{$values}]"] = $this->installValues($client, $values);

        return $steps;
    }

    protected function installEntries(Client $client, string $entries): string
    {
        if (! $this->dataStreamExists($client, $entries)) {
            if ($this->exists($client, $entries)) {
                throw new InstallException("[{$entries}] exists but is not a data stream; delete or rename it, or choose another pulse.storage.elasticsearch.prefix.");
            }

            $created = $this->createIgnoringExisting(fn () => $client->indices()->createDataStream(['name' => $entries]));

            if ($created) {
                return 'created';
            }
        }

        $client->indices()->putMapping(['index' => $entries, 'body' => $this->schema->entriesMappings()]);

        if ($this->schema->ilm()) {
            $client->indices()->putSettings([
                'index' => $entries,
                'body' => ['index.lifecycle.name' => $this->schema->policy()],
            ]);
        }

        return 'updated';
    }

    protected function installValues(Client $client, string $values): string
    {
        if (! $this->exists($client, $values)) {
            $created = $this->createIgnoringExisting(fn () => $client->indices()->create([
                'index' => $values,
                'body' => $this->schema->valuesIndexBody(),
            ]));

            if ($created) {
                return 'created';
            }
        }

        $client->indices()->putMapping(['index' => $values, 'body' => $this->schema->valuesMappings()]);

        return 'updated';
    }

    /**
     * Whether an index, alias or data stream exists. HEAD requests do not
     * throw on 404.
     */
    protected function exists(Client $client, string $name): bool
    {
        return $client->indices()->exists(['index' => $name])->asBool();
    }

    /**
     * Whether a data stream of exactly this name exists. Elasticsearch
     * answers 200 with an empty list when the name is a regular index, so
     * the list itself has to name the stream.
     */
    protected function dataStreamExists(Client $client, string $name): bool
    {
        try {
            $streams = $client->indices()->getDataStream(['name' => $name])->asArray()['data_streams'] ?? [];

            return array_any($streams, fn (array $stream): bool => ($stream['name'] ?? null) === $name);
        } catch (ClientResponseException $e) {
            if ($e->getCode() === 404) {
                return false;
            }

            throw $e;
        }
    }

    /**
     * Run a create call; false when another process created it first.
     */
    protected function createIgnoringExisting(callable $create): bool
    {
        try {
            $create();

            return true;
        } catch (ClientResponseException $e) {
            if ($e->getCode() === 400 && str_contains($e->getMessage(), 'resource_already_exists_exception')) {
                return false;
            }

            throw $e;
        }
    }
}
