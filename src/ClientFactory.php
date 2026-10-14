<?php

namespace Fledge\Pulse\Elasticsearch;

use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\ClientBuilder;
use Fledge\Fiber\Http\FiberHttpServiceProvider;
use Fledge\Fiber\Http\FledgeGuzzle;
use Fledge\Fiber\Search\ElasticClientFactory;
use Fledge\Pulse\Elasticsearch\Support\ResurrectingNodePool;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Container\Container;
use InvalidArgumentException;

/**
 * Builds the Elasticsearch client Pulse reads and writes through.
 *
 * The client comes from fledge-fiber's ElasticClientFactory, which needs only
 * elasticsearch/elasticsearch (no pdphilip/elasticsearch): its HTTP transport
 * is the Fledge Guzzle handler, so a request suspends only the fiber that
 * issued it and parallel dashboard cards query concurrently.
 *
 * Registered as a singleton, so one client (and one keep-alive pool) serves
 * every read and write of the process.
 */
class ClientFactory
{
    private ?Client $client = null;

    public function __construct(
        private readonly Container $container,
        private readonly Repository $config,
    ) {}

    /**
     * The shared client for the configured connection.
     */
    public function client(): Client
    {
        return $this->client ??= $this->make($this->connectionConfig());
    }

    /**
     * Build a new client from a `database.connections.*` block.
     *
     * @param  array<string, mixed>  $config
     */
    public function make(array $config): Client
    {
        $builder = $this->elasticClientFactory()->builder($config);

        // ElasticClientFactory leaves SSL verification at the client default
        // (on). pdphilip-shaped configs carry the flag under `options`.
        $verify = $config['options']['ssl_verification'] ?? $config['ssl_verification'] ?? null;

        if ($verify !== null && $verify !== '') {
            $builder->setSSLVerification(filter_var($verify, FILTER_VALIDATE_BOOLEAN));
        }

        // Replaces the options ElasticClientFactory set (only its handler),
        // so the same fiber-aware handler is passed again with the timeouts.
        $builder->setHttpClientOptions([
            'handler' => FledgeGuzzle::stack($this->connectionsPerHost()),
            ...$this->httpOptions(),
        ]);

        $hosts = ($config['auth_type'] ?? 'http') === 'cloud' ? 1 : max(1, count((array) ($config['hosts'] ?? [])));

        return $this->applyRetries($this->harden($builder)->build(), $hosts);
    }

    /**
     * Guzzle request options for every request: `timeout` (whole transfer
     * and inactivity) and `connect_timeout` (TCP connect and TLS handshake),
     * in seconds. 0 leaves the transport's own default.
     *
     * @return array{timeout: float, connect_timeout: float}
     */
    public function httpOptions(): array
    {
        return [
            'timeout' => max(0.0, (float) ($this->config->get('pulse.storage.elasticsearch.timeout') ?? 10)),
            'connect_timeout' => max(0.0, (float) ($this->config->get('pulse.storage.elasticsearch.connect_timeout') ?? 2)),
        ];
    }

    /**
     * Keep the client usable after a network error: a node pool that
     * revives dead nodes. The transport default (SimpleNodePool with
     * NoResurrect, which ElasticClientFactory::builder() does not override)
     * marks the only host dead on one timeout and then refuses every request
     * with "No alive nodes" until the process restarts.
     */
    public function harden(ClientBuilder $builder): ClientBuilder
    {
        $builder->setNodePool(new ResurrectingNodePool);

        return $builder;
    }

    /**
     * Apply `retries` to a built client; unset means one retry per extra
     * host (0 for a single host, so a failed request costs one timeout, not
     * two). ClientBuilder treats 0 as unset and falls back to one retry per
     * host, so the count is set on the transport directly.
     */
    public function applyRetries(Client $client, int $hosts = 1): Client
    {
        $retries = $this->config->get('pulse.storage.elasticsearch.retries');

        $client->getTransport()->setRetries(
            $retries === null || $retries === '' ? max(0, $hosts - 1) : max(0, (int) $retries)
        );

        return $client;
    }

    /**
     * The `database.connections.<name>` block named by `pulse.storage.elasticsearch.connection`.
     *
     * @return array<string, mixed>
     */
    public function connectionConfig(): array
    {
        $name = $this->config->get('pulse.storage.elasticsearch.connection') ?: 'elasticsearch';

        $config = $this->config->get("database.connections.{$name}");

        if (! is_array($config)) {
            throw new InvalidArgumentException("Elasticsearch connection [{$name}] is not configured in database.connections.");
        }

        return $config;
    }

    /**
     * fledge-fiber binds a singleton (with the `fledge-http.pool_per_host`
     * limit) only when pdphilip/elasticsearch is installed; otherwise build
     * one with the same limit.
     */
    protected function elasticClientFactory(): ElasticClientFactory
    {
        if ($this->container->bound(ElasticClientFactory::class)) {
            return $this->container->make(ElasticClientFactory::class);
        }

        return new ElasticClientFactory($this->connectionsPerHost());
    }

    /**
     * The `fledge-http.pool_per_host.elasticsearch` limit (default 8, null
     * for unlimited), as fledge-fiber resolves it.
     */
    protected function connectionsPerHost(): ?int
    {
        return FiberHttpServiceProvider::perHost($this->container, 'elasticsearch', ElasticClientFactory::DEFAULT_CONNECTIONS_PER_HOST);
    }
}
