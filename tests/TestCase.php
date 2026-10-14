<?php

namespace Fledge\Pulse\Elasticsearch\Tests;

use Fledge\Pulse\Elasticsearch\ElasticsearchServiceProvider;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Support\Str;
use Laravel\Pulse\Fiber\FledgePulseServiceProvider;
use Laravel\Pulse\PulseServiceProvider;
use Livewire\LivewireServiceProvider;
use Orchestra\Testbench\TestCase as OrchestraTestCase;

abstract class TestCase extends OrchestraTestCase
{
    /**
     * A unique resource prefix per test, so runs never touch real Pulse data
     * and parallel runs never collide.
     */
    protected string $prefix;

    protected function setUp(): void
    {
        $this->prefix = 'fpes-test-'.strtolower(Str::random(10));

        parent::setUp();
    }

    protected function getPackageProviders($app): array
    {
        return [
            LivewireServiceProvider::class,
            PulseServiceProvider::class,
            FledgePulseServiceProvider::class,
            ElasticsearchServiceProvider::class,
        ];
    }

    protected function defineEnvironment($app): void
    {
        tap($app['config'], function (Repository $config): void {
            $config->set('app.timezone', 'UTC');

            $config->set('database.connections.elasticsearch', elasticsearchConnectionConfig());

            $config->set('pulse.storage.elasticsearch.prefix', $this->prefix);
        });
    }
}
