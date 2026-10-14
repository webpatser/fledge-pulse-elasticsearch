<?php

namespace Fledge\Pulse\Elasticsearch;

use Elastic\Elasticsearch\Client;
use Fledge\Pulse\Elasticsearch\Commands\InstallCommand;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\ServiceProvider;
use Laravel\Pulse\Contracts\Storage;
use Laravel\Pulse\Fiber\Storage\StorageManager;

/**
 * Registers the `elasticsearch` Pulse storage driver.
 *
 * Select it with `pulse.storage.driver` (`PULSE_STORAGE_DRIVER=elasticsearch`).
 * Nothing here connects to Elasticsearch while the application boots: the
 * client is built when the storage is first resolved.
 */
class ElasticsearchServiceProvider extends ServiceProvider
{
    /**
     * Register any application services.
     */
    public function register(): void
    {
        $this->app->singleton(ClientFactory::class);

        // Contextual, so an application's own Client binding stays untouched.
        $this->app->when(ElasticsearchStorage::class)
            ->needs(Client::class)
            ->give(fn (Application $app): Client => $app->make(ClientFactory::class)->client());

        // StorageManager is a singleton; extending it on (or right after,
        // when it already resolved) its first resolution keeps the driver
        // registered for the life of the application.
        $this->callAfterResolving(StorageManager::class, function (StorageManager $manager): void {
            $manager->extend('elasticsearch', fn (Application $app): Storage => $app->make(ElasticsearchStorage::class));
        });
    }

    /**
     * Bootstrap any application services.
     */
    public function boot(): void
    {
        $this->mergeStorageConfig();

        if ($this->app->runningInConsole()) {
            $this->commands([
                InstallCommand::class,
            ]);
        }
    }

    /**
     * Fill `pulse.storage.elasticsearch` with the package defaults.
     *
     * Done in boot() rather than with mergeConfigFrom() in register(): the
     * key is nested inside Pulse's own config, which Pulse merges in its
     * register(), and the application's values must win over the defaults.
     *
     * A null application value (typically `env('PULSE_ES_ILM')` with the
     * variable unset) does not override the default: otherwise an unset
     * variable would turn ILM off or drop the bulk chunk size.
     */
    protected function mergeStorageConfig(): void
    {
        $config = $this->app->make(Repository::class);

        $app = array_filter(
            (array) $config->get('pulse.storage.elasticsearch', []),
            fn (mixed $value): bool => $value !== null,
        );

        $config->set('pulse.storage.elasticsearch', [
            ...require __DIR__.'/../config/elasticsearch.php',
            ...$app,
        ]);
    }
}
