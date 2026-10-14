<?php

namespace Fledge\Pulse\Elasticsearch\Commands;

use Elastic\Elasticsearch\Exception\ElasticsearchException;
use Elastic\Transport\Exception\TransportException;
use Fledge\Pulse\Elasticsearch\Installer;
use Fledge\Pulse\Elasticsearch\InstallException;
use Illuminate\Console\Command;
use Symfony\Component\Console\Attribute\AsCommand;

#[AsCommand(name: 'pulse:elasticsearch:install')]
class InstallCommand extends Command
{
    /**
     * The command's signature.
     *
     * @var string
     */
    protected $signature = 'pulse:elasticsearch:install';

    /**
     * The command's description.
     *
     * @var string
     */
    protected $description = 'Create or update the ILM policy, index template, data stream and values index of the Pulse Elasticsearch storage';

    /**
     * Handle the command.
     */
    public function handle(Installer $installer): int
    {
        try {
            $steps = $installer->install();
        } catch (ElasticsearchException|TransportException|InstallException $e) {
            $this->components->error('Installing the Pulse Elasticsearch resources failed: '.$e->getMessage());

            return self::FAILURE;
        }

        foreach ($steps as $resource => $action) {
            $this->components->twoColumnDetail($resource, $action);
        }

        return self::SUCCESS;
    }
}
