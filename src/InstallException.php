<?php

namespace Fledge\Pulse\Elasticsearch;

use RuntimeException;

/**
 * The cluster is in a state the installer cannot fix on its own.
 */
class InstallException extends RuntimeException {}
