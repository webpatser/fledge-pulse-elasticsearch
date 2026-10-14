<?php

namespace Fledge\Pulse\Elasticsearch\Support;

use RuntimeException;
use Throwable;

/**
 * Elasticsearch rejected at least one document of a `_bulk` write.
 */
class BulkException extends RuntimeException
{
    /**
     * @param  array<string, mixed>  $error  the item's `error` object
     */
    public static function fromItem(string $operation, string $index, array $error): self
    {
        $type = $error['type'] ?? 'unknown_error';
        $reason = $error['reason'] ?? 'no reason given';

        return new self("Elasticsearch rejected a Pulse [{$operation}] into [{$index}]: {$type}: {$reason}");
    }

    /**
     * Documents were still rejected with 429 after every retry.
     *
     * @param  array<string, mixed>  $error
     */
    public static function stillRejected(int $retries, string $operation, string $index, array $error): self
    {
        $item = self::fromItem($operation, $index, $error);

        return new self("{$item->getMessage()} (still rejected after {$retries} retries)");
    }

    /**
     * The entries data stream is missing (or the name is a regular index).
     *
     * @param  array<string, mixed>  $error
     */
    public static function missingDataStream(string $stream, array $error = [], ?Throwable $previous = null): self
    {
        $type = $error['type'] ?? 'index_not_found_exception';
        $reason = $error['reason'] ?? 'no such data stream';

        return new self(
            "Pulse entries need the [{$stream}] data stream, which does not exist (or is not a data stream): {$type}: {$reason}. Run `php artisan pulse:elasticsearch:install` first.",
            0,
            $previous,
        );
    }
}
