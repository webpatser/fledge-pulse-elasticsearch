<?php

namespace Fledge\Pulse\Elasticsearch\Query;

/**
 * Small builders for the query DSL fragments the storage repeats.
 */
final class Dsl
{
    /**
     * @return array<string, mixed>
     */
    public static function term(string $field, string|int|bool $value): array
    {
        return ['term' => [$field => $value]];
    }

    /**
     * @param  list<string>  $values
     * @return array<string, mixed>
     */
    public static function terms(string $field, array $values): array
    {
        return ['terms' => [$field => array_values($values)]];
    }

    /**
     * Entries with `@timestamp` at or after the given epoch second.
     *
     * @return array<string, mixed>
     */
    public static function since(int $timestamp, string $field = '@timestamp'): array
    {
        return ['range' => [$field => ['gte' => $timestamp, 'format' => 'epoch_second']]];
    }

    /**
     * Entries with `@timestamp` strictly before the given epoch second.
     *
     * @return array<string, mixed>
     */
    public static function before(int $timestamp, string $field = '@timestamp'): array
    {
        return ['range' => [$field => ['lt' => $timestamp, 'format' => 'epoch_second']]];
    }

    /**
     * Documents with the field at or before the given epoch second.
     *
     * @return array<string, mixed>
     */
    public static function until(int $timestamp, string $field = '@timestamp'): array
    {
        return ['range' => [$field => ['lte' => $timestamp, 'format' => 'epoch_second']]];
    }

    /**
     * The onlyBuckets() marker; raw entries are those without it.
     *
     * @return array<string, mixed>
     */
    public static function onlyBuckets(): array
    {
        return self::term('only_buckets', true);
    }

    /**
     * Raw entries: everything not stored with onlyBuckets(). Written as a
     * must_not so documents without the field count as raw.
     *
     * @return array<string, mixed>
     */
    public static function raw(): array
    {
        return self::bool(mustNot: [self::onlyBuckets()]);
    }

    /**
     * @param  list<array<string, mixed>>  $filter
     * @param  list<array<string, mixed>>  $should  at least one must match
     * @param  list<array<string, mixed>>  $mustNot
     * @return array<string, mixed>
     */
    public static function bool(array $filter = [], array $should = [], array $mustNot = []): array
    {
        $bool = array_filter([
            'filter' => $filter,
            'should' => $should,
            'must_not' => $mustNot,
        ]);

        if ($should !== []) {
            $bool['minimum_should_match'] = 1;
        }

        return ['bool' => $bool === [] ? (object) [] : $bool];
    }

    /**
     * @return array<string, mixed>
     */
    public static function matchAll(): array
    {
        return ['match_all' => (object) []];
    }

    /**
     * Value statistics (count of non-null values, min, max, sum, avg).
     *
     * @return array<string, mixed>
     */
    public static function stats(): array
    {
        return ['stats' => ['field' => 'value']];
    }
}
