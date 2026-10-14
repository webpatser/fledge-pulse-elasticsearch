<?php

namespace Fledge\Pulse\Elasticsearch\Support;

use Closure;
use Elastic\Transport\Exception\NoNodeAvailableException;
use Elastic\Transport\NodePool\Node;
use Elastic\Transport\NodePool\NodePoolInterface;

/**
 * A node pool that revives its hosts, but never makes a request wait for a dead one.
 *
 * elastic-transport's default pool (SimpleNodePool with NoResurrect) marks a
 * node dead on the first network error (a timeout, a refused connection) and
 * never revives it, so a long-running process with one host refuses every
 * later request with "No alive nodes" until it restarts.
 *
 * This pool hands out its nodes round-robin. A dead node is skipped until its
 * `resurrectAfter` window (seconds, counted from the moment it was marked dead)
 * has passed; then it is tried again, and a failed attempt marks it dead anew.
 * When every node is dead and none is past its window the pool throws
 * NoNodeAvailableException instead of handing out a dead host. That keeps a
 * failing single-host setup cheap: the first failure costs one timeout and
 * marks the host dead, the transport's own retries inside the same request fail
 * fast (no second timeout), and so does every request during the next window.
 * After the window the host is tried again.
 */
final class ResurrectingNodePool implements NodePoolInterface
{
    public const float DEFAULT_RESURRECT_AFTER = 5.0;

    /**
     * Nodes that remember when they were marked dead (`deadAt`, pool clock).
     *
     * @var list<Node&object{deadAt: float|null}>
     */
    private array $nodes = [];

    private int $cursor = 0;

    private readonly Closure $clock;

    /**
     * @param  float  $resurrectAfter  Seconds a dead node is skipped before it is tried again.
     * @param  Closure|null  $clock  Returns the current time in seconds as a float; defaults to microtime(true).
     */
    public function __construct(private readonly float $resurrectAfter = self::DEFAULT_RESURRECT_AFTER, ?Closure $clock = null)
    {
        $this->clock = $clock ?? static fn (): float => microtime(true);
    }

    /**
     * @param  array<string>  $hosts
     */
    public function setHosts(array $hosts): self
    {
        $this->nodes = [];

        foreach ($hosts as $host) {
            $this->nodes[] = $this->makeNode($host);
        }

        shuffle($this->nodes);

        $this->cursor = 0;

        return $this;
    }

    /**
     * @throws NoNodeAvailableException when no host is configured, or every host is dead and still inside its window
     */
    public function nextNode(): Node
    {
        $count = count($this->nodes);

        if ($count === 0) {
            throw new NoNodeAvailableException('No Elasticsearch hosts are configured.');
        }

        $now = ($this->clock)();

        for ($i = 0; $i < $count; $i++) {
            $node = $this->nodes[$this->cursor];
            $this->cursor = ($this->cursor + 1) % $count;

            if ($node->isAlive()) {
                return $node;
            }

            if ($now - ($node->deadAt ?? $now) >= $this->resurrectAfter) {
                // Eligible again. A failed attempt marks it dead anew.
                $node->markAlive(true);

                return $node;
            }
        }

        throw new NoNodeAvailableException(sprintf(
            'No alive nodes: all %d Elasticsearch host(s) are down and none is past its %s second resurrect_after window.',
            $count,
            rtrim(rtrim(number_format($this->resurrectAfter, 3, '.', ''), '0'), '.'),
        ));
    }

    /**
     * A node that records, on the pool clock, when it was marked dead.
     *
     * @return Node&object{deadAt: float|null}
     */
    private function makeNode(string $host): Node
    {
        return new class($host, $this->clock) extends Node
        {
            public ?float $deadAt = null;

            public function __construct(string $host, private readonly Closure $clock)
            {
                parent::__construct($host);
            }

            public function markAlive(bool $alive): void
            {
                parent::markAlive($alive);

                $this->deadAt = $alive ? null : ($this->clock)();
            }
        };
    }
}
