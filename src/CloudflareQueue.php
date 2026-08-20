<?php

namespace OssyCodes\LaravelCloudflareQueue;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Collection;

class CloudflareQueue extends Queue implements QueueContract
{
    /**
     * Cloudflare's hard maximum for delay_seconds on a single message (12 hours).
     *
     * Jobs delayed beyond this are transparently re-queued in 12-hour hops
     * until the target execution time is reached — supporting arbitrary delays.
     */
    const MAX_CF_DELAY_SECONDS = 43_200; // 12 * 3600

    /**
     * Marker key embedded in the message body when a job needs more than one hop.
     * Any message containing this key is a delay-relay wrapper, not a real job.
     */
    const DELAY_WRAPPER_KEY = '__cf_delay_wrapper';

    /**
     * In-memory buffer of messages pulled in the last batch.
     *
     * Laravel's Worker calls pop() one job at a time. Without a buffer every
     * pop() call would make a separate HTTP request even when batch_size > 1.
     * The buffer lets us pull N messages at once and return them one-by-one.
     *
     * Important: messages in the buffer are already "invisible" to other
     * consumers for visibility_timeout_ms. Set visibility_timeout_ms high
     * enough to cover (batch_size × average job processing time).
     */
    private array $messageBuffer = [];

    public function __construct(
        private readonly CloudflareClient $client,
        private readonly array $config,
    ) {
        $this->dispatchAfterCommit = $this->config['after_commit'] ?? false;
    }

    // -------------------------------------------------------------------------
    // Queue inspection
    // -------------------------------------------------------------------------

    /**
     * Get the total number of messages in the queue.
     */
    public function size($queue = null): int
    {
        $info = $this->client->info();

        return (int) ($info['message_backlog_count'] ?? 0);
    }

    /**
     * Get the number of pending (ready) messages.
     *
     * Cloudflare only exposes a single total backlog count, so we return that.
     */
    public function pendingSize($queue = null): int
    {
        return $this->size($queue);
    }

    /**
     * Cloudflare does not expose delayed message counts separately.
     */
    public function delayedSize($queue = null): int
    {
        return 0;
    }

    /**
     * Cloudflare does not expose reserved (in-flight) message counts separately.
     */
    public function reservedSize($queue = null): int
    {
        return 0;
    }

    /**
     * Cloudflare does not support listing individual pending messages via the REST API.
     */
    public function pendingJobs($queue = null): Collection
    {
        return new Collection();
    }

    /**
     * Cloudflare does not support listing individual delayed messages via the REST API.
     */
    public function delayedJobs($queue = null): Collection
    {
        return new Collection();
    }

    /**
     * Cloudflare does not support listing individual reserved messages via the REST API.
     */
    public function reservedJobs($queue = null): Collection
    {
        return new Collection();
    }

    /**
     * Cloudflare does not support listing individual pending messages via the REST API.
     */
    public function allPendingJobs(): Collection
    {
        return new Collection();
    }

    /**
     * Cloudflare does not support listing individual delayed messages via the REST API.
     */
    public function allDelayedJobs(): Collection
    {
        return new Collection();
    }

    /**
     * Cloudflare does not support listing individual reserved messages via the REST API.
     */
    public function allReservedJobs(): Collection
    {
        return new Collection();
    }

    /**
     * Cloudflare does not expose per-message creation timestamps via the REST API.
     */
    public function creationTimeOfOldestPendingJob($queue = null): ?int
    {
        return null;
    }

    // -------------------------------------------------------------------------
    // Pushing jobs
    // -------------------------------------------------------------------------

    /**
     * Push a new job onto the queue.
     */
    public function push($job, $data = '', $queue = null): mixed
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            null,
            fn ($payload, $queue) => $this->pushRaw($payload, $queue),
        );
    }

    /**
     * Push a raw payload string onto the queue.
     *
     * If the requested delay exceeds Cloudflare's 12-hour maximum, the payload
     * is wrapped with an absolute target timestamp and queued for the first 12-hour
     * hop. On each subsequent hop the worker detects the wrapper, re-queues for
     * the next hop, and ACKs the current message — repeating until the target
     * time is reached, at which point the original payload is unwrapped and processed.
     *
     * Example: a 30-hour delay becomes three hops — 12h, 12h, 6h — then runs.
     */
    public function pushRaw($payload, $queue = null, array $options = []): ?string
    {
        $delay         = (int) ($options['delay'] ?? 0);
        $originalUuid  = json_decode($payload, true)['uuid'] ?? null;

        if ($delay > self::MAX_CF_DELAY_SECONDS) {
            $payload = $this->wrapForDelayedHop($payload, time() + $delay);
            $delay   = self::MAX_CF_DELAY_SECONDS;
        }

        $this->client->send($payload, $delay);

        return $originalUuid;
    }

    /**
     * Push a new job onto the queue after a delay.
     */
    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data, $delay),
            $queue,
            $delay,
            fn ($payload, $queue, $delay) => $this->pushRaw(
                $payload,
                $queue,
                ['delay' => $this->secondsUntil($delay)]
            ),
        );
    }

    /**
     * Push an array of jobs onto the queue using the batch API when possible.
     */
    public function bulk($jobs, $data = '', $queue = null): void
    {
        if (empty($jobs)) {
            return;
        }

        $payloads = [];

        foreach ((array) $jobs as $job) {
            $payloads[] = $this->createPayload($job, $this->getQueue($queue), $data);
        }

        $this->client->bulkSend($payloads);
    }

    // -------------------------------------------------------------------------
    // Popping jobs
    // -------------------------------------------------------------------------

    /**
     * Pop the next job off the queue.
     *
     * Uses an in-memory message buffer: the first pop() in a cycle pulls a
     * full batch from Cloudflare and stores remaining messages locally.
     * Subsequent pop() calls drain the buffer without hitting the API.
     *
     * Delay-relay wrappers (jobs delayed beyond 12 hours) are transparently
     * re-queued for the next hop and skipped — the worker never sees them.
     */
    public function pop($queue = null): ?CloudflareJob
    {
        // Refill buffer when empty
        if (empty($this->messageBuffer)) {
            $this->messageBuffer = $this->client->pull(
                $this->batchSize(),
                $this->visibilityTimeoutMs(),
            );
        }

        if (empty($this->messageBuffer)) {
            return null;
        }

        $message = array_shift($this->messageBuffer);

        // Handle delay-relay wrappers transparently
        $message = $this->resolveDelayWrapper($message);

        // resolveDelayWrapper returns null when a wrapper was re-queued for a
        // future hop — recurse to get the next real job from the buffer.
        if ($message === null) {
            return $this->pop($queue);
        }

        return new CloudflareJob(
            $this->container,
            $this->client,
            $message,
            ['raw_handler' => $this->config['raw_handler'] ?? null],
            $this->connectionName,
            $this->getQueue($queue),
        );
    }

    /**
     * Inspect a pulled message for a delay-relay wrapper.
     *
     * - Not a wrapper           → return message unchanged (normal processing)
     * - Wrapper, time not yet   → re-queue for next hop, ACK current, return null
     * - Wrapper, time reached   → unwrap and return message with original payload
     */
    private function resolveDelayWrapper(array $message): ?array
    {
        $body = json_decode($message['body'], associative: true);

        if (! isset($body[self::DELAY_WRAPPER_KEY])) {
            return $message; // not a wrapper, nothing to do
        }

        $remaining = (int) $body['__cf_execute_at'] - time();

        if ($remaining > 0) {
            // Not ready yet — re-queue for the next hop
            $nextDelay = min($remaining, self::MAX_CF_DELAY_SECONDS);

            if ($remaining <= self::MAX_CF_DELAY_SECONDS) {
                // Final hop: send the original payload directly with remaining delay
                $this->client->send($body['__cf_payload'], $nextDelay);
            } else {
                // More hops needed: re-send the wrapper with the same execute_at
                $this->client->send(
                    $this->wrapForDelayedHop($body['__cf_payload'], (int) $body['__cf_execute_at']),
                    $nextDelay,
                );
            }

            // ACK the current wrapper — it has been handed off to the next hop
            $this->client->ack($message['lease_id']);

            return null; // signal pop() to move to the next message
        }

        // Target time reached — unwrap and let the worker process the original job
        $message['body'] = $body['__cf_payload'];

        return $message;
    }

    /**
     * Wrap a payload in a delay-relay envelope for multi-hop delivery.
     *
     * @param string $payload    The original job payload to preserve.
     * @param int    $executeAt  Unix timestamp when the job should actually run.
     */
    private function wrapForDelayedHop(string $payload, int $executeAt): string
    {
        return json_encode([
            self::DELAY_WRAPPER_KEY => true,
            '__cf_execute_at'       => $executeAt,
            '__cf_payload'          => $payload,
        ]);
    }

    // -------------------------------------------------------------------------
    // Job reservation management
    // -------------------------------------------------------------------------

    /**
     * Delete a reserved job from the queue by acknowledging it.
     */
    public function deleteReserved(string $queue, string $leaseId): void
    {
        $this->client->ack($leaseId);
    }

    /**
     * Delete a reserved job and release it back onto the queue with a delay.
     */
    public function deleteAndRelease(string $queue, CloudflareJob $job, int $delay): void
    {
        $this->client->retry($job->getLeaseId(), max(0, $delay));
    }

    // -------------------------------------------------------------------------
    // Clear
    // -------------------------------------------------------------------------

    /**
     * Cloudflare Queues does not expose a purge endpoint via the consumer REST API.
     *
     * To clear a queue, use the Cloudflare dashboard or Wrangler CLI:
     *   wrangler queues purge <queue-name>
     *
     * @throws \RuntimeException
     */
    public function clear($queue = null): int
    {
        throw new \RuntimeException(
            'Cloudflare Queues does not support clearing the queue via the REST API. '
            . 'Use the Cloudflare dashboard or `wrangler queues purge <queue-name>` instead.'
        );
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    public function getQueue($queue): string
    {
        return $queue ?: ($this->config['queue'] ?? 'default');
    }

    public function getDispatchAfterCommit(): bool
    {
        return $this->dispatchAfterCommit;
    }

    private function batchSize(): int
    {
        return max(1, (int) ($this->config['batch_size'] ?? 1));
    }

    private function visibilityTimeoutMs(): int
    {
        return max(1_000, (int) ($this->config['visibility_timeout_ms'] ?? 30_000));
    }
}
