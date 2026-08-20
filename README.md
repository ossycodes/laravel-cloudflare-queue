# Laravel Cloudflare Queue

A production-ready Cloudflare Queue driver for Laravel that implements the full Queue contract, fixing all known issues with existing community packages.

## Requirements

- PHP 8.1+
- Laravel 10, 11, or 12
- Cloudflare account with a Queue created
- Cloudflare API token with **Queues Read** and **Queues Write** permissions

## Installation

```bash
composer require ossycodes/laravel-cloudflare-queue
```

The service provider is auto-discovered via Laravel's package discovery.

## Configuration

Add a connection entry to `config/queue.php`:

```php
'connections' => [

    'cloudflare' => [
        'driver'               => 'cloudflare',
        'account_id'           => env('CLOUDFLARE_ACCOUNT_ID'),
        'queue_id'             => env('CLOUDFLARE_QUEUE_ID'),
        'api_token'            => env('CLOUDFLARE_API_TOKEN'),

        // Number of messages to pull per poll cycle (default: 1)
        // Increase this to reduce API calls when processing at volume.
        // Set visibility_timeout_ms high enough to cover:
        //   batch_size × average job processing time
        'batch_size'           => env('CLOUDFLARE_QUEUE_BATCH_SIZE', 1),

        // How long a pulled message is hidden from other consumers (ms, default: 30000)
        // Must be > batch_size × average processing time to prevent duplicate processing.
        'visibility_timeout_ms' => env('CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS', 30000),

        // Dispatch jobs after the current database transaction commits (default: false)
        'after_commit'         => false,

        // Optional: class to handle raw messages pushed by a Cloudflare Worker
        // (not by Laravel's own Queue::push). Leave null for standard Laravel jobs.
        'raw_handler'          => null,
    ],

],
```

Add the corresponding `.env` variables:

```env
CLOUDFLARE_ACCOUNT_ID=your-account-id
CLOUDFLARE_QUEUE_ID=your-queue-id
CLOUDFLARE_API_TOKEN=your-api-token
```

Set as the default queue driver:

```env
QUEUE_CONNECTION=cloudflare
```

## Usage

### Dispatching Standard Laravel Jobs

Works exactly like any other Laravel queue driver:

```php
// Dispatch immediately
MyJob::dispatch($data);

// Dispatch with a delay
MyJob::dispatch($data)->delay(now()->addMinutes(5));

// Dispatch multiple jobs
Queue::bulk([new JobA(), new JobB()]);
```

### Processing Jobs

```bash
php artisan queue:work cloudflare
php artisan queue:work cloudflare --sleep=3 --tries=3
```

### Batch Processing (Reducing API Calls)

Set `batch_size` > 1 to pull multiple messages per API call. The driver buffers pulled messages in memory and returns them one-by-one to Laravel's worker loop — so the API is only called when the buffer is empty, not on every job.

```php
// config/queue.php
'cloudflare' => [
    'batch_size'            => 10,
    'visibility_timeout_ms' => 60_000, // 60s — must cover 10 × avg job time
],
```

> **Important:** `visibility_timeout_ms` must be large enough to cover
> `batch_size × average_job_processing_time`. If jobs in the buffer expire
> before they're processed, Cloudflare will make them visible again, causing
> duplicate processing.

### Handling Raw Messages from a Cloudflare Worker

If you push messages from a Cloudflare Worker (not from Laravel), the message body won't follow Laravel's job serialization format. Use `raw_handler` to route these messages to a specific class:

**Cloudflare Worker (producer):**
```javascript
await env.MY_QUEUE.send({ email_to: "user@example.com", subject: "Hello" });
```

**Laravel config:**
```php
'cloudflare' => [
    'raw_handler' => App\Jobs\HandleWorkerEmail::class,
],
```

**Handler class:**
```php
class HandleWorkerEmail implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable;

    public function __construct(public readonly array $data) {}

    public function handle(): void
    {
        // $this->data = ['email_to' => 'user@example.com', 'subject' => 'Hello']
        Mail::to($this->data['email_to'])->send(new MyMail($this->data));
    }
}
```

### `after_commit`

When `after_commit` is `true`, jobs dispatched inside a database transaction won't be placed on the queue until the transaction successfully commits. This prevents a race where the worker picks up the job before the DB write is visible.

```php
'after_commit' => true,
```

## Known Limitations

### Queue Clearing Not Supported

The Cloudflare Queues REST API does not expose a purge endpoint for consumers. Calling `Queue::clear()` or `php artisan queue:clear cloudflare` will throw a `RuntimeException`.

To clear a queue, use:
- The Cloudflare Dashboard → Queues → select queue → Purge
- Or the Wrangler CLI: `wrangler queues purge <queue-name>`

### Job Inspection (Laravel Horizon)

Cloudflare Queues does not expose individual job listings via the REST API. The following methods return empty collections (same behaviour as SQS and Beanstalkd):

- `pendingJobs()`, `delayedJobs()`, `reservedJobs()`
- `allPendingJobs()`, `allDelayedJobs()`, `allReservedJobs()`

`pendingSize()` returns the total message backlog count from the queue metadata.
`delayedSize()` and `reservedSize()` return 0.

Laravel Horizon is **not fully supported**.

### Delayed Jobs (Arbitrary Delays Supported)

Cloudflare Queues caps `delay_seconds` at 12 hours (43,200 seconds). This package transparently handles delays beyond that limit using a **hop-based relay** — the payload is re-queued in 12-hour increments until the target time is reached.

```php
// All of these work, even beyond 12 hours
MyJob::dispatch($data)->delay(now()->addHours(6));   // single hop
MyJob::dispatch($data)->delay(now()->addHours(20));  // two hops: 12h + 8h
MyJob::dispatch($data)->delay(now()->addDays(3));    // six hops of 12h
```

No configuration required — it works automatically.

### No Blocking Pop

Unlike Redis and Beanstalkd, Cloudflare Queues does not support long-polling. The worker will poll every `--sleep` seconds when the queue is empty.


## Running Tests

```bash
composer install
./vendor/bin/pest
./vendor/bin/pest --coverage
```

## License

MIT

## How do I say Thank you?

Please buy me a cup of coffee https://www.paypal.com/paypalme/osaigbovoemmanuel , Leave a star and follow me on [Twitter](https://twitter.com/ossycodes) .