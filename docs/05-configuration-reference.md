# Configuration Reference

Every configuration option explained in detail — what it does, what happens if you set it wrong, and how to choose the right value.

---

## Full Configuration Example

```php
// config/queue.php
'cloudflare' => [
    'driver'                => 'cloudflare',
    'account_id'            => env('CLOUDFLARE_ACCOUNT_ID'),
    'queue_id'              => env('CLOUDFLARE_QUEUE_ID'),
    'api_token'             => env('CLOUDFLARE_API_TOKEN'),
    'batch_size'            => env('CLOUDFLARE_QUEUE_BATCH_SIZE', 1),
    'visibility_timeout_ms' => env('CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS', 30000),
    'after_commit'          => false,
    'raw_handler'           => null,
    'queue'                 => 'default',
],
```

---

## Required Options

### `driver`
**Value:** Always `'cloudflare'`

Tells Laravel which connector to use. This string must match what the service provider registers (`addConnector('cloudflare', ...)`). Don't change it.

---

### `account_id`
**Type:** String  
**Where to find:** Cloudflare Dashboard → right sidebar under any page

Your Cloudflare account identifier. Looks like: `a1b2c3d4e5f6...` (32 hex characters).

Used to construct the API URL:
```
https://api.cloudflare.com/client/v4/accounts/{account_id}/queues/{queue_id}/...
```

**What happens if wrong:** `RuntimeException` — Cloudflare returns 403 or 404.

---

### `queue_id`
**Type:** String (UUID)  
**Where to find:** Cloudflare Dashboard → Workers & Pages → Queues → click your queue

The unique identifier for the specific queue. Looks like: `abc123de-f456-7890-abcd-ef1234567890`.

Different from the queue name (`my-laravel-queue`) — the ID is the stable identifier used in API calls.

**What happens if wrong:** `RuntimeException` — Cloudflare returns 404 "queue not found".

---

### `api_token`
**Type:** String  
**Required permissions:** Cloudflare Queues → Edit

The Bearer token for authenticating with the Cloudflare API.

**Security:** Always load from `.env`, never hardcode.

**What happens if wrong:** `RuntimeException` — Cloudflare returns 403 "insufficient permissions" or 401 "invalid token".

---

## Optional Options

### `batch_size`
**Type:** Integer  
**Default:** `1`  
**Range:** 1–100 (Cloudflare's maximum)  
**Env var:** `CLOUDFLARE_QUEUE_BATCH_SIZE`

How many messages to pull from Cloudflare in a single API call. Pulled messages are stored in an in-memory buffer and returned one-by-one to Laravel's worker on each `pop()` call.

**Effect on performance:**

| batch_size | API calls per 100 jobs | HTTP overhead |
|---|---|---|
| 1 | 100 calls | High |
| 10 | 10 calls | Low |
| 50 | 2 calls | Very low |
| 100 | 1 call | Minimal |

**The tradeoff:** Higher `batch_size` means fewer API calls but longer messages sit in the buffer. Messages in the buffer are invisible to other consumers for `visibility_timeout_ms`. If they stay in the buffer longer than that timeout, Cloudflare makes them visible again, causing duplicate processing.

**Rule of thumb:**
```
visibility_timeout_ms > batch_size × average_job_processing_time_ms

Example: batch_size=10, jobs take ~2s each
  Worst case: job 10 waits 9 × 2000ms = 18_000ms in buffer
  Set visibility_timeout_ms to at least 30_000ms (30s)
```

**Recommended starting values:**
- Single worker, fast jobs (<1s): `batch_size=10`, `visibility_timeout_ms=30000`
- Multiple workers, slower jobs (1-5s): `batch_size=5`, `visibility_timeout_ms=60000`
- Very slow jobs (>5s) or critical no-duplicate guarantee: `batch_size=1`

---

### `visibility_timeout_ms`
**Type:** Integer (milliseconds)  
**Default:** `30000` (30 seconds)  
**Minimum enforced by this package:** `1000` (1 second)  
**Env var:** `CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS`

How long a pulled message is "hidden" from all consumers after being pulled. During this window, no other worker can see or pull the same message, preventing duplicate processing.

**If too short:**
- A job is pulled but takes longer than `visibility_timeout_ms` to process
- Cloudflare makes the message visible again while the original worker is still processing it
- Another worker pulls the same message → **duplicate processing**

**If too long:**
- If a worker crashes mid-job, the message won't reappear for retry until the timeout expires
- High `visibility_timeout_ms` means slower recovery from worker crashes

**General guidance:**
- For individual messages (`batch_size=1`): set to `max(30000, job_timeout_ms × 2)`
- For batches: set to `max(30000, batch_size × avg_job_ms × 2)`
- Never set below 10 seconds in production

---

### `after_commit`
**Type:** Boolean  
**Default:** `false`

When `true`, jobs dispatched inside a database transaction are not sent to Cloudflare until the transaction successfully commits.

**When to use `true`:**

```php
// Scenario: creating a user and dispatching a welcome email
DB::transaction(function () use ($data) {
    $user = User::create($data);
    SendWelcomeEmail::dispatch($user);
    // With after_commit=false (default):
    //   Job dispatched immediately → worker might run before User::create() commits
    //   Result: SendWelcomeEmail::handle() finds no user → error
    //
    // With after_commit=true:
    //   Job held in memory until this transaction block exits cleanly
    //   Worker only runs after the user row is committed to the DB
});
```

**When to leave as `false` (default):**
- You're not using database transactions, OR
- Your jobs don't read from the database immediately

---

### `raw_handler`
**Type:** `null` or a fully-qualified class name  
**Default:** `null`

> **Most applications leave this `null`.** You only need it when a Cloudflare Worker (not Laravel) is the producer.

```
Who is pushing messages to the queue?
        │
        ├── Laravel only  (::dispatch())
        │       → raw_handler = null          ← default, nothing to configure
        │
        ├── Cloudflare Worker only
        │       → raw_handler = YourHandler::class
        │
        └── Both Laravel and a Cloudflare Worker
                → two separate connections pointing to two different CF Queues
                  one with raw_handler set, one without
```

**Why it exists:** When Laravel dispatches a job it wraps it in a PHP-serialized payload that the worker knows how to deserialize. A Cloudflare Worker just sends plain JSON — no PHP serialization, no class name, nothing Laravel recognizes. `raw_handler` tells the package which class to route those plain JSON messages to.

**When `null` (default):** Every message is treated as a standard Laravel job payload. This is correct for 99% of Laravel applications.

**When set to a class:** Every message on that connection is decoded from JSON and routed to that class — including any Laravel-dispatched jobs, which is why mixing the two on the same connection breaks things.

```php
'raw_handler' => App\Jobs\HandleWorkerMessage::class,
```

Your handler class must implement `ShouldQueue` and accept the raw data array in its constructor:

```php
class HandleWorkerMessage implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable;

    // Receives the decoded JSON from the CF Worker message as a plain PHP array
    public function __construct(public readonly array $data) {}

    public function handle(): void
    {
        // $this->data = whatever the CF Worker sent
        // e.g. ['email_to' => 'user@example.com', 'subject' => 'Hello']
        Mail::to($this->data['email_to'])->send(...);
    }
}
```

For a complete example including validation, retry logic, and the `failed()` method, see **[07 — Raw Handler Guide](./07-raw-handler-guide.md)**.

---

### `queue`
**Type:** String  
**Default:** `'default'`

The default queue name used when no specific queue is specified in the dispatch call.

```php
// Uses 'default' queue
SendEmailJob::dispatch($user);

// Overrides to use 'emails' queue
SendEmailJob::dispatch($user)->onQueue('emails');
```

Note: Cloudflare Queue is a single queue identified by `queue_id`. The queue name here is a label for Laravel's internal routing — it doesn't create a new Cloudflare queue. If you need multiple actual queues, create multiple Cloudflare queues and define multiple connections in `config/queue.php`.

---

## Multiple Queue Connections

You can define multiple Cloudflare Queue connections, each pointing to a different Cloudflare Queue:

```php
'connections' => [

    'cloudflare-emails' => [
        'driver'     => 'cloudflare',
        'account_id' => env('CLOUDFLARE_ACCOUNT_ID'),
        'queue_id'   => env('CLOUDFLARE_EMAIL_QUEUE_ID'),    // different queue
        'api_token'  => env('CLOUDFLARE_API_TOKEN'),
        'batch_size' => 10,
    ],

    'cloudflare-notifications' => [
        'driver'     => 'cloudflare',
        'account_id' => env('CLOUDFLARE_ACCOUNT_ID'),
        'queue_id'   => env('CLOUDFLARE_NOTIF_QUEUE_ID'),    // different queue
        'api_token'  => env('CLOUDFLARE_API_TOKEN'),
        'batch_size' => 5,
    ],

],
```

Dispatch to a specific connection:
```php
SendEmailJob::dispatch($user)->onConnection('cloudflare-emails');
```

Run separate workers per connection:
```bash
php artisan queue:work cloudflare-emails &
php artisan queue:work cloudflare-notifications &
```

---

## Delays Beyond 12 Hours — Automatic Hop Relay

Cloudflare Queues caps `delay_seconds` at **43,200 seconds (12 hours)**. If you dispatch a job with a longer delay, this package transparently handles it using a hop-based relay — no configuration needed.

### How it works

When `delay > 43,200s`, `pushRaw()` wraps the original payload in an envelope:

```json
{
    "__cf_delay_wrapper": true,
    "__cf_execute_at": 1234567890,
    "__cf_payload": "{...original Laravel job payload...}"
}
```

`__cf_execute_at` is the absolute Unix timestamp for when the job should actually run. The wrapper is sent with a 12-hour delay — Cloudflare's maximum.

When the worker pulls the wrapper after 12 hours, `pop()` inspects it:

```
remaining = __cf_execute_at - now()

if remaining > 12h  → re-wrap with same execute_at, re-queue with 12h delay, ACK current
if remaining ≤ 12h  → send original payload with remaining delay (no wrapper), ACK current
if remaining ≤ 0    → unwrap, process the original job now
```

The worker never sees the wrapper — it's an internal relay mechanism.

### Example: a 30-hour delay

```
Dispatch: MyJob::dispatch()->delay(now()->addHours(30))
  ↓ execute_at = now + 30h
  ↓ send wrapper with delay=12h

After 12h: worker pulls wrapper, remaining=18h > 12h
  → re-queue wrapper with delay=12h
  → ACK current wrapper

After 24h: worker pulls wrapper, remaining=6h ≤ 12h
  → send original payload with delay=6h (no wrapper)
  → ACK current wrapper

After 30h: worker pulls original payload
  → MyJob::handle() runs ✅
```

### Usage

```php
// All of these work transparently
MyJob::dispatch($data)->delay(now()->addHours(6));    // within 12h, no hops needed
MyJob::dispatch($data)->delay(now()->addHours(20));   // two hops: 12h + 8h
MyJob::dispatch($data)->delay(now()->addDays(3));     // six hops of 12h
MyJob::dispatch($data)->delay(now()->addDays(30));    // 60 hops — works fine
```

### One thing to be aware of

The worker must be **running** when each hop lands, otherwise the re-queuing is delayed until the next time `queue:work` polls. This is the same behaviour as any other delayed job — if the worker is down, jobs are processed late. The job will never be lost (Cloudflare holds it), just delayed until the worker is back.

---

## Environment Variable Reference

| Variable | Config key | Default | Required |
|---|---|---|---|
| `CLOUDFLARE_ACCOUNT_ID` | `account_id` | — | Yes |
| `CLOUDFLARE_QUEUE_ID` | `queue_id` | — | Yes |
| `CLOUDFLARE_API_TOKEN` | `api_token` | — | Yes |
| `CLOUDFLARE_QUEUE_BATCH_SIZE` | `batch_size` | `1` | No |
| `CLOUDFLARE_QUEUE_VISIBILITY_TIMEOUT_MS` | `visibility_timeout_ms` | `30000` | No |
