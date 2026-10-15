---
myst:
  html_meta:
    description: "Choose between async def and def in Serve deployments, size the thread pool, and avoid blocking I/O behind a FastAPI ingress."
---

(serve-asyncio-best-practices)=

# Concurrency and `asyncio` best practices in Ray Serve

The code inside each replica of a Ray Serve deployment runs on an `asyncio` event loop. An `asyncio` event loop handles I/O-bound concurrency efficiently, but you need to follow a few best practices to get the best performance from it.

This guide explains when to use `async def` versus `def` in Ray Serve and how Ray Serve executes your code across event loops, threads, and the router. It also covers how `max_ongoing_requests` interacts with `asyncio` concurrency, and how to think about Python's global interpreter lock (GIL), native code, and true parallelism.

The examples assume the following imports unless stated otherwise:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __imports_begin__
:end-before: __imports_end__
:language: python
```

## How to choose between `async def` and `def`

Use this decision table as a starting point:

| Workload type | Recommended handler | Reason |
| --- | --- | --- |
| I/O-bound, such as databases, HTTP calls, and queues | `async def` | The event loop handles many requests while each waits on I/O. |
| CPU-bound, such as model inference and heavy numeric compute | `def` or `async def` with offload | Async alone doesn't make CPU work faster. You need more replicas, threads, or native parallelism. |
| Streaming responses | `async def` generator | Integrates with backpressure and non-blocking iteration. |
| FastAPI ingress with `@serve.ingress` | `def` or `async def` | FastAPI runs `def` endpoints in a thread pool, so they don't block the loop. |

## How Ray Serve executes your code

At a high level, requests go through a router to a replica actor that runs your code:

```text
Client
   ↓
Serve router (asyncio loop A)
   ↓
Replica actor
   ├─ System / control loop
   └─ User code loop (your handlers)
       └─ Optional thread pool for sync methods
```

Keep the following ideas in mind when you choose between `async def` and `def`:

- Serve uses `asyncio` event loops for routing and for running replicas.
- By default, user code runs on a separate event loop from the replica's main control loop, so blocking user code doesn't interfere with health checks and autoscaling.
- Depending on the value of `RAY_SERVE_RUN_SYNC_IN_THREADPOOL`, `def` handlers run either directly on the user event loop, where they block it, or in a thread pool, where they don't block the loop.

(pure-serve-deployments-no-fastapi-ingress)=
### Pure Serve deployments without FastAPI ingress

The following example shows an asynchronous deployment:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __echo_async_begin__
:end-before: __echo_async_end__
:language: python
```

- `async def __call__` runs directly on the replica's user event loop.
- While this handler awaits `asyncio.sleep`, the loop can start handling other requests.

The following example shows a synchronous deployment:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __blocking_echo_begin__
:end-before: __blocking_echo_end__
:language: python
```

How this method runs depends on the value of `RAY_SERVE_RUN_SYNC_IN_THREADPOOL`:

- With `RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1`, which is the default, Serve offloads `__call__` to a thread pool so the event loop stays responsive.
- With `RAY_SERVE_RUN_SYNC_IN_THREADPOOL=0`, `__call__` runs directly on the user event loop and blocks it for 1 second.

(fastapi-ingress-serve-ingress)=
### FastAPI ingress with `@serve.ingress`

When you use a FastAPI ingress, FastAPI controls how endpoints run:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __fastapi_deployment_begin__
:end-before: __fastapi_deployment_end__
:language: python
```

FastAPI ingress and pure Serve deployments differ in how they run `def` methods:

- FastAPI always dispatches `def` endpoints to a thread pool.
- In pure Serve, `def` methods run in a thread pool by default. Set `RAY_SERVE_RUN_SYNC_IN_THREADPOOL=0` to run them on the event loop instead.

(threadpool-sizing-and-overrides)=
## Thread pool sizing and overrides

Serve sets a default thread pool size for user code that mirrors Python's `ThreadPoolExecutor` defaults while respecting `ray_actor_options["num_cpus"]`.

If you need to tune the size, you can override the default executor inside your deployment:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __threadpool_override_begin__
:end-before: __threadpool_override_end__
:language: python
```

Use the following guidance to choose a size:

- The default is fine in most cases.
- For I/O-blocking code, consider a thread pool larger than `num_cpus`.
- For compute that releases the GIL, such as NumPy, pandas, and SciPy operations, keep the thread pool at or below `num_cpus`.

## Blocking versus non-blocking in practice

Blocking code keeps the event loop from processing other work. Non-blocking code yields control back to the loop when it's waiting on something.

### Blocking I/O versus asynchronous I/O

The following example uses blocking I/O:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __blocking_http_begin__
:end-before: __blocking_http_end__
:language: python
```

Even though the method is `async def`, `requests.get` blocks the loop. No other requests can run on this replica during the call. Blocking in `async def` is still blocking.

If the blocking call hangs and you've configured a request timeout, Ray Serve cancels the request when the timeout expires. That cancellation is best-effort. Python only delivers `asyncio` cancellation when the task cooperates and yields control back to the event loop. A synchronous call such as `requests.get` doesn't yield, so one hung request can stall the replica's event loop and keep later requests from running on that replica.

The following non-blocking equivalent uses an async HTTP client:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __async_http_begin__
:end-before: __async_http_end__
:language: python
```

The following non-blocking equivalent uses a thread pool:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __threaded_http_begin__
:end-before: __threaded_http_end__
:language: python
```

## Concurrency doesn't equal parallelism in Python

It's common to expect `async` code to "use all the cores" or make CPU-heavy code faster, but `asyncio` doesn't do either.

(concurrency-handling-many-waiting-operations)=
### Concurrency: Many waiting operations

For I/O-bound workloads, `asyncio` gives you *concurrency*:

- While one request waits on the database, another can wait on an HTTP call.
- Handlers yield back to the event loop at each `await`.

This model is ideal for high-throughput APIs that mostly wait on external systems.

(parallelism-using-multiple-cpu-cores)=
### Parallelism: Multiple CPU cores

True CPU parallelism usually comes from the following sources:

- Multiple processes, such as multiple Serve replicas.
- Native code that releases the GIL and runs across cores.

Because of Python's GIL, a process runs pure Python bytecode on one thread at a time, even if you use a thread pool.

(using-gil-releasing-native-code)=
### GIL-releasing native code

Many numeric and ML libraries release the GIL while doing heavy work in native code:

- NumPy, many linear algebra routines
- PyTorch and some other deep learning frameworks
- Some image-processing or compression libraries

In these cases, you can still get useful parallelism from threads inside a single replica process:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __numpy_deployment_begin__
:end-before: __numpy_deployment_end__
:language: python
```

Keep the following caveats in mind:

- GIL-releasing behavior is library-specific and sometimes operation-specific.
- Some libraries use their own internal thread pools. Combining them with your own thread pools can oversubscribe CPUs.
- Verify that your model stack is thread-safe before you rely on this form of parallelism.

For predictable CPU scaling, it's usually simpler to increase the number of replicas.

### Summary

- `async def` improves concurrency for I/O-bound code.
- Making CPU-bound code `async` doesn't make it faster.
- Parallel CPU scaling comes mostly from more processes, such as replicas or tasks, and in some cases from native code that releases the GIL.

## How `max_ongoing_requests` and replica concurrency work

Each deployment has a `max_ongoing_requests` setting that controls how many in-flight requests a replica handles at once.

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __max_ongoing_requests_begin__
:end-before: __max_ongoing_requests_end__
:language: python
```

The limit works as follows:

- Ray Serve uses an internal semaphore to limit concurrent in-flight requests per replica to `max_ongoing_requests`.
- Requests beyond that limit queue in the router or handle until capacity becomes available, or they fail with backpressure depending on configuration.

How useful `max_ongoing_requests` is depends on how your handler behaves.

### `async` handlers and `max_ongoing_requests`

With an `async def` handler that spends most of its time awaiting I/O, `max_ongoing_requests` directly controls concurrency:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __async_io_bound_begin__
:end-before: __async_io_bound_end__
:language: python
```

- Up to 100 requests can be in flight per replica.
- While one request is waiting, the event loop can work on others.

### Blocking `def` handlers and `max_ongoing_requests`

With a blocking `def` handler that runs on the event loop because the thread pool is disabled, `max_ongoing_requests` doesn't give you the concurrency you expect:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __blocking_cpu_begin__
:end-before: __blocking_cpu_end__
:language: python
```

This handler behaves as follows:

- The event loop can only run one handler at a time.
- Even though `max_ongoing_requests=100`, the replica effectively processes requests serially.

If you run sync methods in a thread pool, as the next section describes, each in-flight request can run in a thread:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __cpu_with_threadpool_begin__
:end-before: __cpu_with_threadpool_end__
:language: python
```

With the thread pool enabled, the handler behaves as follows:

- Up to `max_ongoing_requests` calls can run at once.
- Real throughput depends on the following factors:
  - How many threads the thread pool uses.
  - Whether your workload is CPU-bound or GIL-releasing.
  - The underlying native libraries and system resources.

For heavily CPU-bound workloads, it's usually better to do the following:

- Keep `max_ongoing_requests` modest to avoid queueing too many heavy tasks.
- Scale replicas with `num_replicas` rather than pushing a single replica's concurrency too high.

(environment-flags-and-sync-in-threadpool-warning)=
## Environment flags and sync-in-thread-pool warning

Ray Serve exposes several environment variables that control how user code interacts with event loops and threads.

### `RAY_SERVE_RUN_SYNC_IN_THREADPOOL`

By default, `RAY_SERVE_RUN_SYNC_IN_THREADPOOL` is `1`, so synchronous methods in a deployment run in a thread pool:

```bash
export RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1
```

When this flag is `1`, Serve behaves as follows:

- Serve runs synchronous methods in a thread pool.
- The event loop can keep serving other requests while sync methods run.

In this mode, make sure of the following:

- Your handler code and any shared state are thread-safe.
- Your code can safely use your model objects from multiple threads, or protects them with locks.

Set this flag to `0` to keep the legacy behavior of running synchronous methods directly on the user event loop.

### `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD`

By default, Serve runs user code in a separate event loop from the replica's main control loop:

```bash
export RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD=1  # default
```

This isolation has two effects:

- It keeps user code from blocking system tasks, such as health checks and controller communication.
- It adds some overhead to cross-loop communication, which increases request latency. For throughput-optimized configurations, see [High throughput optimization](serve-high-throughput).

You can disable this behavior:

```bash
export RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD=0
```

This setting is for advanced use only. When user code and system tasks share a loop, any blocking operation in user code can interfere with replica health and control-plane operations.

### `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP`

By default, Serve also runs its request router on its own event loop:

```bash
export RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=1  # default
```

The separate loop means the router can continue routing and load balancing requests even if some replicas are running slow user code.

To disable this behavior, set the flag to `0`:

```bash
export RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0
```

With the flag at `0`, the router shares an event loop with other work. This can reduce overhead in advanced, highly optimized scenarios, but makes the system more sensitive to blocking operations. See [High throughput optimization](serve-high-throughput).

For most production deployments, keep both separate-loop flags at their default of `1`.

## Batching and streaming semantics

Batching and streaming both rely on the event loop to stay responsive. They don't change where your code runs: batched handlers and streaming handlers still run on the same user event loop as any other handler. Adding batching or streaming on top of blocking code can make the effects of event loop blocking much worse.

### Batching

When you enable batching, Serve groups multiple incoming requests together and passes them to your handler as a list. The handler still runs on the user event loop, but each call processes many requests at once instead of one. If that batched work is blocking, it blocks the event loop for all of those requests at the same time.

The following example shows a batched deployment:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __batched_model_begin__
:end-before: __batched_model_end__
:language: python
```

The batch handler runs on the user event loop. If `_run_model` is CPU-heavy and runs inline, it blocks the loop for the duration of the batch. You can offload the batch computation instead:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __batched_model_offload_begin__
:end-before: __batched_model_offload_end__
:language: python
:emphasize-lines: 9-16
```

This keeps the event loop responsive while the model runs in a thread.

#### `max_concurrent_batches` and event loop yielding

The `@serve.batch` decorator accepts a `max_concurrent_batches` argument that controls how many batches can run concurrently. However, this argument only works effectively if your batch handler yields control back to the event loop during processing.

If your batch handler blocks the event loop, for example by doing heavy CPU work without awaiting or offloading it, `max_concurrent_batches` doesn't provide the concurrency you expect. The event loop can only start processing a new batch when the current batch yields control.

To benefit from `max_concurrent_batches`, do the following:

- Use `async def` for your batch handler and `await` I/O operations or offloaded CPU work.
- Offload CPU-heavy batch processing to a thread pool with `asyncio.to_thread()` or `loop.run_in_executor()`.
- Avoid blocking operations that prevent the event loop from scheduling other batches.

In the preceding offloaded batch example, the handler yields to the event loop when it awaits the thread pool executor. As a result, multiple batches can be in flight at once, up to the `max_concurrent_batches` limit.

### Streaming

Streaming is different from a regular response because the client starts receiving data while your handler is still running. Serve calls your handler once, gets back a generator or async generator, and then repeatedly asks it for the next chunk. That generator code still runs on the user event loop, unless you offload it to a worker thread.

Streaming is especially sensitive to blocking:

- If you block between chunks, you delay the next piece of data to the client.
- While the generator blocks the event loop, other requests on that loop can't make progress.
- The system also can't react quickly to backpressure from slow clients or to cancellation.

The following example blocks while streaming:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __blocking_stream_begin__
:end-before: __blocking_stream_end__
:language: python
```

The following example shows a non-blocking version:

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __async_stream_begin__
:end-before: __async_stream_end__
:language: python
```

When you stream responses, follow these guidelines:

- Prefer `async def` generators that use `await` between yields.
- Avoid long CPU-bound loops between yields. Offload them if you need them.

(offloading-patterns-i-o-cpu)=
## Offloading patterns for I/O and CPU work

This section summarizes common offloading patterns you can use inside `async` handlers.

### Blocking I/O in `async def`

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __offload_io_begin__
:end-before: __offload_io_end__
:language: python
```

### CPU-heavy code in `async def`

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __offload_cpu_begin__
:end-before: __offload_cpu_end__
:language: python
```

(advance-using-ray-tasks-or-remote-actors-for-true-parallelism)=
### Advanced: Ray tasks or remote actors for true parallelism

:::{note}
You can spawn Ray tasks from Ray Serve deployments, but this approach isn't recommended because it lacks tooling for observability and debugging.
:::

```{literalinclude} ../doc_code/asyncio_best_practices.py
:start-after: __ray_parallel_begin__
:end-before: __ray_parallel_end__
:language: python
```

This pattern uses multiple Ray workers and processes and bypasses the GIL limitation of a single Python process.

## Summary

- Use `async def` for I/O-bound and streaming work so the event loop can stay responsive.
- Use `max_ongoing_requests` to bound concurrency per replica, but remember that blocking `def` handlers can still serialize work if they run on the event loop.
- Keep `RAY_SERVE_RUN_SYNC_IN_THREADPOOL` at its default of `1` only if your code is thread-safe. If you set it to `0`, Serve warns when it calls a sync method directly on the event loop.
- For CPU-heavy workloads, scale replicas or use GIL-releasing native code for real parallelism.
