---
myst:
  html_meta:
    description: "Tune Ray Serve performance: debug the request path, use async methods, set end-to-end request timeouts, and configure locality-based routing."
---

(serve-perf-tuning)=
# Performance tuning

This page describes Ray Serve's performance characteristics and offers ways to debug and tune your Serve application's performance.

:::{note}
See the [architecture page](serve-architecture) for helpful context, including an overview of the HTTP proxy actor and deployment replica actors.
:::

```{contents}
```

## Performance and benchmarks

Ray Serve is built on top of Ray, so Ray's scalability bounds Serve's scalability. See Ray's [scalability envelope](https://github.com/ray-project/ray/blob/master/release/benchmarks/README.md) to learn more about the maximum number of nodes and other limitations.

(debugging-performance-issues-in-request-path)=
## Debug performance issues in the request path

The performance issue you're most likely to encounter is high latency or low throughput for requests.

After you set up [monitoring](serve-monitoring) with Ray and Ray Serve, these issues might appear as the following symptoms:

* `serve_num_router_requests_total` staying constant while your load increases.
* `serve_deployment_processing_latency_ms` spiking up as queries queue up in the background.

To address these issues, try the following:

1. Make sure you're using the right hardware and resources:
   * Are you reserving GPUs for your deployment replicas with `ray_actor_options`, for example, `ray_actor_options={"num_gpus": 1}`?
   * Are you reserving one or more cores for your deployment replicas with `ray_actor_options`, for example, `ray_actor_options={"num_cpus": 2}`?
   * Are you setting [`OMP_NUM_THREADS`](serve-omp-num-threads) to increase the performance of your deep learning framework?
1. Try batching your requests. See [Dynamic request batching](serve-performance-batching-requests).
1. Consider using `async` methods in your callable. See [Use `async` methods](serve-performance-async-methods).
1. Set an end-to-end timeout for your HTTP requests. See [Set an end-to-end request timeout](serve-performance-e2e-timeout).


(serve-performance-async-methods)=
(using-async-methods)=
### Use `async` methods

:::{note}
According to the [FastAPI documentation](https://fastapi.tiangolo.com/async/#very-technical-details), FastAPI calls `def` endpoint functions in a separate thread pool. As a result, you might observe many requests running at the same time inside one replica, which might cause out-of-memory (OOM) errors or resource starvation. In this case, you can try `async def` to control the workload performance.
:::

Are you using `async def` in your callable? If you're using `asyncio` and hitting the queuing issue described earlier, you might want to increase `max_ongoing_requests`. By default, Serve sets `max_ongoing_requests` to a low value, 5, so that clients receive proper backpressure. You can increase the value in the deployment decorator, for example, `@serve.deployment(max_ongoing_requests=1000)`.

(serve-performance-e2e-timeout)=
### Set an end-to-end request timeout

By default, Serve runs client HTTP requests to completion no matter how long they take. Slow requests can bottleneck replica processing and block other waiting requests. Set an end-to-end timeout so that Serve terminates slow requests and clients can retry them.

To set an end-to-end timeout for HTTP requests, set the `request_timeout_s` parameter in the `http_options` field of the Serve config file. HTTP proxies wait that many seconds before terminating an HTTP request. This setting is global to your Ray cluster, and you can't update it at runtime. Use [client-side retries](serve-best-practices-http-requests) to retry requests that time out because of transient failures.

:::{note}
Serve returns a response with status code `408` when a request times out. Clients can retry when they receive this `408` response.
:::

(serve-performance-per-request-headers)=
### Override timeout and disconnect behavior per request

Use two request headers to override the global `request_timeout_s` setting and the client-disconnect policy for a single request.

`x-request-timeout-seconds`
: Overrides `request_timeout_s` for this single request.
  - A positive float sets the timeout in seconds, for example, `x-request-timeout-seconds: 30`.
  - A non-positive value, such as `0` or `-1`, disables the timeout for this request regardless of the global setting.
  - An absent or non-numeric value falls back to the global `request_timeout_s`.

`x-request-disconnect-disabled`
: Controls whether Serve monitors for client disconnects on this request.
  - `?1` disables disconnect detection. Serve continues processing even if the client closes the connection.
  - `?0`, or no header, keeps disconnect detection on. This is the default behavior.

#### Performance impact of disabling the timeout and disconnect detection

When a request sets both `x-request-timeout-seconds` to a non-positive value and `x-request-disconnect-disabled: ?1`, the direct-ingress handler takes a fast path that yields a meaningful throughput improvement. The fast path differs from the default path in three ways:

- Serve skips wrapping the user handler in an `asyncio.create_task()` call.
- Serve skips the `asyncio.wait()` call that would otherwise coordinate the handler task, the disconnect-monitoring task, and the timeout watcher.
- Instead, Serve directly `await`s the user handler, which eliminates all per-request event-loop scheduling overhead for monitoring.

In the proxy path, the gain is more modest. Serve calls `asyncio.wait()` for each response chunk with one fewer task and no timeout handle, which reduces event-loop overhead at high request rates.


(set-backoff-time-when-choosing-replica)=
### Set the backoff time for choosing a replica

The request router uses an exponential backoff strategy when it retries routing requests to replicas that are temporarily unavailable. Tuning this backoff behavior can help reduce latency while the router waits for replicas to become ready. To optimize it for your workload, configure the following environment variables:

- `RAY_SERVE_ROUTER_RETRY_INITIAL_BACKOFF_S`: The initial backoff time, in seconds, before retrying a request. Default is `0.025`.
- `RAY_SERVE_ROUTER_RETRY_BACKOFF_MULTIPLIER`: The multiplier applied to the backoff time after each retry. Default is `2`.
- `RAY_SERVE_ROUTER_RETRY_MAX_BACKOFF_S`: The maximum backoff time, in seconds, between retries. Default is `0.5`.

### Set timeouts while probing replicas for queue length

Ray Serve's request router probes replicas for their queue lengths to make load-balancing decisions. You can tune the following environment variables to optimize this behavior for your workload:

- `RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S`: The initial timeout, in seconds, for replicas to respond with their queue length information. Default is `0.1`.
- `RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S`: The maximum timeout, in seconds, for queue length responses. When the router retries with exponential backoff, the deadline increases but never exceeds this value. Default is `1.0`.
- `RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S`: How long, in seconds, the router treats cached queue length information from replicas as valid. After this timeout, the cache entry expires and the router must probe the replica again. Default is `10.0`.

### Configure locality-based routing

Ray Serve routes requests to replicas based on locality to reduce network latency. Ray Serve applies locality routing to proxy-to-replica communication, which carries HTTP and gRPC requests, and to inter-deployment communication, which carries replica-to-replica calls through a `DeploymentHandle`.

#### Routing priority

When locality routing is enabled, Ray Serve selects replicas in the following priority order:

1. **Same node**: Replicas running on the same node as the caller, which have the lowest latency.
1. **Same availability zone**: Replicas in the same availability zone as the caller.
1. **Any replica**: All available replicas, as a fallback when local replicas are busy.

If replicas at a higher priority level are busy or unavailable, Ray Serve falls back to the next level.

#### Proxy-to-replica routing

Configure proxy routing behavior with the following environment variables:

- `RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING`: When enabled, the proxy prefers routing requests to replicas on the same node. Default is `1`, which means enabled.
- `RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING`: When enabled, the proxy prefers routing requests to replicas in the same availability zone. Default is `1`, which means enabled.

#### Inter-deployment routing

When one deployment calls another through a `DeploymentHandle`, you can enable locality routing to reduce latency between deployments.

By default, inter-deployment calls don't prefer same-node replicas. To enable same-node routing, initialize the handle with the `_prefer_local_routing` option:

```python
from ray import serve
from ray.serve.handle import DeploymentHandle

@serve.deployment
class Caller:
    def __init__(self, target_handle: DeploymentHandle):
        # Enable same-node routing for this handle
        self._handle = target_handle.options(_prefer_local_routing=True)

    async def call_target(self):
        # Requests prefer replicas on the same node as this Caller replica
        return await self._handle.remote()
```

The `RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING` environment variable controls availability zone routing for both proxy and inter-deployment communication. You can't configure availability zone routing per handle.

(serve-high-throughput)=
### Enable throughput-optimized serving

:::{note}
In Ray v2.54.0, the defaults for `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD` and `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP` will change to `0` for improved performance.
:::

This section describes how to turn on Ray Serve options that target higher throughput and lower latency. These options focus on two areas:

- Reducing overhead associated with frequent logging.
- Disabling behavior that tolerates blocking operations in Serve applications.

If your Ray Serve code includes thread-blocking operations, you must refactor your code to get the higher throughput. The following table shows examples of blocking and non-blocking code:

<table>
<tr>
<th>Blocking operation (❌)</th>
<th>Non-blocking operation (✅)</th>
</tr>
<tr>
<td>

```python
from ray import serve
from fastapi import FastAPI
import time

app = FastAPI()

@serve.deployment
@serve.ingress(app)
class BlockingDeployment:
    @app.get("/process")
    async def process(self):
        # ❌ Blocking operation
        time.sleep(2)
        return {"message": "Processed (blocking)"}

serve.run(BlockingDeployment.bind())
```

</td>
<td>

```python
from ray import serve
from fastapi import FastAPI
import asyncio

app = FastAPI()

@serve.deployment
@serve.ingress(app)
class NonBlockingDeployment:
    @app.get("/process")
    async def process(self):
        # ✅ Non-blocking operation
        await asyncio.sleep(2)
        return {"message": "Processed (non-blocking)"}

serve.run(NonBlockingDeployment.bind())
```

</td>
</tr>
</table>

To configure all options to the recommended settings, set the environment variable `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`.

You can also configure each option individually. The following table describes the recommended configurations and their impact:

| Configured value | Impact |
| --- | --- |
| `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD=0` | Your code runs in the same event loop as the replica's main event loop. You must avoid blocking operations in your request path. Set this variable to `1` to run your code in a separate event loop, which protects the replica's ability to communicate with the Serve controller if your code has blocking operations. |
| `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0`| The request router runs in the same event loop as your code. You must avoid blocking operations in your request path. Set this variable to `1` to run the router in a separate event loop, which protects Ray Serve's request routing when your code has blocking operations. |
| `RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE=1000` | Sets the log buffer to batch writes to every `1000` logs, flushing the buffer on write. Ray Serve always flushes the buffer and writes logs when it detects a line with level `ERROR`. Set the buffer size to `1` to disable buffering and write logs immediately. |
| `RAY_SERVE_LOG_TO_STDERR=0` | Ray Serve writes logs only to files under the `logs/serve/` directory. Proxy, controller, and replica logs no longer appear in the console, worker files, or the **Actor Logs** section of the Ray dashboard. Set this variable to `1` to enable additional logging. |

To turn on throughput-optimized serving while customizing individual options, set `RAY_SERVE_THROUGHPUT_OPTIMIZED=1` and override the specific options. For example, to keep logging to stderr with throughput-optimized serving, set `RAY_SERVE_THROUGHPUT_OPTIMIZED=1` and also set `RAY_SERVE_LOG_TO_STDERR=1` to override the logging option.

(serve-haproxy)=
### Use HAProxy load balancing

By default, Ray Serve uses a Python-based HTTP and gRPC proxy to route requests to replicas. To improve throughput and lower latency at high request rates, replace it with [HAProxy](https://www.haproxy.org/), a C-based load balancer.

In HAProxy mode, an `HAProxyManager` actor runs on each node by default and translates Serve's routing table into HAProxy configuration reloads. Each ingress replica opens a port, and HAProxy routes traffic directly to replicas, replacing the Python proxy entirely. Live traffic flows through the HAProxy subprocess, not through any Python actor.

#### Prerequisites

HAProxy must be available on every node that runs a Serve proxy. On Linux, `pip install "ray[serve]"` installs the [`ray-haproxy`](https://pypi.org/project/ray-haproxy/) package, which ships a prebuilt HAProxy binary that Serve uses automatically. To use a different binary, set `RAY_SERVE_HAPROXY_BINARY_PATH` to its absolute path.

(enabling-haproxy)=
#### Enable HAProxy

Set the `RAY_SERVE_ENABLE_HA_PROXY` environment variable to `1` on all nodes before starting Ray:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
```

The following tabs show how to set the variable on each type of cluster.

::::{tab-set}

:::{tab-item} KubeRay
```yaml
# In the Ray container spec for head and worker groups:
env:
  - name: RAY_SERVE_ENABLE_HA_PROXY
    value: "1"
```
:::

:::{tab-item} VM cluster
```bash
# On every node (head and workers)
export RAY_SERVE_ENABLE_HA_PROXY=1
ray start --head  # or ray start --address=<head-ip>:6379 on workers
```
:::

::::

(installing-haproxy-manually-example)=
#### Example: Install HAProxy manually

Some platforms have no `ray-haproxy` wheel, such as non-glibc Linux, macOS, and Windows. On those platforms, install HAProxy 2.8 from source on every node. The following steps are only an example.

These steps are for Ubuntu or Debian:

```bash
# Install build dependencies
apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl libc6-dev \
    liblua5.3-dev libpcre3-dev libssl-dev zlib1g-dev

# Build HAProxy from source
export HAPROXY_VERSION="2.8.20"
curl -sSfL -o /tmp/haproxy.tar.gz \
  "https://www.haproxy.org/download/2.8/src/haproxy-${HAPROXY_VERSION}.tar.gz"
mkdir -p /tmp/haproxy-build && tar -xzf /tmp/haproxy.tar.gz -C /tmp/haproxy-build --strip-components=1
make -C /tmp/haproxy-build TARGET=linux-glibc \
  USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1 -j$(nproc)
make -C /tmp/haproxy-build install SBINDIR=/usr/local/bin
rm -rf /tmp/haproxy-build /tmp/haproxy.tar.gz

# Install runtime dependencies
apt-get install -y --no-install-recommends liblua5.3-0

# Create required directories
mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy
```

The required build flags are `USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1`. The runtime dependency is `liblua5.3-0`, the Lua runtime library.

(serve-interdeployment-grpc)=
(use-grpc-for-interdeployment-communication)=
### Use gRPC for inter-deployment communication

By default, when one deployment calls another through a `DeploymentHandle`, Serve sends requests through Ray's actor RPC system. To switch this internal transport to gRPC, set `RAY_SERVE_USE_GRPC_BY_DEFAULT=1` on all nodes before starting Ray. With this setting, all `DeploymentHandle` calls use gRPC transport, which serializes requests and sends them directly to the target replica's gRPC server. gRPC transport is most beneficial for high-throughput workloads with small payloads of less than about 1 MB, where bypassing Ray's object store reduces per-request overhead.

#### When not to use gRPC

Avoid gRPC transport in the following cases:

- Because gRPC must serialize every payload, don't use it for large payloads of more than about 1 MB. If you turned on gRPC by default, set an individual handle's transport back to actor RPC with `handle.options(_by_reference=True)`, which passes larger objects by reference.
- When you pass a `DeploymentResponse` from one deployment into another without awaiting it first, gRPC resolves the value at the caller and serializes it over the wire. Actor RPC forwards the underlying `ObjectRef` without materializing the data. If your pipeline chains `DeploymentResponse` objects through multiple deployments with payloads larger than 100 KB, avoid using gRPC for those handles.

```{literalinclude} ../doc_code/interdeployment_grpc.py
:start-after: __start_grpc_override__
:end-before: __end_grpc_override__
:language: python
```

(debugging-performance-issues-in-controller)=
## Debug performance issues in the controller

The Serve controller runs on the Ray head node and handles a variety of tasks, including receiving autoscaling metrics from other Ray Serve components. Symptoms of an overloaded controller might include high CPU usage and a large number of pending `ServeController.record_autoscaling_metrics_from_handle` tasks. If the controller becomes overloaded, you can tune the following environment variables:

- `RAY_SERVE_CONTROL_LOOP_INTERVAL_S`: The interval between cycles of the control loop. Defaults to `0.1` seconds. Increasing this value gives the controller more time to process requests and might help relieve overload.
- `RAY_SERVE_CONTROLLER_MAX_CONCURRENCY`: The maximum number of concurrent requests the controller can handle. Defaults to `15000`. The controller accepts one long-poll request per handle, so its concurrency needs scale with the number of handles. Increase this value if you have a large number of deployment handles.
- `RAY_SERVE_MAX_CACHED_HANDLES`: The maximum number of cached deployment handles. Defaults to `100`. Each handle maintains a long-poll connection to the controller, so limiting the cache size reduces controller overhead. Decrease this value if many handles are overloading the controller.

To get a quantitative view of controller health, send a GET request to the `/api/serve/applications/` endpoint and inspect the [`controller_health_metrics`](../api/doc/ray.serve.schema.ControllerHealthMetrics.rst) field on the response. The field exposes the following metrics:

- Rolling-window [`DurationStats`](../api/doc/ray.serve.schema.DurationStats.rst) statistics for the control-loop duration and for the update latencies of the deployment, application, proxy, and node state managers. Each reports the mean, standard deviation, minimum, and maximum.
- The event-loop delay, which compares the actual sleep time to the expected sleep time. Positive values indicate that the loop is falling behind.
- The count of pending `asyncio` tasks.
- The delay between when autoscaling metrics are generated and when they reach the controller.

A high `event_loop_delay_s` or a growing `loop_duration_s.mean` is the clearest signal that the controller is overloaded and that you should apply the tuning settings described earlier in this section.
