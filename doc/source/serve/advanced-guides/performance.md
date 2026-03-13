(serve-perf-tuning)=
# Performance Tuning

This section should help you:

- understand Ray Serve's performance characteristics
- find ways to debug and tune your Serve application's performance

:::{note}
This section offers some tips and tricks to improve your Ray Serve application's performance. Check out the [architecture page](serve-architecture) for helpful context, including an overview of the HTTP proxy actor and deployment replica actors.
:::

```{contents}
```

## Performance and benchmarks

Ray Serve is built on top of Ray, so its scalability is bounded by Ray’s scalability. See Ray’s [scalability envelope](https://github.com/ray-project/ray/blob/master/release/benchmarks/README.md) to learn more about the maximum number of nodes and other limitations.

## Debugging performance issues in request path

The performance issue you're most likely to encounter is high latency or low throughput for requests.

Once you set up [monitoring](serve-monitoring) with Ray and Ray Serve, these issues may appear as:

* `serve_num_router_requests_total` staying constant while your load increases
* `serve_deployment_processing_latency_ms` spiking up as queries queue up in the background

The following are ways to address these issues:

1. Make sure you are using the right hardware and resources:
   * Are you reserving GPUs for your deployment replicas using `ray_actor_options` (e.g., `ray_actor_options={“num_gpus”: 1}`)?
   * Are you reserving one or more cores for your deployment replicas using `ray_actor_options` (e.g., `ray_actor_options={“num_cpus”: 2}`)?
   * Are you setting [OMP_NUM_THREADS](serve-omp-num-threads) to increase the performance of your deep learning framework?
2. Try batching your requests. See [Dynamic Request Batching](serve-performance-batching-requests).
3. Consider using `async` methods in your callable. See [the section below](serve-performance-async-methods).
4. Set an end-to-end timeout for your HTTP requests. See [the section below](serve-performance-e2e-timeout).


(serve-performance-async-methods)=
### Using `async` methods

:::{note}
According to the [FastAPI documentation](https://fastapi.tiangolo.com/async/#very-technical-details), `def` endpoint functions are called in a separate threadpool, so you might observe many requests running at the same time inside one replica, and this scenario might cause OOM or resource starvation. In this case, you can try to use `async def` to control the workload performance.
:::

Are you using `async def` in your callable? If you are using `asyncio` and
hitting the same queuing issue mentioned above, you might want to increase
`max_ongoing_requests`. By default, Serve sets this to a low value (5) to ensure clients receive proper backpressure.
You can increase the value in the deployment decorator; for example,
`@serve.deployment(max_ongoing_requests=1000)`.

(serve-performance-e2e-timeout)=
### Set an end-to-end request timeout

By default, Serve lets client HTTP requests run to completion no matter how long they take. However, slow requests could bottleneck the replica processing, blocking other requests that are waiting. Set an end-to-end timeout, so slow requests can be terminated and retried.

You can set an end-to-end timeout for HTTP requests by setting the `request_timeout_s` parameter
in the `http_options` field of the Serve config. HTTP Proxies wait for that many
seconds before terminating an HTTP request. This config is global to your Ray cluster,
and you can't update it during runtime. Use [client-side retries](serve-best-practices-http-requests)
to retry requests that time out due to transient failures.

:::{note}
Serve returns a response with status code `408` when a request times out. Clients can retry when they receive this `408` response.
:::


### Set backoff time when choosing replica

Ray Serve allows you to fine-tune the backoff behavior of the request router, which can help reduce latency when waiting for replicas to become ready. It uses exponential backoff strategy when retrying to route requests to replicas that are temporarily unavailable. You can optimize this behavior for your workload by configuring the following environment variables:

- `RAY_SERVE_ROUTER_RETRY_INITIAL_BACKOFF_S`: The initial backoff time (in seconds) before retrying a request. Default is `0.025`.
- `RAY_SERVE_ROUTER_RETRY_BACKOFF_MULTIPLIER`: The multiplier applied to the backoff time after each retry. Default is `2`.
- `RAY_SERVE_ROUTER_RETRY_MAX_BACKOFF_S`: The maximum backoff time (in seconds) between retries. Default is `0.5`.

### Configure locality-based routing

Ray Serve routes requests to replicas based on locality to reduce network latency. The system applies locality routing in two scenarios: proxy-to-replica communication (HTTP/gRPC requests) and inter-deployment communication (replica-to-replica calls through `DeploymentHandle`).

#### Routing priority

When locality routing is enabled, the system selects replicas in the following priority order:

1. **Same node**: Replicas running on the same node as the caller (lowest latency)
2. **Same availability zone**: Replicas in the same availability zone as the caller
3. **Any replica**: All available replicas (fallback when local replicas are busy)

If replicas at a higher priority level are busy or unavailable, the system automatically falls back to the next level.

#### Proxy-to-replica routing

You can configure proxy routing behavior through environment variables:

- `RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING`: When enabled, the proxy prefers routing requests to replicas on the same node. Default is `1` (enabled).
- `RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING`: When enabled, the proxy prefers routing requests to replicas in the same availability zone. Default is `1` (enabled).

#### Inter-deployment routing

When one deployment calls another through a `DeploymentHandle`, you can enable locality routing to reduce latency between deployments.

**Same-node routing**: By default, inter-deployment calls don't prefer same-node replicas. To enable same-node routing, initialize the handle with the `_prefer_local_routing` option:

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

**Same-AZ routing**: The `RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING` environment variable controls availability zone routing for both proxy and inter-deployment communication. You can't configure AZ routing per-handle.

(serve-high-throughput)=
### Enable throughput-optimized serving

:::{note}
In Ray v2.54.0, the defaults for `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD` and `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP` will change to `0` for improved performance.
:::

This section details how to enable Ray Serve options focused on improving throughput and reducing latency. These configurations focus on the following:

- Reducing overhead associated with frequent logging.
- Disabling behavior that allowed Serve applications to include blocking operations.

If your Ray Serve code includes thread blocking operations, you must refactor your code to achieve enhanced throughput. The following table shows examples of blocking and non-blocking code:

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

You can also configure each option individually. The following table details the recommended configurations and their impact:

| Configured value | Impact |
| --- | --- |
| `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD=0` | Your code runs in the same event loop as the replica's main event loop. You must avoid blocking operations in your request path. Set this configuration to `1` to run your code in a separate event loop, which protects the replica's ability to communicate with the Serve Controller if your code has blocking operations. |
| `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0`| The request router runs in the same event loop as the your code's event loop. You must avoid blocking operations in your request path. Set this configuration to `1` to run the router in a separate event loop, which protect Ray Serve's request routing ability when your code has blocking operations |
| `RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE=1000` | Sets the log buffer to batch writes to every `1000` logs, flushing the buffer on write. The system always flushes the buffer and writes logs when it detects a line with level ERROR.  Set the buffer size to `1` to disable buffering and write logs immediately. |
| `RAY_SERVE_LOG_TO_STDERR=0` | Only write logs to files under the `logs/serve/` directory. Proxy, Controller, and Replica logs no longer appear in the console, worker files, or the Actor Logs section of the Ray Dashboard. Set this property to `1` to enable additional logging. |

You may want to enable throughput-optimized serving while customizing the options above. You can do this by setting `RAY_SERVE_THROUGHPUT_OPTIMIZED=1` and overriding the specific options. For example, to enable throughput-optimized serving and continue logging to stderr, you should set `RAY_SERVE_THROUGHPUT_OPTIMIZED=1` and override with `RAY_SERVE_LOG_TO_STDERR=1`.

(serve-haproxy)=
### Use HAProxy load balancing

By default, Ray Serve uses a Python-based HTTP/gRPC proxy to route requests to replicas. You can replace this with [HAProxy](https://www.haproxy.org/), a high-performance C-based load balancer, for improved throughput and lower latency at high request rates.

When HAProxy mode is enabled:
- An `HAProxyManager` actor runs on each node (by default) and translates Serve's routing table into HAProxy configuration reloads.
- Each replica opens a direct ingress port, and HAProxy routes traffic directly to replicas — replacing the Python proxy entirely.
- Live traffic flows through the HAProxy subprocess, not through any Python actor.

#### Prerequisites

HAProxy must be installed and available on `$PATH` as `haproxy` on every node that runs a Serve proxy. The official Ray Docker images (2.55+) include HAProxy pre-built. No additional installation is needed when using `rayproject/ray` images.

#### Enabling HAProxy

Set the `RAY_SERVE_ENABLE_HA_PROXY` environment variable to `1` on all nodes **before** starting Ray:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
```

This single variable is all that is required. It implicitly enables direct ingress (replicas listen on individual ports that HAProxy routes to).

:::{note}
On multi-node clusters, you must also set `RAY_SERVE_DEFAULT_HTTP_HOST=0.0.0.0` so that replica direct ingress servers bind to all interfaces. Without this, replicas bind to `127.0.0.1` (the default) and HAProxy on other nodes cannot connect to them.
:::

::::{tab-set}

:::{tab-item} KubeRay
```yaml
# In the Ray container spec for head and worker groups:
env:
  - name: RAY_SERVE_ENABLE_HA_PROXY
    value: "1"
  - name: RAY_SERVE_DEFAULT_HTTP_HOST
    value: "0.0.0.0"
```
:::

:::{tab-item} VM cluster
```bash
# On every node (head and workers)
export RAY_SERVE_ENABLE_HA_PROXY=1
export RAY_SERVE_DEFAULT_HTTP_HOST=0.0.0.0
ray start --head  # or ray start --address=<head-ip>:6379 on workers
```
:::

::::

#### Installing HAProxy manually

If you are not using the official Ray Docker images, install HAProxy 2.8+ from source on every node. The following steps are for Ubuntu/Debian — adapt them to your OS:

```bash
# Install build dependencies
apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl libc6-dev \
    liblua5.3-dev libpcre3-dev libssl-dev zlib1g-dev

# Build HAProxy from source
export HAPROXY_VERSION="2.8.12"
curl -sSfL -o /tmp/haproxy.tar.gz \
  "https://www.haproxy.org/download/2.8/src/haproxy-${HAPROXY_VERSION}.tar.gz"
mkdir -p /tmp/haproxy-build && tar -xzf /tmp/haproxy.tar.gz -C /tmp/haproxy-build --strip-components=1
make -C /tmp/haproxy-build TARGET=linux-glibc \
  USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1 -j$(nproc)
make -C /tmp/haproxy-build install SBINDIR=/usr/local/bin
rm -rf /tmp/haproxy-build /tmp/haproxy.tar.gz

# Install runtime dependencies
apt-get install -y --no-install-recommends socat liblua5.3-0

# Create required directories
mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy
```

The required build flags are `USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1`. The runtime dependencies are `socat` (for the admin socket) and `liblua5.3-0` (Lua runtime library).

(serve-interdeployment-grpc)=
### Use gRPC for interdeployment communication

By default, when one deployment calls another via a `DeploymentHandle`, requests are sent through Ray's actor RPC system. You can switch this internal transport to gRPC by setting `RAY_SERVE_USE_GRPC_BY_DEFAULT=1` on all nodes before starting Ray. This makes all `DeploymentHandle` calls use gRPC transport, which serializes requests and sends them directly to the target replica's gRPC server. Individual handles can override with `handle.options(_by_reference=True)`. gRPC transport is most beneficial for high-throughput workloads with small payloads (under ~1 MB), where bypassing Ray's object store reduces per-request overhead.

## Debugging performance issues in controller

The Serve Controller runs on the Ray head node and is responsible for a variety of tasks,
including receiving autoscaling metrics from other Ray Serve components.
If the Serve Controller becomes overloaded
(symptoms might include high CPU usage and a large number of pending `ServeController.record_autoscaling_metrics_from_handle` tasks),
you can tune the following environment variables:

- `RAY_SERVE_CONTROL_LOOP_INTERVAL_S`: The interval between cycles of the control loop (defaults to `0.1` seconds). Increasing this value gives the Controller more time to process requests and may help alleviate overload.
- `RAY_SERVE_CONTROLLER_MAX_CONCURRENCY`: The maximum number of concurrent requests the Controller can handle (defaults to `15000`). The Controller accepts one long poll request per handle, so its concurrency needs scale with the number of handles. Increase this value if you have a large number of deployment handles.
- `RAY_SERVE_MAX_CACHED_HANDLES`: The maximum number of cached deployment handles (defaults to `100`). Each handle maintains a long poll connection to the controller, so limiting the cache size reduces controller overhead. Decrease this value if you're experiencing controller overload due to many handles.
