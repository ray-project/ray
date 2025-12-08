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

## Debugging performance issues in controller

The Serve Controller runs on the Ray head node and is responsible for a variety of tasks,
including receiving autoscaling metrics from other Ray Serve components.
If the Serve Controller becomes overloaded
(symptoms might include high CPU usage and a large number of pending `ServeController.record_autoscaling_metrics_from_handle` tasks),
you can increase the interval between cycles of the control loop
by setting the `RAY_SERVE_CONTROL_LOOP_INTERVAL_S` environment variable (defaults to `0.1` seconds).
This setting gives the Controller more time to process requests and may help alleviate the overload.
