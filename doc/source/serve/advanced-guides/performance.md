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

## Debugging performance issues

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
`max_ongoing_requests`. Serve sets a low number (100) by default so the client gets
proper backpressure. You can increase the value in the deployment decorator; e.g.,
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

### Give the Serve Controller more time to process requests

The Serve Controller runs on the Ray head node and is responsible for a variety of tasks,
including receiving autoscaling metrics from other Ray Serve components.
If the Serve Controller becomes overloaded
(symptoms might include high CPU usage and a large number of pending `ServeController.record_handle_metrics` tasks),
you can increase the interval between cycles of the control loop
by setting the `RAY_SERVE_CONTROL_LOOP_INTERVAL_S` environment variable (defaults to `0.1` seconds).
This setting gives the Controller more time to process requests and may help alleviate the overload.
