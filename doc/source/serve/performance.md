# Performance Tuning

This section should help you:

- understand Ray Serve's performance characteristics
- find ways to debug and tune your Serve application's performance

:::{note}
This section offers some tips and tricks to improve your Ray Serve application's performance. Check out the [architecture page](serve-architecture) for helpful context, including an overview of the HTTP proxy actor and deployment replica actors.
:::

```{contents}
```

## Performance and known benchmarks

We are continuously benchmarking Ray Serve. The metrics we care about are latency, throughput, and scalability. We can confidently say:

- Ray Serve’s latency overhead is single digit milliseconds, around 1-2 milliseconds on average.
- For throughput, Serve achieves about 3-4k queries per second on a single machine (8 cores) using 1 HTTP proxy actor and 8 replicas performing no-op requests.
- It is horizontally scalable so you can add more machines to increase the overall throughput. Ray Serve is built on top of Ray,
  so its scalability is bounded by Ray’s scalability. Please see Ray’s [scalability envelope](https://github.com/ray-project/ray/blob/master/release/benchmarks/README.md)
  to learn more about the maximum number of nodes and other limitations.

We run long-running benchmarks nightly:

```{eval-rst}
.. list-table::
   :header-rows: 1

   * - Benchmark
     - Description
     - Cluster Details
     - Performance Numbers
   * - `Single Deployment <https://github.com/ray-project/ray/blob/de227ac407d6cad52a4ead09571eff6b1da73a6d/release/serve_tests/workloads/single_deployment_1k_noop_replica.py>`_
     - Runs 10 minute `wrk <https://github.com/wg/wrk>`_ trial on a single no-op deployment with 1000 replicas.
     - Head node: AWS EC2 m5.8xlarge. 32 worker nodes: AWS EC2 m5.8xlarge.
     - * per_thread_latency_avg_ms = 22.41
       * per_thread_latency_max_ms = 1400.0
       * per_thread_avg_tps = 55.75
       * per_thread_max_tps = 121.0
       * per_node_avg_tps = 553.17
       * per_node_avg_transfer_per_sec_KB = 83.19
       * cluster_total_thoughput = 10954456
       * cluster_total_transfer_KB = 1647441.9199999997
       * cluster_total_timeout_requests = 0
       * cluster_max_P50_latency_ms = 8.84
       * cluster_max_P75_latency_ms = 35.31
       * cluster_max_P90_latency_ms = 49.69
       * cluster_max_P99_latency_ms = 56.5
   * - `Multiple Deployments <https://github.com/ray-project/ray/blob/de227ac407d6cad52a4ead09571eff6b1da73a6d/release/serve_tests/workloads/multi_deployment_1k_noop_replica.py>`_
     - Runs 10 minute `wrk <https://github.com/wg/wrk>`_ trial on 10 deployments with 100 replicas each. Each deployment recursively sends queries to up to 5 other deployments.
     - Head node: AWS EC2 m5.8xlarge. 32 worker nodes: AWS EC2 m5.8xlarge.
     - * per_thread_latency_avg_ms = 0.0
       * per_thread_latency_max_ms = 0.0
       * per_thread_avg_tps = 0.0
       * per_thread_max_tps = 0.0
       * per_node_avg_tps = 0.35
       * per_node_avg_transfer_per_sec_KB = 0.05
       * cluster_total_thoughput = 6964
       * cluster_total_transfer_KB = 1047.28
       * cluster_total_timeout_requests = 6964.0
       * cluster_max_P50_latency_ms = 0.0
       * cluster_max_P75_latency_ms = 0.0
       * cluster_max_P90_latency_ms = 0.0
       * cluster_max_P99_latency_ms = 0.0
   * - `Deployment Graph: Ensemble <https://github.com/ray-project/ray/blob/f6735f90c72581baf83a9cea7cbbe3ea2f6a56d8/release/serve_tests/workloads/deployment_graph_wide_ensemble.py>`_
     - Runs 10 node ensemble, constructed with a call graph, that performs basic arithmetic at each node. Ensemble pattern routes the input to 10 different nodes, and their outputs are combined to produce the final output. Simulates 4 clients making 20 requests each.
     - Head node: AWS EC2 m5.8xlarge. 0 Worker nodes.
     - * throughput_mean_tps = 8.75
       * throughput_std_tps = 0.43
       * latency_mean_ms = 126.15
       * latency_std_ms = 18.35
```

:::{note}
The performance numbers above come from a recent run of the nightly benchmarks.
:::

<!--- See https://github.com/ray-project/ray/pull/27711 for more context on the benchmarks. -->

Check out [our benchmark workloads'](https://github.com/ray-project/ray/tree/f6735f90c72581baf83a9cea7cbbe3ea2f6a56d8/release/serve_tests/workloads) source code directly to get a better sense of what they test. You can see which cluster templates each benchmark uses [here](https://github.com/ray-project/ray/blob/8eca6ae852e2d23bcf49680fef6f0384a1b63564/release/release_tests.yaml#L2328-L2576) (under the `cluster_compute` key), and you can see what type of nodes each template spins up [here](https://github.com/ray-project/ray/tree/8beb887bbed31ecea3d2813b61833b81c45712e1/release/serve_tests).

You can check out our [microbenchmark instructions](https://github.com/ray-project/ray/blob/master/python/ray/serve/benchmarks/README.md)
to benchmark Ray Serve on your hardware.

(serve-performance-batching-requests)=

## Request Batching

Serve offers a request batching feature that can improve your service throughput without sacrificing latency. This is possible because ML models can utilize efficient vectorized computation to process a batch of request at a time. Batching is also necessary when your model is expensive to use and you want to maximize the utilization of hardware.

Machine Learning (ML) frameworks such as Tensorflow, PyTorch, and Scikit-Learn support evaluating multiple samples at the same time.
Ray Serve allows you to take advantage of this feature via dynamic request batching.
When a request arrives, Serve puts the request in a queue. This queue buffers the requests to form a batch. The deployment picks up the batch and evaluates it. After the evaluation, the resulting batch will be split up, and each response is returned individually.

### Enable batching for your deployment
You can enable batching by using the {mod}`ray.serve.batch` decorator. Let's take a look at a simple example by modifying the `MyModel` class to accept a batch.
```{literalinclude} doc_code/batching_guide.py
---
start-after: __single_sample_begin__
end-before: __single_sample_end__
---
```

The batching decorators expect you to make the following changes in your method signature:
- The method is declared as an async method because the decorator batches in asyncio event loop.
- The method accepts a list of its original input types as input. For example, `arg1: int, arg2: str` should be changed to `arg1: List[int], arg2: List[str]`.
- The method returns a list. The length of the return list and the input list must be of equal lengths for the decorator to split the output evenly and return a corresponding response back to its respective request.

```{literalinclude} doc_code/batching_guide.py
---
start-after: __batch_begin__
end-before: __batch_end__
emphasize-lines: 6-9
---
```

You can supply two optional parameters to the decorators.
- `batch_wait_timeout_s` controls how long Serve should wait for a batch once the first request arrives.
- `max_batch_size` controls the size of the batch.
Once the first request arrives, the batching decorator will wait for a full batch (up to `max_batch_size`) until `batch_wait_timeout_s` is reached. If the timeout is reached, the batch will be sent to the model regardless the batch size.

### Tips for fine-tuning batching parameters

`max_batch_size` ideally should be a power of 2 (2, 4, 8, 16, ...) because CPUs and GPUs are both optimized for data of these shapes. Large batch sizes incur a high memory cost as well as latency penalty for the first few requests.

`batch_wait_timeout_s` should be set considering the end to end latency SLO (Service Level Objective). For example, if your latency target is 150ms, and the model takes 100ms to evaluate the batch, the `batch_wait_timeout_s` should be set to a value much lower than 150ms - 100ms = 50ms.

When using batching in a Serve Deployment Graph, the relationship between an upstream node and a downstream node might affect the performance as well. Consider a chain of two models where first model sets `max_batch_size=8` and second model sets `max_batch_size=6`. In this scenario, when the first model finishes a full batch of 8, the second model will finish one batch of 6 and then to fill the next batch, which will initially only be partially filled with 8 - 6 = 2 requests, incurring latency costs. The batch size of downstream models should ideally be multiples or divisors of the upstream models to ensure the batches play well together.

## Debugging performance issues

The performance issue you're most likely to encounter is high latency and/or low throughput for requests.

Once you set up [monitoring](serve-monitoring) with Ray and Ray Serve, these issues may appear as:

* `serve_num_router_requests` staying constant while your load increases
* `serve_deployment_processing_latency_ms` spiking up as queries queue up in the background

There are handful of ways to address these issues:

1. Make sure you are using the right hardware and resources:
   * Are you reserving GPUs for your deployment replicas using `ray_actor_options` (e.g. `ray_actor_options={“num_gpus”: 1}`)?
   * Are you reserving one or more cores for your deployment replicas using `ray_actor_options` (e.g. `ray_actor_options={“num_cpus”: 2}`)?
   * Are you setting [OMP_NUM_THREADS](serve-omp-num-threads) to increase the performance of your deep learning framework?
2. Try batching your requests. See [the section above](serve-performance-batching-requests).
3. Consider using `async` methods in your callable. See [the section below](serve-performance-async-methods).
4. Set an end-to-end timeout for your HTTP requests. See [the section below](serve-performance-e2e-timeout).


(serve-performance-async-methods)=
### Using `async` methods

:::{note}
According to the [FastAPI documentation](https://fastapi.tiangolo.com/async/#very-technical-details), `def` endpoint functions will be called in a separate threadpool, so you might observe many requests running at the same time inside one replica, and this scenario might cause OOM or resource starvation. In this case, you can try to use `async def` to control the workload performance.
:::

Are you using `async def` in your callable? If you are using `asyncio` and
hitting the same queuing issue mentioned above, you might want to increase
`max_concurrent_queries`. Serve sets a low number (100) by default so the client gets
proper backpressure. You can increase the value in the deployment decorator; e.g.
`@serve.deployment(max_concurrent_queries=1000)`.

(serve-performance-e2e-timeout)=
### Set an end-to-end request timeout

By default, Serve lets client HTTP requests run to completion no matter how long they take. Slow requests could bottleneck the replica processing them while other requests wait in a queue. It's recommended that you set an end-to-end timeout, so slow requests can be terminated and retried at another replica.

You can set an end-to-end timeout for HTTP requests by setting the `SERVE_REQUEST_PROCESSING_TIMEOUT_S` environment variable. HTTP Proxies will wait for that many seconds before terminating an HTTP request and retrying it at another replica. This environment variable should be set on every node in your Ray cluster, and it cannot be updated during runtime.
