# Performance Tuning

This section should help you:

- understand the performance characteristics of Ray Serve
- find ways to debug and tune the performance of your Serve deployment

:::{note}
While this section offers some tips and tricks to improve the performance of your Serve deployment,
the [architecture doc](serve-architecture) is helpful to gain a deeper understanding of these contexts and parameters.
:::

```{contents}
```

## Performance and known benchmarks

We are continuously benchmarking Ray Serve. The metrics we care about are latency, throughput, and scalability. We can confidently say:

- Ray Serve’s latency overhead is single digit milliseconds, around 1-2 milliseconds on average.
- For throughput, Serve achieves about 3-4k queries per second on a single machine (8 cores) using 1 http proxy and 8 replicas performing noop requests.
- It is horizontally scalable so you can add more machines to increase the overall throughput. Ray Serve is built on top of Ray,
  so its scalability is bounded by Ray’s scalability. Please check out Ray’s [scalability envelope](https://github.com/ray-project/ray/blob/master/release/benchmarks/README.md)
  to learn more about the maximum number of nodes and other limitations.

You can check out our [microbenchmark instruction](https://github.com/ray-project/ray/blob/master/python/ray/serve/benchmarks/README.md)
to benchmark on your hardware.

## Debugging performance issues

The performance issue you're most likely to encounter is high latency and/or low throughput for requests.

If you have set up [monitoring](serve-monitoring) with Ray and Ray Serve, you will likely observe that
`serve_num_router_requests` is constant while your load increases
`serve_deployment_queuing_latency_ms` is spiking up as queries queue up in the background

Given the symptom, there are several ways to fix it.

### Choosing the right hardware

Make sure you are using the right hardware and resources.
Are you using GPUs (`actor_init_options={“num_gpus”: 1}`) or 1+ cores (`actor_init_options={“num_cpus”: 2}`, and setting `OMP_NUM_THREADS`)
to increase the performance of your deep learning framework?

### Async functions

Are you using `async def` in your callable? If you are using asyncio and
hitting the same queuing issue mentioned above, you might want to increase
`max_concurrent_queries`. Serve sets a low number by default so the client gets
proper backpressure. You can increase the value in the Deployment decorator.

### Batching

If your deployment can process a batch at a time at a sublinear latency
(for example, if it takes 1ms to process 1 query and 5ms to process 10 of them)
then batching is your best approach. Check out the [batching guide](serve-batching) to
make your deployment accept batches (especially for GPU-based ML inference). You might want to tune your `max_batch_size` and `batch_wait_timeout` in the `@serve.batch` decorator to maximize the benefits:

- `max_batch_size` specifies how big the batch should be. Generally,
  we recommend choosing the largest batch size your function can handle
  AND the performance improvement is no longer sublinear. Take a dummy
  example: suppose it takes 1ms to process 1 query, 5ms to process 10 queries,
  and 6ms to process 11 queries. Here you should set the batch size to to 10
  because adding more queries won’t improve the performance.
- `batch_wait_timeout` specifies how the maximum amount of time to wait before
  a batch should be processed, even if it’s not full.  It should be set according
  to `batch-wait-timeout + full batch processing time ~= expected latency`. The idea
  here is to have the first query wait for the longest possible time to achieve high throughput.
  This means you should set `batch_wait_timeout` as large as possible without exceeding your desired expected latency in the equation above.

### Scaling HTTP servers

Sometimes it’s not about your code: Serve’s HTTP server can become the bottleneck.
If you observe that the CPU utilization for HTTPProxy actor spike up to 100%, the HTTP server is the bottleneck.
Serve only starts a single HTTP server on the Ray head node by default.
This single HTTP server can handle about 3k queries per second.
If your workload exceeds this number, you might want to consider starting one
HTTP server per Ray node to spread the load by `serve.start(http_options={“location”: “EveryNode”})`.
This configuration tells Serve to spawn one HTTP server per node.
You should put an external load balancer in front of it.
