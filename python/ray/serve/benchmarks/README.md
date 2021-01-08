## Ray Serve Benchmarks

This directory contains code that setup benchmark for Serve.

### `microbenchmark.py` runs several scenarios

```
{'max_batch_size': 1, 'max_concurrent_queries': 1, 'num_replicas': 1}:
        single client small data 629.38 +- 5.34 requests/s
        8 clients small data 888.99 +- 14.0 requests/s
{'max_batch_size': 1, 'max_concurrent_queries': 10000, 'num_replicas': 1}:
        single client small data 610.65 +- 11.99 requests/s
        8 clients small data 1856.55 +- 17.33 requests/s
{'max_batch_size': 10000, 'max_concurrent_queries': 10000, 'num_replicas': 1}:
        single client small data 602.93 +- 5.57 requests/s
        8 clients small data 1723.73 +- 88.33 requests/s
{'max_batch_size': 1, 'max_concurrent_queries': 1, 'num_replicas': 8}:
        single client small data 492.09 +- 6.11 requests/s
        8 clients small data 1662.08 +- 92.08 requests/s
{'max_batch_size': 1, 'max_concurrent_queries': 10000, 'num_replicas': 8}:
        single client small data 459.71 +- 25.66 requests/s
        8 clients small data 1860.39 +- 24.45 requests/s
{'max_batch_size': 10000, 'max_concurrent_queries': 10000, 'num_replicas': 8}:
        single client small data 487.65 +- 15.61 requests/s
        8 clients small data 1917.84 +- 95.61 requests/s
```

### `noop_latency.py` set up a noop backend for external benchmarks.

```
python noop_latency.py --blocking --num-replicas 8 --num-queries 500 --max-concurrent-queries 10000
```

- `--blocking` flags will blocks the server after firing `--num-queries` for warm up.
- `--num-replicas` and `--max-concurrent-queries` configures the backend replicas.

Once you setup deployment, external benchmark services like `wrk`, ApacheBench, or locust can be used. Example

```
$ wrk -c 100 -t 2 -d 10s --latency http://127.0.0.1:8000/noop
Running 10s test @ http://127.0.0.1:8000/noop
  2 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    44.14ms   11.24ms 151.03ms   94.78%
    Req/Sec     1.15k   185.88     1.36k    91.50%
  Latency Distribution
     50%   42.10ms
     75%   44.78ms
     90%   49.40ms
     99%  106.94ms
  22917 requests in 10.04s, 3.56MB read
Requests/sec:   2283.17
Transfer/sec:    363.43KB
```

Typically 100~200 connections should suffice to profile throughput.

### Use py-spy to generate flamegraphs

```
sudo env "PATH=$PATH" py-spy record --duration 30 -o out.svg --pid PID --native
```

Tips:

- If a process is overloaded, py-spy might not be able to find the Python stacks due to the heavy use of Cython extension
  in Ray. In that case, you can start py-spy first and then start the load generation.
