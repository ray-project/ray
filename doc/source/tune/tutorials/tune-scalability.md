---
orphan: true
myst:
  html_meta:
    description: "Benchmark results for Ray Tune's overhead and throughput at scale, covering result throughput and thousands of concurrent trials."
---

# Ray Tune scalability and overhead benchmarks

The Ray project ran a series of micro-benchmarks to evaluate the scalability of Ray Tune and analyze its performance overhead. The Tune documentation draws on these results, for example, in its suggestions on {ref}`how to remove performance bottlenecks <tune-bottlenecks>`.

This page gives an overview of these benchmarks. For each benchmark, the goal was to examine the total runtime of the experiment and to address issues when the observed overhead over the minimal theoretical time was too high, for example, more than 20%.

Some benchmarks change the default settings for maximum throughput, for example, by disabling trial synchronization or result logging. Each benchmark description states when it does so.

```{list-table} Ray Tune scalability benchmarks overview
:header-rows: 1

* - Variable
  - Number of trials
  - Results per second per trial
  - Number of nodes
  - CPUs per node
  - Trial length (s)
  - Observed runtime
* - [Trial bookkeeping and scheduling overhead](https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_bookkeeping_overhead.py)
  - 10,000
  - 1
  - 1
  - 16
  - 1
  - 715.27\
    Minimum: 625
* - [Result throughput with many trials](https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_result_throughput_cluster.py)
  - 1,000
  - 0.1
  - 16
  - 64
  - 100
  - 168.18
* - [Result throughput with many results](https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_result_throughput_single_node.py)
  - 96
  - 10
  - 1
  - 96
  - 100
  - 168.94
* - [Network communication overhead](https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_network_overhead.py)
  - 200
  - 1
  - 200
  - 2
  - 300
  - 2280.82
* - [Long-running, 3.75 GB checkpoints](https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_long_running_large_checkpoints.py)
  - 16
  - Results: 1/60\
    Checkpoint: 1/900
  - 1
  - 16
  - 86,400
  - 88687.41
* - [Durable trainable](https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_durable_trainable.py)
  - 16
  - 10/60\
    with 10 MB checkpoints
  - 16
  - 2
  - 300
  - 392.42
```

The following sections discuss some of the results that showed high overhead.

## Result throughput

Result throughput is the number of results Ray Tune can process in a given time frame, such as results per second. The higher the throughput, the more concurrent results Ray Tune can process without major delays.

The time it takes to process results limits result throughput. When a trial reports results, it continues training only after the trial executor re-triggers the remote training function. If many trials report results at the same time, each subsequent remote training call is triggered only after that trial's results are handled.

To speed up the process, Ray Tune adaptively buffers results, so trials continue training earlier when many trials run in parallel and report many results at the same time. Still, processing hundreds of results per trial for dozens or hundreds of trials can become a bottleneck.

:::{tip}
Ray Tune warns you when trial processing becomes a bottleneck. If this becomes a problem, follow the guidelines in {ref}`the FAQ <tune-bottlenecks>`. Generally, avoid reporting too many results at the same time. Consider increasing the report intervals by a factor of 5 to 10.
:::

The following sections present more detailed results on result throughput.

### Benchmarking many concurrent Tune trials

This setup disables the CSV, JSON, and TensorBoardX loggers and trial synchronization, except where noted.

This benchmark runs up to 1,000 concurrent trials on a cluster. It then adjusts the reporting frequency of the trials, measured in results per second, to find the throughput limits.

With logging and synchronization disabled, the threshold for acceptable performance seems to be around 500 total results per second. With logging enabled, Ray Tune can still manage around 50 to 100 results per second without too much overhead. Beyond that, consider measures to decrease the number of incoming results.

```{list-table}
:header-rows: 1

* - Number of trials
  - Results per second per trial
  - Number of nodes
  - CPUs per node
  - Trial length
  - Observed runtime
* - 1,000
  - 10
  - 16
  - 64
  - 100s
  - 248.39
* - 1,000
  - 1
  - 16
  - 64
  - 100s
  - 175.00
* - 1,000
  - 0.1 with logging
  - 16
  - 64
  - 100s
  - 168.18
* - 384
  - 10
  - 16
  - 64
  - 100s
  - 125.17
* - 256
  - 50
  - 16
  - 64
  - 100s
  - 307.02
* - 256
  - 20
  - 16
  - 64
  - 100s
  - 146.20
* - 256
  - 10
  - 16
  - 64
  - 100s
  - 113.40
* - 256
  - 10 with logging
  - 16
  - 64
  - 100s
  - 436.12
* - 256
  - 0.1 with logging
  - 16
  - 64
  - 100s
  - 106.75
```

### Benchmarking many Tune results on a single node

This setup disables the CSV, JSON, and TensorBoardX loggers, except where noted.

This benchmark runs 96 concurrent trials on a single node. It then adjusts the reporting frequency of the trials, measured in results per second, to find the throughput limits. Because fewer trials run in parallel than in the cluster benchmark, the trials report much more often.

On a single node, throughput seems to be a bit higher. With logging, handling 1,000 results per second seems acceptable in terms of overhead, though you should probably still target a lower number.

```{list-table}
:header-rows: 1

* - Number of trials
  - Results per second per trial
  - Number of nodes
  - CPUs per node
  - Trial length
  - Observed runtime
* - 96
  - 500
  - 1
  - 96
  - 100s
  - 959.32
* - 96
  - 100
  - 1
  - 96
  - 100s
  - 219.48
* - 96
  - 80
  - 1
  - 96
  - 100s
  - 197.15
* - 96
  - 50
  - 1
  - 96
  - 100s
  - 110.55
* - 96
  - 50 with logging
  - 1
  - 96
  - 100s
  - 702.64
* - 96
  - 10
  - 1
  - 96
  - 100s
  - 103.51
* - 96
  - 10 with logging
  - 1
  - 96
  - 100s
  - 168.94
```

## Network overhead in Ray Tune

Running Ray Tune in a distributed setup adds network communication overhead. Most of this overhead comes from trial synchronization, which periodically synchronizes results and checkpoints and sends them through the network. By default, synchronization uses SSH, where each connection initialization can take between 1 and 2 seconds. Because this blocking operation happens per trial, this synchronization quickly becomes a bottleneck when you run many concurrent trials.

This benchmark runs a number of trials on a cluster, each on a separate node. It varies the number of concurrent trials and nodes to see how much network communication affects total runtime.

:::{tip}
When you run many concurrent trials in a distributed setup, consider using {ref}`cloud checkpointing <tune-cloud-checkpointing>` for checkpoint synchronization instead. Another option is to use shared storage and disable syncing to the driver. The {ref}`best practices for Kubernetes setups <tune-kubernetes>` apply to any kind of setup.
:::

The following table shows more detailed results on the network communication overhead.

```{list-table}
:header-rows: 1

* - Number of trials
  - Results per second per trial
  - Number of nodes
  - CPUs per node
  - Trial length
  - Observed runtime
* - 200
  - 1
  - 200
  - 2
  - 300s
  - 2280.82
* - 100
  - 1
  - 100
  - 2
  - 300s
  - 1470
* - 100
  - 0.01
  - 100
  - 2
  - 300s
  - 473.41
* - 50
  - 1
  - 50
  - 2
  - 300s
  - 474.30
* - 50
  - 0.1
  - 50
  - 2
  - 300s
  - 441.54
* - 10
  - 1
  - 10
  - 2
  - 300s
  - 334.37
```
