:orphan:

Scalability and Overhead Benchmarks
===================================

We conducted a series of micro-benchmarks where we evaluated the scalability of Ray Tune and analyzed the
performance overhead we observed. The results from these benchmarks are reflected in the documentation,
e.g. when we make suggestions on :ref:`how to remove performance bottlenecks <tune-bottlenecks>`.

This page gives an overview over the experiments we did. For each of these experiments, the goal was to
examine the total runtime of the experiment and address issues when the observed overhead compared to the
minimal theoretical time was too high (e.g. more than 20% overhead).

In some of the experiments we tweaked the default settings for maximum throughput, e.g. by disabling
trial synchronization or result logging. If this is the case, this is stated in the respective benchmark
description.


.. list-table:: Ray Tune scalability benchmarks overview
   :header-rows: 1

   * - Variable
     - # of trials
     - Results/second /trial
     - # of nodes
     - # CPUs/node
     - Trial length (s)
     - Observed runtime
   * - `Trial bookkeeping /scheduling overhead <https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_bookkeeping_overhead.py>`_
     - 10,000
     - 1
     - 1
     - 16
     - 1
     - | 715.27
       | (625 minimum)
   * - `Result throughput (many trials) <https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_result_throughput_cluster.py>`_
     - 1,000
     - 0.1
     - 16
     - 64
     - 100
     - 168.18
   * - `Result throughput (many results) <https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_result_throughput_single_node.py>`_
     - 96
     - 10
     - 1
     - 96
     - 100
     - 168.94
   * - `Network communication overhead <https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_network_overhead.py>`_
     - 200
     - 1
     - 200
     - 2
     - 300
     - 2280.82
   * - `Long running, 3.75 GB checkpoints <https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_long_running_large_checkpoints.py>`_
     - 16
     - | Results: 1/60
       | Checkpoint: 1/900
     - 1
     - 16
     - 86,400
     - 88687.41
   * - `XGBoost parameter sweep <https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_xgboost_sweep.py>`_
     - 16
     - ?
     - 16
     - 64
     - ?
     - 3903
   * - `Durable trainable <https://github.com/ray-project/ray/blob/master/release/tune_tests/scalability_tests/workloads/test_durable_trainable.py>`_
     - 16
     - | 10/60
       | with 10MB CP
     - 16
     - 2
     - 300
     - 392.42


Below we discuss some insights on results where we observed much overhead.


Result throughput
-----------------
Result throughput describes the number of results Ray Tune can process in a given timeframe (e.g.
"results per second").
The higher the throughput, the more concurrent results can be processed without major delays.

Result throughput is limited by the time it takes to process results. When a trial reports results, it only
continues training once the trial executor re-triggered the remote training function. If many trials report
results at the same time, each subsequent remote training call is only triggered after handling that trial's
results.

To speed the process up, Ray Tune adaptively buffers results, so that trial training is continued earlier if
many trials are running in parallel and report many results at the same time. Still, processing hundreds of
results per trial for dozens or hundreds of trials can become a bottleneck.

**Main insight**: Ray Tune will throw a warning when trial processing becomes a bottleneck. If you notice
that this becomes a problem, please follow our guidelines outlined :ref:`in the FAQ <tune-bottlenecks>`.
Generally, it is advised to not report too many results at the same time. Consider increasing the report
intervals by a factor of 5-10x.

Below we present more detailed results on the result throughput performance.

Many concurrent trials
""""""""""""""""""""""
In this setup, loggers (CSV, JSON, and TensorBoardX) and trial synchronization are disabled, except when
explicitly noted.

In this experiment, we're running many concurrent trials (up to 1,000) on a cluster. We then adjust the
reporting frequency (number of results per second) of the trials to measure the throughput limits.

It seems that around 500 total results/second seem to be the threshold for acceptable performance
when logging and synchronization are disabled. With logging enabled, around 50-100 results per second
can still be managed without too much overhead, but after that measures to decrease incoming results
should be considered.

+-------------+--------------------------+---------+---------------+------------------+---------+
| # of trials | Results / second / trial | # Nodes | # CPUs / Node | Length of trial. | Current |
+=============+==========================+=========+===============+==================+=========+
| 1,000       | 10                       | 16      | 64            | 100s             | 248.39  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 1,000       | 1                        | 16      | 64            | 100s             | 175.00  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 1,000       | 0.1 with logging         | 16      | 64            | 100s             | 168.18  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 384         | 10                       | 16      | 64            | 100s             | 125.17  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 256         | 50                       | 16      | 64            | 100s             | 307.02  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 256         | 20                       | 16      | 64            | 100s             | 146.20  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 256         | 10                       | 16      | 64            | 100s             | 113.40  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 256         | 10 with logging          | 16      | 64            | 100s             | 436.12  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 256         | 0.1 with logging         | 16      | 64            | 100s             | 106.75  |
+-------------+--------------------------+---------+---------------+------------------+---------+


Many results on a single node
"""""""""""""""""""""""""""""
In this setup, loggers (CSV, JSON, and TensorBoardX) are disabled, except when
explicitly noted.

In this experiment, we're running 96 concurrent trials on a single node. We then adjust the
reporting frequency (number of results per second) of the trials to find the throughput limits.
Compared to the cluster experiment setup, we report much more often, as we're running less total trials in parallel.

On a single node, throughput seems to be a bit higher. With logging, handling 1000 results per second
seems acceptable in terms of overhead, though you should probably still target for a lower number.

+-------------+--------------------------+---------+---------------+------------------+---------+
| # of trials | Results / second / trial | # Nodes | # CPUs / Node | Length of trial. | Current |
+=============+==========================+=========+===============+==================+=========+
| 96          | 500                      | 1       | 96            | 100s             | 959.32  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 96          | 100                      | 1       | 96            | 100s             | 219.48  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 96          | 80                       | 1       | 96            | 100s             | 197.15  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 96          | 50                       | 1       | 96            | 100s             | 110.55  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 96          | 50 with logging          | 1       | 96            | 100s             | 702.64  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 96          | 10                       | 1       | 96            | 100s             | 103.51  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 96          | 10 with logging          | 1       | 96            | 100s             | 168.94  |
+-------------+--------------------------+---------+---------------+------------------+---------+


Network overhead
----------------
Running Ray Tune on a distributed setup leads to network communication overhead. This is mostly due to
trial synchronization, where results and checkpoints are periodically synchronized and sent via the network.
Per default this happens via SSH, where connnection initialization can take between 1 and 2 seconds each time.
Since this is a blocking operation that happens on a per-trial basis, running many concurrent trials
quickly becomes bottlenecked by this synchronization.

In this experiment, we ran a number of trials on a cluster. Each trial was run on a separate node. We
varied the number of concurrent trials (and nodes) to see how much network communication affects
total runtime.

**Main insight**: When running many concurrent trials in a distributed setup, consider using
:ref:`cloud checkpointing <tune-cloud-checkpointing>` for checkpoint synchronization instead. Another option would
be to use a shared storage and disable syncing to driver. The best practices are described
:ref:`here for Kubernetes setups <tune-kubernetes>` but is applicable for any kind of setup.


In the table below we present more detailed results on the network communication overhead.

+-------------+--------------------------+---------+---------------+------------------+---------+
| # of trials | Results / second / trial | # Nodes | # CPUs / Node | Length of trial  | Current |
+=============+==========================+=========+===============+==================+=========+
| 200         | 1                        | 200     | 2             | 300s             | 2280.82 |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 100         | 1                        | 100     | 2             | 300s             | 1470    |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 100         | 0.01                     | 100     | 2             | 300s             | 473.41  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 50          | 1                        | 50      | 2             | 300s             | 474.30  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 50          | 0.1                      | 50      | 2             | 300s             | 441.54  |
+-------------+--------------------------+---------+---------------+------------------+---------+
| 10          | 1                        | 10      | 2             | 300s             | 334.37  |
+-------------+--------------------------+---------+---------------+------------------+---------+
