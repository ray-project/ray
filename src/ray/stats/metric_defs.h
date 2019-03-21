#ifndef RAY_STATS_METRIC_DEFS_H_
#define RAY_STATS_METRIC_DEFS_H_

/// The definitions of metrics that you can use every where.
///
/// There are 4 types fo metric:
///   Histogram: Histogram distribution of metric points.
///   Gauge: Keeps the last recorded value, drops everything before.
///   Count: The count of the number of metric points.
///   Sum: A sum up of the metric points.
///
/// You can follow these examples to define your metrics.


//static Metric TaskElapse = Metric::MakeHistogram("task_elapse",
//                                                 "the task elapse in raylet",
//                                                 "ms",
//                                                 {0, 25, 50, 75, 100},
//                                                 {NodeAddressKey, CustomKey});
//
//static Metric RedisLatency = Metric::MakeHistogram("redis_latency",
//                                                   "the latency of redis",
//                                                   "ms",
//                                                   {0, 200, 400, 600, 800, 1000},
//                                                   {NodeAddressKey, CustomKey});
//
//
//static Metric TaskCount = Metric::MakeCount("task_count",
//                                            "the task count that the raylet received",
//                                            "pcs");

static Gauge WorkerCount("worker_count", "the worker count", "pcs", {CustomKey});

static Histogram RedisLatency("redis_latency", "the latency of redis", "ms", {0, 200, 400, 600, 800, 1000}, {NodeAddressKey, CustomKey});

#endif // RAY_STATS_METRIC_DEFS_H_
