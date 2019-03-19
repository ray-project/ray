#ifndef RAY_METRIC_DEFS_H_
#define RAY_METRIC_DEFS_H_

/// Use `RAY_METRIC(name, description, unit)` to define a metric
/// which parameters are `metric_name`, `description` and `unit`.

static Metric TaskElapse = Metric::MakeHistogram("task_elapse",
                                                 "the task elapse in raylet",
                                                 "ms",
                                                 {0, 25, 50, 75, 100},
                                                 {NodeAddressKey, CustomKey});

static Metric RedisLatency = Metric::MakeHistogram("redis_latency",
                                                   "the latency of redis",
                                                   "ms",
                                                   {0, 200, 400, 600, 800, 1000},
                                                   {NodeAddressKey, CustomKey});


static Metric TaskCount = Metric::MakeCount("task_count",
                                            "the task count that the raylet received",
                                            "pcs");

static Metric WorkerCount = Metric::MakeGauge("worker_count",
                                            "the worker count",
                                            "pcs",
                                            {CustomKey});

#endif // RAY_METRIC_DEFS_H_
