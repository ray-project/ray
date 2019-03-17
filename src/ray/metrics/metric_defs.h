// Use `RAY_METRIC(name, description, unit)` to define a metric
// which parameters are `metric_name`, `description` and `unit`.

static Metric TaskElapse = Metric::MakeHistogram("task_elapse",
                                                 "the task elapse in raylet",
                                                 "ms",
                                                 {0, 25, 50, 75, 100, 200},
                                                 {JobNameKey, NodeAddressKey});

static Metric RedisLatency = Metric::MakeHistogram("redis_latency",
                                                   "the latency of redis",
                                                   "ms",
                                                   {0, 25, 50, 75, 100, 200},
                                                   {JobNameKey, NodeAddressKey});


static Metric TaskCount = Metric::MakeGauge("task_count",
                                            "the task count that the raylet received",
                                            "pcs",
                                            {JobNameKey});

static Metric WorkerCount = Metric::MakeGauge("worker_count",
                                            "the worker count",
                                            "pcs",
                                            {JobNameKey, NodeAddressKey});
