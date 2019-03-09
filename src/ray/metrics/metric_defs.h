// Use `RAY_METRIC(name, description, unit)` to define a metric
// which parameters are `metric_name`, `description` and `unit`.

static Metric RedisLatency("RedisLatency", "the latency of redis", "ms");

static Metric TaskElapse("TaskElapse", "the task elapse in raylet", "ms");

static Metric TaskCount("TaskCount", "the count that the raylet received", "pcs");
