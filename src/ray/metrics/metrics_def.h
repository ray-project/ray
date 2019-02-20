// Use `RAY_METRIC(name, description, unit)` to define a metric
// which parameters are `metric_name`, `description` and `unit`.

RAY_METRIC(RedisLatency, "the latency of redis", "ms");

RAY_METRIC(TaskElapse, "the task elapse in raylet", "pcs");

RAY_METRIC(TaskCount, "the count that the raylet received", "pcs");
