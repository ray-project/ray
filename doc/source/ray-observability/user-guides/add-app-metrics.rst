.. _application-level-metrics:

Adding Application-Level Metrics
--------------------------------

Ray provides a convenient API in :ref:`ray.util.metrics <custom-metric-api-ref>` for defining and exporting custom metrics for visibility into your applications.
Three metrics are supported: Counter, Gauge, and Histogram.
These metrics correspond to the same `Prometheus metric types <https://prometheus.io/docs/concepts/metric_types/>`_.
Below is a simple example of an Actor that exports metrics using these APIs:

.. literalinclude:: ../doc_code/metrics_example.py
   :language: python

While the script is running, the metrics are exported to ``localhost:8080`` (this is the endpoint that Prometheus would be configured to scrape).
Open this in the browser. You should see the following output:

.. code-block:: none

  # HELP ray_request_latency Latencies of requests in ms.
  # TYPE ray_request_latency histogram
  ray_request_latency_bucket{Component="core_worker",Version="3.0.0.dev0",actor_name="my_actor",le="0.1"} 2.0
  ray_request_latency_bucket{Component="core_worker",Version="3.0.0.dev0",actor_name="my_actor",le="1.0"} 2.0
  ray_request_latency_bucket{Component="core_worker",Version="3.0.0.dev0",actor_name="my_actor",le="+Inf"} 2.0
  ray_request_latency_count{Component="core_worker",Version="3.0.0.dev0",actor_name="my_actor"} 2.0
  ray_request_latency_sum{Component="core_worker",Version="3.0.0.dev0",actor_name="my_actor"} 0.11992454528808594
  # HELP ray_curr_count Current count held by the actor. Goes up and down.
  # TYPE ray_curr_count gauge
  ray_curr_count{Component="core_worker",Version="3.0.0.dev0",actor_name="my_actor"} -15.0
  # HELP ray_num_requests_total Number of requests processed by the actor.
  # TYPE ray_num_requests_total counter
  ray_num_requests_total{Component="core_worker",Version="3.0.0.dev0",actor_name="my_actor"} 2.0

Please see :ref:`ray.util.metrics <custom-metric-api-ref>` for more details.
