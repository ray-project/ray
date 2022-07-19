.. _ray-metrics:

Exporting Metrics
=================
To help monitoring Ray applications, Ray

- Collects Ray's pre-selected system level metrics.
- Exposes metrics in a Prometheus format. We'll call the endpoint to access these metrics a Prometheus endpoint.
- Support custom metrics APIs that resemble Prometheus `metric types <https://prometheus.io/docs/concepts/metric_types/>`_.

This page describes how to access these metrics using Prometheus.

.. note::

    It is currently an experimental feature and under active development. APIs are subject to change.

Getting Started (Single Node)
-----------------------------

First, install Ray with the proper dependencies:

.. code-block:: bash

  pip install "ray[default]"

Ray exposes its metrics in Prometheus format. This allows us to easily scrape them using Prometheus.

Let's expose metrics through `ray start`.

.. code-block:: bash

    ray start --head --metrics-export-port=8080 # Assign metrics export port on a head node.

Now, you can scrape Ray's metrics using Prometheus.

First, download Prometheus. `Download Link <https://prometheus.io/download/>`_

.. code-block:: bash

    tar xvfz prometheus-*.tar.gz
    cd prometheus-*

Let's modify Prometheus's config file to scrape metrics from Prometheus endpoints.

.. code-block:: yaml

    # prometheus.yml
    global:
      scrape_interval:     5s
      evaluation_interval: 5s

    scrape_configs:
      - job_name: prometheus
        static_configs:
        - targets: ['localhost:8080'] # This must be same as metrics_export_port

Next, let's start Prometheus.

.. code-block:: shell

    ./prometheus --config.file=./prometheus.yml

Now, you can access Ray metrics from the default Prometheus url, `http://localhost:9090`.

.. _multi-node-metrics:

Getting Started (Multi-nodes)
-----------------------------
Let's now walk through how to import metrics from a Ray cluster.

Ray runs a metrics agent per node. Each metrics agent collects metrics from a local node and exposes in a Prometheus format.
You can then scrape each endpoint to access Ray's metrics.

At a head node,

.. code-block:: bash

    ray start --head --metrics-export-port=8080 # Assign metrics export port on a head node.

At a worker node,

.. code-block:: bash

    ray start --address=[head_node_address] --metrics-export-port=8080

You can now get the url of metrics agents using `ray.nodes()`

.. code-block:: python

    # In a head node,
    import ray
    ray.init(address='auto')
    from pprint import pprint
    pprint(ray.nodes())

    """
    [{'Alive': True,
      'MetricsExportPort': 8080,
      'NodeID': '2f480984702a22556b90566bdac818a4a771e69a',
      'NodeManagerAddress': '192.168.1.82',
      'NodeManagerHostname': 'host2.attlocal.net',
      'NodeManagerPort': 61760,
      'ObjectManagerPort': 61454,
      'ObjectStoreSocketName': '/tmp/ray/session_2020-08-04_18-18-16_481195_34255/sockets/plasma_store',
      'RayletSocketName': '/tmp/ray/session_2020-08-04_18-18-16_481195_34255/sockets/raylet',
      'Resources': {'CPU': 1.0,
                    'memory': 123.0,
                    'node:192.168.1.82': 1.0,
                    'object_store_memory': 2.0},
      'alive': True},
    {'Alive': True,
     'MetricsExportPort': 8080,
     'NodeID': 'ce6f30a7e2ef58c8a6893b3df171bcd464b33c77',
     'NodeManagerAddress': '192.168.1.82',
     'NodeManagerHostname': 'host1.attlocal.net',
     'NodeManagerPort': 62052,
     'ObjectManagerPort': 61468,
     'ObjectStoreSocketName': '/tmp/ray/session_2020-08-04_18-18-16_481195_34255/sockets/plasma_store.1',
     'RayletSocketName': '/tmp/ray/session_2020-08-04_18-18-16_481195_34255/sockets/raylet.1',
     'Resources': {'CPU': 1.0,
                    'memory': 134.0,
                    'node:192.168.1.82': 1.0,
                    'object_store_memory': 2.0},
     'alive': True}]
    """

Now, setup your prometheus to read metrics from `[NodeManagerAddress]:[MetricsExportPort]` from all nodes in the cluster.
If you'd like to make this process automated, you can also use `file based service discovery <https://prometheus.io/docs/guides/file-sd/#installing-configuring-and-running-prometheus>`_.
This will allow Prometheus to dynamically find endpoints it should scrape (service discovery). You can easily get all endpoints using `ray.nodes()`

Getting Started (Cluster Launcher)
----------------------------------
When you use a Ray cluster launcher, it is common node IP addresses are changing because cluster is scaling up and down.
In this case, you can use Prometheus' `file based service discovery <https://prometheus.io/docs/guides/file-sd/#installing-configuring-and-running-prometheus>`_.

Prometheus Service Discovery Support
------------------------------------
Ray auto-generates a Prometheus `service discovery file <https://prometheus.io/docs/guides/file-sd/#installing-configuring-and-running-prometheus>`_ in a head node to help metrics agents' service discovery.
This allows you to easily scrape all metrics at each node in autoscaling clusters. Let's walkthrough how to acheive this.

The service discovery file is generated in a head node. Note that head node is a node where you started by `ray start --head` or ran `ray.init()`.

Inside a head node, check out a `temp_dir` of Ray. By default, it is `/tmp/ray` (in both Linux and MacOS). You should be able to find a file `prom_metrics_service_discovery.json`.
Ray periodically updates the addresses of all metrics agents in a cluster to this file.

Now, modify a Prometheus config to scrape the file for service discovery.

.. code-block:: yaml

    # Prometheus config file

    # my global config
    global:
      scrape_interval:     2s
      evaluation_interval: 2s

    # A scrape configuration containing exactly one endpoint to scrape:
    # Here it's Prometheus itself.
    scrape_configs:
    - job_name: 'ray'
      file_sd_configs:
      - files:
        - '/tmp/ray/prom_metrics_service_discovery.json'

Prometheus will automatically detect that the file contents are changing and update addresses it scrapes to based on the service discovery file generated by Ray.

.. _application-level-metrics:

Application-level Metrics
-------------------------
Ray provides a convenient API in :ref:`ray.util.metrics <custom-metric-api-ref>` for defining and exporting custom metrics for visibility into your applications.
There are currently three metrics supported: Counter, Gauge, and Histogram.
These metrics correspond to the same `Prometheus metric types <https://prometheus.io/docs/concepts/metric_types/>`_.
Below is a simple example of an actor that exports metrics using these APIs:

.. literalinclude:: /ray-core/doc_code/metrics_example.py
   :language: python

While the script is running, the metrics will be exported to ``localhost:8080`` (this is the endpoint that Prometheus would be configured to scrape).
If you open this in the browser, you should see the following output:

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
