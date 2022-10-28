.. _ray-metrics:

Metrics
=======

To help monitor Ray applications, Ray

- Collects system-level metrics.
- Provides a default configuration for prometheus.
- Provides a default Grafana dashboard.
- Exposes metrics in a Prometheus format. We'll call the endpoint to access these metrics a Prometheus endpoint.
- Supports custom metrics APIs that resemble Prometheus `metric types <https://prometheus.io/docs/concepts/metric_types/>`_.

Getting Started
---------------

Ray exposes its metrics in Prometheus format. This allows us to easily scrape them using Prometheus.

First, `download Prometheus <https://prometheus.io/download/>`_. Make sure to download the correct binary for your operating system. (Ex: darwin for mac osx)

Then, unzip the the archive into a local directory using the following command.

.. code-block:: bash

    tar xvfz prometheus-*.tar.gz
    cd prometheus-*

Ray exports metrics only when ``ray[default]`` is installed.

.. code-block:: bash

  pip install "ray[default]"

Ray provides a prometheus config that works out of the box. After running ray, it can be found at `/tmp/ray/session_latest/metrics/prometheus/prometheus.yml`.

.. code-block:: yaml

    global:
      scrape_interval: 15s
      evaluation_interval: 15s

    scrape_configs:
    # Scrape from each ray node as defined in the service_discovery.json provided by ray.
    - job_name: 'ray'
      file_sd_configs:
      - files:
        - '/tmp/ray/prom_metrics_service_discovery.json'


Next, let's start Prometheus.

.. code-block:: shell

    ./prometheus --config.file=/tmp/ray/session_latest/metrics/prometheus/prometheus.yml

Now, you can access Ray metrics from the default Prometheus url, `http://localhost:9090`.

See :ref:`here <multi-node-metrics>` for more information on how to set up Prometheus on a Ray Cluster.

.. _grafana:

Grafana
-------
Grafana is a tool that supports more advanced visualizations of prometheus metrics and
allows you to create custom dashboards with your favorite metrics. Ray exports some default
configurations which includes a default dashboard showing some of the most valuable metrics
for debugging ray applications.

First, `download Grafana <https://grafana.com/grafana/download>`_. Follow the instructions on the download page to download the right binary for your operating system.

Then go to to the location of the binary and run grafana using the built in configuration found in `/tmp/ray/session_latest/metrics/grafana` folder.

.. code-block:: shell

    ./bin/grafana-server --config /tmp/ray/session_latest/metrics/grafana/grafana.ini web

Now, you can access grafana using the default grafana url, `http://localhost:3000`.
If this is your first time, you can login with the username: `admin` and password `admin`.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/grafana_login.png
    :align: center

You can then see the default dashboard by going to dashboards -> manage -> Ray -> Default Dashboard. The same metric graphs are also accessible via :ref:`Ray Dashboard <ray-dashboard>`.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/default_grafana_dashboard.png
    :align: center

.. _application-level-metrics:

Application-level Metrics
-------------------------
Ray provides a convenient API in :ref:`ray.util.metrics <custom-metric-api-ref>` for defining and exporting custom metrics for visibility into your applications.
There are currently three metrics supported: Counter, Gauge, and Histogram.
These metrics correspond to the same `Prometheus metric types <https://prometheus.io/docs/concepts/metric_types/>`_.
Below is a simple example of an actor that exports metrics using these APIs:

.. literalinclude:: doc_code/metrics_example.py
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

Customize prometheus export port
--------------------------------

Ray by default provides the service discovery file, but you can directly scrape metrics from prometheus ports.
To do that, you may want to customize the port that metrics gets exposed to a pre-defined port.

.. code-block:: bash

    ray start --head --metrics-export-port=8080 # Assign metrics export port on a head node.

Now, you can scrape Ray's metrics using Prometheus via ``<ip>:8080``.
