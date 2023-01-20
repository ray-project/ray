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

.. note::
    If you are using mac, you may receive an error at this point about trying to launch an application where the developer has not been verified. See :ref:`this link <unverified-developer>` to fix the issue.

Now, you can access Ray metrics from the default Prometheus url, `http://localhost:9090`.

See :ref:`here <multi-node-metrics>` for more information on how to set up Prometheus on a Ray Cluster.

.. _grafana:

Grafana
-------
Grafana is a tool that supports more advanced visualizations of prometheus metrics and
allows you to create custom dashboards with your favorite metrics. Ray exports some default
configurations which includes a default dashboard showing some of the most valuable metrics
for debugging ray applications.


Deploying Grafana
~~~~~~~~~~~~~~~~~

First, `download Grafana <https://grafana.com/grafana/download>`_. Follow the instructions on the download page to download the right binary for your operating system.

Then go to to the location of the binary and run grafana using the built in configuration found in `/tmp/ray/session_latest/metrics/grafana` folder.

.. code-block:: shell

    ./bin/grafana-server --config /tmp/ray/session_latest/metrics/grafana/grafana.ini web

Now, you can access grafana using the default grafana url, `http://localhost:3000`.
You can then see the default dashboard by going to dashboards -> manage -> Ray -> Default Dashboard. The same :ref:`metric graphs <system-metrics>` are also accessible via :ref:`Ray Dashboard <ray-dashboard>`.

.. tip::

  If this is your first time using Grafana, you can login with the username: `admin` and password `admin`.

.. image:: images/graphs.png
    :align: center

Using an existing Grafana instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you want to use existing Grafana instance, before starting your Ray cluster you will need to setup environment variable `RAY_GRAFANA_HOST` with an URL of your Grafana. After starting Ray, you can find Grafana dashboard json at `/tmp/ray/session_latest/metrics/grafana/dashboards/default_grafana_dashboard.json`. `Import this dashboard <https://grafana.com/docs/grafana/latest/dashboards/manage-dashboards/#import-a-dashboard>`_ to your Grafana.

If Grafana reports that datasource is not found, you can `add a datasource variable <https://grafana.com/docs/grafana/latest/dashboards/variables/add-template-variables/?pg=graf&plcmt=data-sources-prometheus-btn-1#add-a-data-source-variable>`_ and using `JSON model view <https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/modify-dashboard-settings/#view-dashboard-json-model>`_ change all values of `datasource` key in the imported `default_grafana_dashboard.json` to the name of the variable. For example, if the variable name is `data_source`, all `"datasource"` mappings should be:

.. code-block:: json

  "datasource": {
    "type": "prometheus",
    "uid": "$data_source"
  }

When existing Grafana instance requires user authentication, the following settings have to be in its `configuration file <https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/>`_ to correctly embed in Ray dashboard:

.. code-block:: ini

  [security]
  allow_embedding = true
  cookie_secure = true
  cookie_samesite = none

If Grafana is exposed via nginx ingress on Kubernetes cluster, the following line should be present in the Grafana ingress annotation:

.. code-block:: yaml

  nginx.ingress.kubernetes.io/configuration-snippet: |
      add_header X-Frame-Options SAMEORIGIN always;

When both Grafana and Ray cluster are on the same Kubernetes cluster, it is important to set `RAY_GRAFANA_HOST` to the external URL of the Grafana ingress. For successful embedding, `RAY_GRAFANA_HOST` needs to be accessible to both Ray cluster backend and Ray dashboard frontend:

* On the backend, *Ray cluster head* does health checks on Grafana. Hence `RAY_GRAFANA_HOST` needs to be accessible in the Kubernetes pod which is running the head node.
* When accessing *Ray dashboard* from the browser, frontend embeds Grafana dashboard using the URL specified in `RAY_GRAFANA_HOST`. Hence `RAY_GRAFANA_HOST` needs to be accessible from the browser as well.

.. _system-metrics:

System Metrics
--------------
Ray exports a number of system metrics, which provide introspection into the state of Ray workloads, as well as hardware utilization statistics. The following table describes the officially supported metrics:

.. note::

   Certain labels are common across all metrics, such as `SessionName` (uniquely identifies a Ray cluster instance), `instance` (per-node label applied by Prometheus, and `JobId` (Ray job id, as applicable).

.. list-table:: Ray System Metrics
   :header-rows: 1

   * - Prometheus Metric
     - Labels
     - Description
   * - `ray_tasks`
     - `Name`, `State`, `IsRetry`
     - Current number of tasks (both remote functions and actor calls) by state. The State label (e.g., RUNNING, FINISHED, FAILED) describes the state of the task. See `rpc::TaskState <https://github.com/ray-project/ray/blob/e85355b9b593742b4f5cb72cab92051980fa73d3/src/ray/protobuf/common.proto#L583>`_ for more information. The function/method name is available as the Name label. If the task was retried due to failure or reconstruction, the IsRetry label will be set to "1", otherwise "0".
   * - `ray_actors`
     - `Name`, `State`
     - Current number of actors in a particular state. The State label is described by `rpc::ActorTableData <https://github.com/ray-project/ray/blob/e85355b9b593742b4f5cb72cab92051980fa73d3/src/ray/protobuf/gcs.proto#L85>`_ proto in gcs.proto. The actor class name is available in the Name label.
   * - `ray_resources`
     - `Name`, `State`, `InstanceId`
     - Logical resource usage for each node of the cluster. Each resource has some quantity that is `in either <https://github.com/ray-project/ray/blob/9eab65ed77bdd9907989ecc3e241045954a09cb4/src/ray/stats/metric_defs.cc#L188>`_ USED state vs AVAILABLE state. The Name label defines the resource name (e.g., CPU, GPU).
   * - `ray_object_store_memory`
     - `Location`, `ObjectState`, `InstanceId`
     - Object store memory usage in bytes, `broken down <https://github.com/ray-project/ray/blob/9eab65ed77bdd9907989ecc3e241045954a09cb4/src/ray/stats/metric_defs.cc#L231>`_ by logical Location (SPILLED, IN_MEMORY, etc.), and ObjectState (UNSEALED, SEALED).
   * - `ray_placement_groups`
     - `State`
     - Current number of placement groups by state. The State label (e.g., PENDING, CREATED, REMOVED) describes the state of the placement group. See `rpc::PlacementGroupTable <https://github.com/ray-project/ray/blob/e85355b9b593742b4f5cb72cab92051980fa73d3/src/ray/protobuf/gcs.proto#L517>`_ for more information.
   * - `ray_node_cpu_utilization`
     - `InstanceId`
     - The CPU utilization per node as a percentage quantity (0..100). This should be scaled by the number of cores per node to convert the units into cores.
   * - `ray_node_cpu_count`
     - `InstanceId`
     - The number of CPU cores per node.
   * - `ray_node_gpus_utilization`
     - `InstanceId`
     - The GPU utilization per node as a percentage quantity (0..NGPU*100). Note that unlike ray_node_cpu_utilization, this quantity is pre-multiplied by the number of GPUs per node.
   * - `ray_node_disk_usage`
     - `InstanceId`
     - The amount of disk space used per node, in bytes.
   * - `ray_node_disk_free`
     - `InstanceId`
     - The amount of disk space available per node, in bytes.
   * - `ray_node_disk_io_write_speed`
     - `InstanceId`
     - The disk write throughput per node, in bytes per second.
   * - `ray_node_disk_io_read_speed`
     - `InstanceId`
     - The disk read throughput per node, in bytes per second.
   * - `ray_node_mem_used`
     - `InstanceId`
     - The amount of physical memory used per node, in bytes.
   * - `ray_node_mem_total`
     - `InstanceId`
     - The amount of physical memory available per node, in bytes.
   * - `ray_component_uss_mb`
     - `Component`, `InstanceId`
     - The measured unique set size in megabytes, broken down by logical Ray component (e.g., raylet, gcs, workers).
   * - `ray_node_gram_used`
     - `InstanceId`
     - The amount of GPU memory used per node, in bytes.
   * - `ray_node_network_receive_speed`
     - `InstanceId`
     - The network receive throughput per node, in bytes per second.
   * - `ray_node_network_send_speed`
     - `InstanceId`
     - The network send throughput per node, in bytes per second.
   * - `ray_cluster_active_nodes`
     - `node_type`
     - The number of healthy nodes in the cluster, broken down by autoscaler node type.
   * - `ray_cluster_failed_nodes`
     - `node_type`
     - The number of failed nodes reported by the autoscaler, broken down by node type.
   * - `ray_cluster_pending_nodes`
     - `node_type`
     - The number of pending nodes reported by the autoscaler, broken down by node type.

Metrics Semantics and Consistency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray guarantees all its internal state metrics are *eventually* consistent even in the presence of failures--- should any worker fail, eventually the right state will be reflected in the Prometheus time-series output. However, any particular metrics query is not guaranteed to reflect an exact snapshot of the cluster state.

For the `ray_tasks` and `ray_actors` metrics, you should use sum queries to plot their outputs (e.g., ``sum(ray_tasks) by (Name, State)``). The reason for this is that Ray's task metrics are emitted from multiple distributed components. Hence, there are multiple metric points, including negative metric points, emitted from different processes that must be summed to produce the correct logical view of the distributed system. For example, for a single task submitted and executed, Ray may emit  ``(submitter) SUBMITTED_TO_WORKER: 1, (executor) SUBMITTED_TO_WORKER: -1, (executor) RUNNING: 1``, which reduces to ``SUBMITTED_TO_WORKER: 0, RUNNING: 1`` after summation.

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


Troubleshooting
---------------

.. _unverified-developer:

Mac does not trust the developer when installing prometheus or grafana
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You may have received an error that looks like this:

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/troubleshooting/prometheus-trusted-developer.png
    :align: center

When downloading binaries from the internet, Mac requires that the binary be signed by a trusted developer ID.
Unfortunately, many developers today are not trusted by Mac and so this requirement must be overridden by the user manaully.

See `these instructions <https://support.apple.com/guide/mac-help/open-a-mac-app-from-an-unidentified-developer-mh40616/mac>`_ on how to override the restriction and install or run the application.

Grafana dashboards are not embedded in the Ray dashboard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you're getting error that `RAY_GRAFANA_HOST` is not setup despite you've set it up, please check:
That you've included protocol in the URL (e.g. `http://your-grafana-url.com` instead of `your-grafana-url.com`).
Also, make sure that url doesn't have trailing slash (e.g. `http://your-grafana-url.com` instead of `http://your-grafana-url.com/`).