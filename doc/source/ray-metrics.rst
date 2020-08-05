Monitoring Ray
==============
To help monitoring Ray applications, Ray

- Collects Ray's pre-selected system level metrics.
- Exposes metrics through Prometheus endpoints.
- Supports application-level metrics collection APIs (EXPERIMENTAL).

.. note::

    It is currently an experimental feature and under active development. APIs are subject to change.

Getting Started (Single Node)
-----------------------------
Ray exposes its metrics in Prometheus format. For the simplicty, let's use Prometheus to scrape these metrics.

First, let's learn how to access metrics when you use `ray.init()`.

.. code-block:: python

    import ray
    ray.init(_metrics_export_port=8080)
    import time
    time.sleep(100)

Now you can access Prometheus endpoint at the address `http://localhost:8080`. Note that if you don't specify `_metrics_export_port`, 
the port is randomly assigned. You can check the port number when you run `ray.init`.

.. code-block:: bash

    {'node_ip_address': '192.168.1.82',
    'raylet_ip_address': '192.168.1.82',
    'redis_address': '192.168.1.82:6379',
    'object_store_address': '/tmp/ray/session_2020-08-04_13-50-35_156166_20127/sockets/plasma_store',
    'raylet_socket_name': '/tmp/ray/session_2020-08-04_13-50-35_156166_20127/sockets/raylet',
    'webui_url': 'localhost:8265',
    'session_dir': '/tmp/ray/session_2020-08-04_13-50-35_156166_20127',
    'metrics_export_port': 63301} # <= You can access metrics through http://localhost:[metrics_export_port]

Second, let's learn how to expose metrics when you use `ray start`.

.. code-block:: bash

    ray start --head --metrics-export-port=8080 # Assign metrics export port on a head node.


Now, you can scrape Ray's metrics using Prometheus. 

First, download Prometheus.

.. code-block:: bash

    tar xvfz prometheus-*.tar.gz
    cd prometheus-*

Let's modify Prometheus's config file to scrape metrics from Prometheus endpoints.

.. code-block:: yaml

    # prometheus.yml
    global:
    scrape_interval:     5s
    evaluation_interval: 5s

    rule_files:
    # - "first.rules"
    # - "second.rules"

    scrape_configs:
    - job_name: prometheus
        static_configs:
        - targets: ['localhost:8080'] # This must be same as metrics_export_port

Next, let's start Prometheus.

.. code-block:: shell

    ./prometheus --config.file=./prometheus.yml

Now, you can access Ray metrics from the default Prometheus url, `http://localhost:9090`.

Getting Started (Multi-nodes)
-----------------------------
Ray runs a metrics agent per node. Each metrics agent collects metrics from a local node and exposes it through a Prometheus endpoint.

We will walkthrough how to import metrics from a Ray cluster using Prometheus.

At a head node,

.. code-block:: bash

    ray start --head --metrics-export-port=8080 # Assign metrics export port on a head node.

At a worker node,

.. code-block:: bash

    ray start --address=[head_node_address] --metrics-export-port=8080

You can now get the url of metrics agents using `ray.nodes()`

.. code-block:: python

    import ray
    ray.init(address='auto')
    from pprint import pprint
    pprint(ray.nodes())

    """
    [{'Alive': True,
    'MetricsExportPort': 8080,
    'NodeID': '02ec0a33d3e75a520fd07238e9363b48a442ce24',
    'NodeManagerAddress': '192.168.1.82',
    'NodeManagerHostname': 'host-MBP.attlocal.net',
    'NodeManagerPort': 57167,
    'ObjectManagerPort': 56201,
    'ObjectStoreSocketName': '/tmp/ray/session_2020-08-04_13-57-29_819087_20360/sockets/plasma_store',
    'RayletSocketName': '/tmp/ray/session_2020-08-04_13-57-29_819087_20360/sockets/raylet',
    'Resources': {'CPU': 16.0,
                    'memory': 91.0,
                    'node:192.168.1.82': 1.0,
                    'object_store_memory': 31.0},
    'alive': True}]
    """

Now, setup your prometheus to read metrics from `[NodeManagerAddress]:[MetricsExportPort]` from all nodes in the cluster.
If you'd like to make this process automated, you can also use `file based service discovery <https://prometheus.io/docs/guides/file-sd/#installing-configuring-and-running-prometheus>`_.

Getting Started (Cluster Launcher)
----------------------------------
When you use a Ray cluster launcher, it is common node IP addresses are changing because cluster is scaling up and down. 
In this case, you can use Prometheus' `file based service discovery <https://prometheus.io/docs/guides/file-sd/#installing-configuring-and-running-prometheus>`_.
