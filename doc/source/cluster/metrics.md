(collect-metrics)=
# Collecting metrics
Metrics are useful for monitoring and troubleshooting your system and Ray applications. For example, you may want to access a node's metrics if it terminates unexpectedly.

Similar to Kubernetes, Ray records and emits time-series metrics using the [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/). Ray does not provide a native storage solution for metrics. Users need to manage the lifecycle of the metrics by themselves. This page provides instructions on how to collect metrics from Ray Clusters.


## System and applcication metrics
**System metrics**: Ray exports a number of system metrics. View {ref}`system metrics <system-metrics>` for more details about the emitted metrics.

**Application metrics**: Application-specific metrics are useful for monitoring your application states. View {ref}`adding application metrics <application-level-metrics>` for how to record metrics.

(prometheus-setup)=
## Setting up your Prometheus server
Ray doesn't start Prometheus servers for users. Users need to decide where to host and configure it to scrape the metrics from the clusters.

```{admonition} Tip
:class: tip
The instructions below describe how to set up Prometheus on your local machine. View [Prometheus documentation](https://prometheus.io/docs/introduction/overview/) for the best strategy to set up your Prometheus Server (e.g., whether to use a multi-tenant Prometheus instance).
```

First, [download Prometheus](https://prometheus.io/download/). Make sure to download the correct binary for your operating system. (For example, Darwin for macOS X.)

Then, unzip the archive into a local directory using the following command:

```bash
tar xvfz prometheus-*.tar.gz
cd prometheus-*
```
Ray exports metrics only when ``ray[default]`` is installed.

```bash
pip install "ray[default]"
```

Ray provides a Prometheus config that works out of the box. After running Ray, you can find the config at `/tmp/ray/session_latest/metrics/prometheus/prometheus.yml`.

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
# Scrape from each Ray node as defined in the service_discovery.json provided by Ray.
- job_name: 'ray'
  file_sd_configs:
  - files:
    - '/tmp/ray/prom_metrics_service_discovery.json'
```

Next, start Prometheus:

```shell
./prometheus --config.file=/tmp/ray/session_latest/metrics/prometheus/prometheus.yml
```
```{admonition} Note
:class: note
If you are using macOS, you may receive an error at this point about trying to launch an application where the developer has not been verified. See the "Troubleshooting" guide below to fix the issue.
```

Now, you can access Ray metrics from the default Prometheus URL, `http://localhost:9090`.


### Troubleshooting
#### Using Ray configurations in Prometheus with Homebrew on macOS X
Homebrew installs Prometheus as a service that is automatically launched for you.
To configure these services, you cannot simply pass in the config files as command line arguments.

Instead, change the --config-file line in `/usr/local/etc/prometheus.args` to read `--config.file /tmp/ray/session_latest/metrics/prometheus/prometheus.yml`.

You can then start or restart the services with `brew services start prometheus`.


#### macOS does not trust the developer to install Prometheus
You may receive the following error:

![trust error](https://raw.githubusercontent.com/ray-project/Images/master/docs/troubleshooting/prometheus-trusted-developer.png)

When downloading binaries from the internet, macOS requires that the binary be signed by a trusted developer ID.
Many developers are not on macOS's trusted list. Users can manually override this requirement.

See [these instructions](https://support.apple.com/guide/mac-help/open-a-mac-app-from-an-unidentified-developer-mh40616/mac) for how to override the restriction and install or run the application.


(scrape-metrics)=
## Scraping metrics
Ray runs a metrics agent per node to export system and application metrics. Each metrics agent collects metrics from the local
node and exposes them in a Prometheus format. You can then scrape each endpoint to access the metrics.

To scrape the endpoints, we need to ensure service discovery, which allows Prometheus to find the metrics agents' endpoints on each node.

### Auto-discovering metrics endpoints

You can allow Prometheus to dynamically find the endpoints to scrape by using Prometheus' [file based service discovery](https://prometheus.io/docs/guides/file-sd/#installing-configuring-and-running-prometheus).
Use auto-discovery to export Prometheus metrics when using the Ray {ref}`cluster launcher <vm-cluster-quick-start>`, as node IP addresses can often change as the cluster scales up and down.

Ray auto-generates a Prometheus [service discovery file](https://prometheus.io/docs/guides/file-sd/#installing-configuring-and-running-prometheus) on the head node to facilitate metrics agents' service discovery. This function allows you to scrape all metrics in the cluster without knowing their IPs. The following information guides you on the setup.

The service discovery file is generated on the {ref}`head node <cluster-head-node>`. On this node, look for ``/tmp/ray/prom_metrics_service_discovery.json`` (or the eqiuvalent file if using a custom Ray ``temp_dir``). Ray periodically updates this file with the addresses of all metrics agents in the cluster.

Ray automatically produces a Prometheus config, which scrapes the file for service discovery found at `/tmp/ray/session_latest/metrics/prometheus/prometheus.yml`. You can choose to use this config or modify your own config to enable this behavior. See the details of the config below. Find the full documentation [here](https://prometheus.io/docs/prometheus/latest/configuration/configuration/).

With this config, Prometheus automatically updates the addresses that it scrapes based on the contents of Ray's service discovery file.

```yaml
# Prometheus config file

# my global config
global:
  scrape_interval:     2s
  evaluation_interval: 2s

# Scrape from Ray.
scrape_configs:
- job_name: 'ray'
  file_sd_configs:
  - files:
    - '/tmp/ray/prom_metrics_service_discovery.json'
```

### Manually discovering metrics endpoints

If you know the IP addresses of the nodes in your Ray Cluster, you can configure Prometheus to read metrics from a static list of endpoints.
Set a fixed port that Ray should use to export metrics.  If you're using the VM Cluster Launcher, pass ``--metrics-export-port=<port>`` to ``ray start``.  If you're using KubeRay, specify ``rayStartParams.metrics-export-port`` in the RayCluster configuration file. You must specify the port on all nodes in the cluster.

If you do not know the IP addresses of the nodes in your Ray cluster, you can also programmatically discover the endpoints by reading the Ray Cluster information. Here, we will use a Python script and the ``ray.nodes()`` API to find the metrics agents' URLs, by combining the ``NodeManagerAddress`` with the ``MetricsExportPort``. For example:

```python
# On a cluster node:
import ray
ray.init()
from pprint import pprint
pprint(ray.nodes())

"""
Pass the <NodeManagerAddress>:<MetricsExportPort> from each of these entries
to Prometheus.
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
```

## Processing and exporting metrics
If you need to process and export metrics into other storage or management systems, you can consider a number of open source metric processing tools available, such as [Vector][Vector].

[Vector]: https://vector.dev/