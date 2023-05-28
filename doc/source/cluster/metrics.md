# Collecting metrics
Metrics (both system and application metrics) are useful for monitoring and troubleshooting Ray applications and system. For example, you may want to access a node's metrics if it dies unexpectedly.

Similar to Kubenetes, Ray records and emits time-series metrics in [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/). Ray does not provide a native storage solution for metrics. Users need to manage the lifecycle of the metrics by themselves. This page provides instructions on how to collect metrics from Ray clusters.


## System and applcication metrics
**System metrics**: Ray exports a number of system metrics. View {ref}`system metrics <system-metrics>` for more details of the emitted metrics .

**Application metrics**: Application-specific metrics are useful to monitor your application states. View {ref}`adding application metrics <application-level-metrics>` for how to record your metrics.

(prometheus-setup)=
## Setting up your Prometheus server
Ray doesn't start Prometheus servers for users. Users need to decide where to host it and configure it so that it can scrape the metrics from clusters.

```{admonition} Tip
:class: tip
The instructions below describe how to up Prometheus on your local machine. View [Prometheus documentation](https://prometheus.io/docs/introduction/overview/) for the best strategy to set up your Prometheus Server (e.g., whether to use a multi-tenant Prometheus instance).
```

First, [download Prometheus](https://prometheus.io/download/). Make sure to download the correct binary for your operating system. (Ex: darwin for mac osx)

Then, unzip the archive into a local directory using the following command.

```bash
tar xvfz prometheus-*.tar.gz
cd prometheus-*
```
Ray exports metrics only when ``ray[default]`` is installed.

```bash
pip install "ray[default]"
```

Ray provides a prometheus config that works out of the box. After running ray, it can be found at `/tmp/ray/session_latest/metrics/prometheus/prometheus.yml`.

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
# Scrape from each ray node as defined in the service_discovery.json provided by ray.
- job_name: 'ray'
  file_sd_configs:
  - files:
    - '/tmp/ray/prom_metrics_service_discovery.json'
```

Next, let's start Prometheus.

```shell
./prometheus --config.file=/tmp/ray/session_latest/metrics/prometheus/prometheus.yml
```
```{admonition} Note
:class: note
If you are using mac, you may receive an error at this point about trying to launch an application where the developer has not been verified. See the "Troubelshooting" guide below to fix the issue.
```

Now, you can access Ray metrics from the default Prometheus url, `http://localhost:9090`.


### Troubleshooting
#### Getting Prometheus to use the Ray configurations when installed via Homebrew on macOS X
With Homebrew, Prometheus is installed as a service that is automatically launched for you.
Therefore, to configure these services, you cannot simply pass in the config files as command line arguments.

Instead, change the --config-file line in `/usr/local/etc/prometheus.args` to read `--config.file /tmp/ray/session_latest/metrics/prometheus/prometheus.yml`.

You can then start or restart the services with `brew services start prometheus`.


#### MacOS does not trust the developer to install Prometheus
You may have received an error that looks like this:

![trust error](https://raw.githubusercontent.com/ray-project/Images/master/docs/troubleshooting/prometheus-trusted-developer.png)

When downloading binaries from the internet, Mac requires that the binary be signed by a trusted developer ID.
Unfortunately, many developers today are not trusted by Mac and so this requirement must be overridden by the user manaully.

See [these instructions](https://support.apple.com/guide/mac-help/open-a-mac-app-from-an-unidentified-developer-mh40616/mac) on how to override the restriction and install or run the application.


(scrape-metrics)=
## Scraping metrics
Ray runs a metrics agent per node to export system and application metrics. Each metrics agent collects metrics from the local
node and exposes these in a Prometheus format. You can then scrape each endpoint to access the metrics.

To scrape the endpoints, we need to ensure service discovery, allowing Prometheus to find the metrics agents' endpoints on each node.

### Auto-discovering metrics endpoints

You can allow Prometheus to dynamically find endpoints it should scrape by using Prometheus' [file based service discovery](https://prometheus.io/docs/guides/file-sd/#installing-configuring-and-running-prometheus).
This is the recommended way to export Prometheus metrics when using the Ray {ref}`cluster launcher <vm-cluster-quick-start>`, as node IP addresses can often change as the cluster scales up and down.

Ray auto-generates a Prometheus [service discovery file](https://prometheus.io/docs/guides/file-sd/#installing-configuring-and-running-prometheus) on the head node to facilitate metrics agents' service discovery. This allows you to scrape all metrics in the cluster without knowing their IPs. Let's walk through how to acheive this.

The service discovery file is generated on the {ref}`head node <cluster-head-node>`. On this node, look for ``/tmp/ray/prom_metrics_service_discovery.json`` (or the eqiuvalent file if using a custom Ray ``temp_dir``). Ray will periodically update this file with the addresses of all metrics agents in the cluster.

Ray automatically produces a Prometheus config which scrapes the file for service discovery found at `/tmp/ray/session_latest/metrics/prometheus/prometheus.yml`. You can choose to use this config or modify your own to enable this behavior. The details of the config can be seen below and full documentation can be found [here](https://prometheus.io/docs/prometheus/latest/configuration/configuration/).

With this config, Prometheus will automatically update the addresses that it scrapes based on the contents of Ray's service discovery file.

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

If you already know the IP addresses of all nodes in your Ray Cluster, you can configure Prometheus to read metrics from a static list of endpoints. To
do this, first set a fixed port that Ray should use to export metrics.  If using the cluster VM launcher, pass ``--metrics-export-port=<port>`` to ``ray start``.  If using KubeRay, you can specify ``rayStartParams.metrics-export-port`` in the RayCluster configuration file. The port must be specified on all nodes in the cluster.

If you do not know the IP addresses of the nodes in your Ray cluster, you can also programmatically discover the endpoints by reading the Ray Cluster information. Here, we will use a Python script and the ``ray.nodes()`` API to find the metrics agents' URLs, by combining the ``NodeManagerAddress`` with the ``MetricsExportPort``. For example:

```python
# On a cluster node:
import ray
ray.init()
from pprint import pprint
pprint(ray.nodes())

"""
The <NodeManagerAddress>:<MetricsExportPort> from each of these entries
should be passed to Prometheus.
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
If you need to process and export metrics into other storage/management systems, there are a number of open source metric processing tools available such as [Vector][Vector].

[Vector]: https://vector.dev/