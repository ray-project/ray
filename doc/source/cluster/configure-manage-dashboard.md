(observability-configure-manage-dashboard)=
# Configuring and Managing Ray Dashboard
{ref}`Ray Dashboard<observability-getting-started>` is one of the most important tools to monitor and debug Ray applications and Clusters. This page describes how to configure Ray Dashboard on your Clusters.

Dashboard configurations may differ depending on how you launch Ray Clusters (e.g., local Ray Cluster v.s. KubeRay). Integrations with Prometheus and Grafana are optional for enhanced Dashboard experience.

:::{note}
Ray Dashboard is useful for interactive development and debugging because when clusters terminate, the dashboard UI and the underlying data are no longer accessible. For production monitoring and debugging, you should rely on [persisted logs](../cluster/kubernetes/user-guides/persist-kuberay-custom-resource-logs.md), [persisted metrics](./metrics.md), [persisted Ray states](../ray-observability/user-guides/cli-sdk.rst), and other observability tools.
:::

## Changing the Ray Dashboard port
Ray Dashboard runs on port `8265` of the head node. Follow the instructions below to customize the port if needed.

::::{tab-set}

:::{tab-item} Single-node local cluster
**Start the cluster explicitly with CLI** <br/>
Pass the ``--dashboard-port`` argument with ``ray start`` in the command line.

**Start the cluster implicitly with `ray.init`** <br/>
Pass the keyword argument ``dashboard_port`` in your call to ``ray.init()``.
:::

:::{tab-item} VM Cluster Launcher
Include the ``--dashboard-port`` argument in the `head_start_ray_commands` section of the [Cluster Launcher's YAML file](https://github.com/ray-project/ray/blob/0574620d454952556fa1befc7694353d68c72049/python/ray/autoscaler/aws/example-full.yaml#L172).
```yaml
head_start_ray_commands:
  - ray stop
  # Replace ${YOUR_PORT} with the port number you need.
  - ulimit -n 65536; ray start --head --dashboard-port=${YOUR_PORT} --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml

```
:::

:::{tab-item} KubeRay
View the [specifying non-default ports](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/config.html#specifying-non-default-ports) page for details.
:::

::::

(dashboard-in-browser)=
## Viewing Ray Dashboard in browsers
When you start a single-node Ray cluster on your laptop, you can access the dashboard through a URL printed when Ray is initialized (the default URL is `http://localhost:8265`).


When you start a remote Ray cluster with the {ref}`VM cluster launcher <vm-cluster-quick-start>`, {ref}`KubeRay operator <kuberay-quickstart>`, or manual configuration, the Ray Dashboard launches on the head node but the dashboard port may not be publicly exposed. You need an additional setup to access the Ray Dashboard from outside the head node.

:::{danger}
For security purpose, do not expose Ray Dashboard publicly without proper authentication in place.
:::

::::{tab-set}

:::{tab-item} VM Cluster Launcher
**Port forwarding** <br/>
You can securely port-forward local traffic to the dashboard with the ``ray
dashboard`` command.

```shell
$ ray dashboard [-p <port, 8265 by default>] <cluster config file>
```

The dashboard is now visible at ``http://localhost:8265``.
:::

:::{tab-item} KubeRay

The KubeRay operator makes Dashboard available via a Service targeting the Ray head pod, named ``<RayCluster name>-head-svc``. Access
Dashboard from within the Kubernetes cluster at ``http://<RayCluster name>-head-svc:8265``.

There are two ways to expose Dashboard outside the Cluster:

**1. Setting up ingress** <br/>
Follow the [instructions](kuberay-ingress) to set up ingress to access Ray Dashboard. **The Ingress must only allows access from trusted sources.**

**2. Port forwarding** <br/>
You can also view the dashboard from outside the Kubernetes cluster by using port-forwarding:

```shell
$ kubectl port-forward service/${RAYCLUSTER_NAME}-head-svc 8265:8265
# Visit ${YOUR_IP}:8265 for the Dashboard (e.g. 127.0.0.1:8265 or ${YOUR_VM_IP}:8265)
```

```{admonition} Note
:class: note
Do not use port forwarding for production environment. Follow the instructions above to expose the Dashboard with Ingress.
```

For more information about configuring network access to a Ray cluster on Kubernetes, see the {ref}`networking notes <kuberay-networking>`.

:::

::::

## Running behind a reverse proxy

Ray Dashboard should work out-of-the-box when accessed via a reverse proxy. API requests don't need to be proxied individually.

Always access the dashboard with a trailing ``/`` at the end of the URL.
For example, if your proxy is set up to handle requests to ``/ray/dashboard``, view the dashboard at ``www.my-website.com/ray/dashboard/``.

The dashboard sends HTTP requests with relative URL paths. Browsers handle these requests as expected when the ``window.location.href`` ends in a trailing ``/``.

This is a peculiarity of how many browsers handle requests with relative URLs, despite what [MDN](https://developer.mozilla.org/en-US/docs/Learn/Common_questions/What_is_a_URL#examples_of_relative_urls) defines as the expected behavior.

Make your dashboard visible without a trailing ``/`` by including a rule in your reverse proxy that redirects the user's browser to ``/``, i.e. ``/ray/dashboard`` --> ``/ray/dashboard/``.

Below is an example with a [traefik](https://doc.traefik.io/traefik/getting-started/quick-start/) TOML file that accomplishes this:

```yaml
[http]
  [http.routers]
    [http.routers.to-dashboard]
      rule = "PathPrefix(`/ray/dashboard`)"
      middlewares = ["test-redirectregex", "strip"]
      service = "dashboard"
  [http.middlewares]
    [http.middlewares.test-redirectregex.redirectRegex]
      regex = "^(.*)/ray/dashboard$"
      replacement = "${1}/ray/dashboard/"
    [http.middlewares.strip.stripPrefix]
      prefixes = ["/ray/dashboard"]
  [http.services]
    [http.services.dashboard.loadBalancer]
      [[http.services.dashboard.loadBalancer.servers]]
        url = "http://localhost:8265"
```

```{admonition} Warning
:class: warning
The Ray Dashboard provides read **and write** access to the Ray Cluster. The reverse proxy must provide authentication or network ingress controls to prevent unauthorized access to the Cluster.
```

## Disabling the Dashboard

Dashboard is included if you use `ray[default]` or {ref}`other installation commands <installation>` and automatically started.

To disable Dashboard, use the following arguments `--include-dashboard`.

::::{tab-set}

:::{tab-item} Single-node local cluster

**Start the cluster explicitly with CLI** <br/>

```bash
ray start --include-dashboard=False
```

**Start the cluster implicitly with `ray.init`** <br/>

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray
ray.init(include_dashboard=False)
```

:::

:::{tab-item} VM Cluster Launcher
Include the `ray start --head --include-dashboard=False` argument
in the `head_start_ray_commands` section of the [Cluster Launcher's YAML file](https://github.com/ray-project/ray/blob/0574620d454952556fa1befc7694353d68c72049/python/ray/autoscaler/aws/example-full.yaml#L172).
:::

:::{tab-item} KubeRay

```{admonition} Warning
:class: warning
It's not recommended to disable Dashboard because several KubeRay features like `RayJob` and `RayService` depend on it.
```

Set `spec.headGroupSpec.rayStartParams.include-dashboard` to `False`. Check out this [example YAML file](https://gist.github.com/kevin85421/0e6a8dd02c056704327d949b9ec96ef9).

:::
::::


(observability-visualization-setup)=
## Embed Grafana visualizations into Ray Dashboard

For the enhanced Ray Dashboard experience, like {ref}`viewing time-series metrics<dash-metrics-view>` together with logs, Job info, etc., set up Prometheus and Grafana and integrate them with Ray Dashboard.

### Setting up Prometheus
To render Grafana visualizations, you need Prometheus to scrape metrics from Ray Clusters. Follow {ref}`the instructions <prometheus-setup>` to set up your Prometheus server and start to scrape system and application metrics from Ray Clusters.


### Setting up Grafana
Grafana is a tool that supports advanced visualizations of Prometheus metrics and allows you to create custom dashboards with your favorite metrics. Follow {ref}`the instructions <grafana>` to set up Grafana.


(embed-grafana-in-dashboard)=
### Embedding Grafana visualizations into Ray Dashboard
To view embedded time-series visualizations in Ray Dashboard, the following must be set up:

1. The head node of the cluster is able to access Prometheus and Grafana.
2. The browser of the dashboard user is able to access Grafana.

Configure these settings using the `RAY_GRAFANA_HOST`, `RAY_PROMETHEUS_HOST`, `RAY_PROMETHEUS_NAME`, and `RAY_GRAFANA_IFRAME_HOST` environment variables when you start the Ray Clusters.

* Set `RAY_GRAFANA_HOST` to an address that the head node can use to access Grafana. Head node does health checks on Grafana on the backend.
* Set `RAY_PROMETHEUS_HOST` to an address the head node can use to access Prometheus.
* Set `RAY_PROMETHEUS_NAME` to select a different data source to use for the Grafana dashboard panels to use. Default is "Prometheus".
* Set `RAY_GRAFANA_IFRAME_HOST` to an address that the user's browsers can use to access Grafana and embed visualizations. If `RAY_GRAFANA_IFRAME_HOST` is not set, Ray Dashboard uses the value of `RAY_GRAFANA_HOST`.

For example, if the IP of the head node is 55.66.77.88 and Grafana is hosted on port 3000. Set the value to `RAY_GRAFANA_HOST=http://55.66.77.88:3000`.
* If you start a single-node Ray Cluster manually, make sure these environment variables are set and accessible before you start the cluster or as a prefix to the `ray start ...` command, e.g., `RAY_GRAFANA_HOST=http://55.66.77.88:3000 ray start ...`
* If you start a Ray Cluster with {ref}`VM Cluster Launcher <cloud-vm-index>`, the environment variables should be set under `head_start_ray_commands` as a prefix to the `ray start ...` command.
* If you start a Ray Cluster with {ref}`KubeRay <kuberay-index>`, refer to this {ref}`tutorial <kuberay-prometheus-grafana>`.

If all the environment variables are set properly, you should see time-series metrics in {ref}`Ray Dashboard <observability-getting-started>`.

:::{note}
If you use a different Prometheus server for each Ray Cluster and use the same Grafana server for all Clusters, set the `RAY_PROMETHEUS_NAME` environment variable to different values for each Ray Cluster and add these datasources in Grafana. Follow {ref}`these instructions <grafana>` to set up Grafana.
:::

#### Alternate Prometheus host location
By default, Ray Dashboard assumes Prometheus is hosted at `localhost:9090`. You can choose to run Prometheus on a non-default port or on a different machine. In this case, make sure that Prometheus can scrape the metrics from your Ray nodes following instructions {ref}`here <scrape-metrics>`.

Then, configure `RAY_PROMETHEUS_HOST` environment variable properly as stated above. For example, if Prometheus is hosted at port 9000 on a node with ip 55.66.77.88, set `RAY_PROMETHEUS_HOST=http://55.66.77.88:9000`.


#### Customize headers for requests from the Ray dashboard to Prometheus

If Prometheus requires additional headers for authentication, set `RAY_PROMETHEUS_HEADERS` in one of the following JSON formats for Ray dashboard to send them to Prometheus:
1. `{"Header1": "Value1", "Header2": "Value2"}`
2. `[["Header1", "Value1"], ["Header2", "Value2"], ["Header2", "Value3"]]`


#### Alternate Grafana host location
By default, Ray Dashboard assumes Grafana is hosted at `localhost:3000`. You can choose to run Grafana on a non-default port or on a different machine as long as the head node and the dashboard browsers of can access it.

If Grafana is exposed with NGINX ingress on a Kubernetes cluster, the following line should be present in the Grafana ingress annotation:

```yaml
nginx.ingress.kubernetes.io/configuration-snippet: |
    add_header X-Frame-Options SAMEORIGIN always;
```

When both Grafana and the Ray Cluster are on the same Kubernetes cluster, set `RAY_GRAFANA_HOST` to the external URL of the Grafana ingress.



#### User authentication for Grafana
When the Grafana instance requires user authentication, the following settings have to be in its [configuration file](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/) to correctly embed in Ray Dashboard:

```ini
  [security]
  allow_embedding = true
  cookie_secure = true
  cookie_samesite = none
```

#### Troubleshooting

##### Dashboard message: either Prometheus or Grafana server is not detected
If you have followed the instructions above to set up everything, run the connection checks below in your browser:
* check Head Node connection to Prometheus server: add `api/prometheus_health` to the end of Ray Dashboard URL (for example: http://127.0.0.1:8265/api/prometheus_health)and visit it.
* check Head Node connection to Grafana server: add `api/grafana_health` to the end of Ray Dashboard URL (for example: http://127.0.0.1:8265/api/grafana_health) and visit it.
* check browser connection to Grafana server: visit the URL used in `RAY_GRAFANA_IFRAME_HOST`.


##### Getting an error that says `RAY_GRAFANA_HOST` is not setup
If you have set up Grafana , check that:
* You've included the protocol in the URL (e.g., `http://your-grafana-url.com` instead of `your-grafana-url.com`).
* The URL doesn't have a trailing slash (e.g., `http://your-grafana-url.com` instead of `http://your-grafana-url.com/`).

##### Certificate Authority (CA error)
You may see a CA error if your Grafana instance is hosted behind HTTPS. Contact the Grafana service owner to properly enable HTTPS traffic.


## Viewing built-in Dashboard API metrics

Dashboard is powered by a server that serves both the UI code and the data about the cluster via API endpoints.
Ray emits basic Prometheus metrics for each API endpoint:

`ray_dashboard_api_requests_count_requests_total`: Collects the total count of requests. This is tagged by endpoint, method, and http_status.

`ray_dashboard_api_requests_duration_seconds_bucket`: Collects the duration of requests. This is tagged by endpoint and method.

For example, you can view the p95 duration of all requests with this query:

```text

histogram_quantile(0.95, sum(rate(ray_dashboard_api_requests_duration_seconds_bucket[5m])) by (le))
```

You can query these metrics from the Prometheus or Grafana UI. Find instructions above for how to set these tools up.
