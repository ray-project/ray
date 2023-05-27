(observability-configure-manage-dashboard)=

# Configuring and Managing Ray Dashboard
{ref}`Ray Dashboard<observability-getting-started>` is one of the most important tools to monitor and debug Ray applications and clusters. This page describes how to configure Ray Dashboard on your clusters.

Dashboard configurations may differ depending on how you launch Ray clusters (e.g., local Ray cluster v.s. KubeRay). Integrations with Prometheus and Grafana are optional for better dashboard experience.

:::{note}
At the moment, Ray Dashboard is only intended for interactive development and debugging because dashboard UI and the underlying data are not accessible any more after the clusters are terminated. For production monitoring and debugging, users should rely on [persisted logs](../cluster/kubernetes/user-guides/logging.md), [persisted metrics](./metrics.md), [persisted Ray states](../ray-observability/user-guides/cli-sdk.rst), and other observability tools.
:::

## Changing Ray Dashboard Port
Ray Dashobard runs on port `8265` of the head node. Follow the instructions below to customize the port if needed.

::::{tab-set}

:::{tab-item} Single-node local cluster
**Start the cluster explicitly with CLI** <br/>
To customize the port on which the dashboard runs, you can pass
the ``--dashboard-port`` argument with ``ray start`` in the command line.

**Start the cluster implicitly with ray.init** <br/>
If you need to customize the port on which the dashboard runs, you can pass the
keyword argument ``dashboard_port`` in your call to ``ray.init()``.
:::

:::{tab-item} VM Cluster Launcher
To disable the dashboard while using the "VM cluster launcher", include the `ray start --head --include-dashboard=False` argument
and specify the desired port number in the `head_start_ray_commands` section of the [cluster launcher's YAML file](https://github.com/ray-project/ray/blob/0574620d454952556fa1befc7694353d68c72049/python/ray/autoscaler/aws/example-full.yaml#L172)`.
:::

:::{tab-item} Kuberay
See the [Specifying non-default ports](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/config.html#specifying-non-default-ports) page.
:::

::::


## View Ray Dashboard in browsers
When you start a single-node Ray cluster on your laptop, you can access the dashboard through a URL printed when Ray is initialized (the default URL is `http://localhost:8265`).


When you start a Ray cluster via the {ref}`VM cluster launcher <vm-cluster-quick-start>` or {ref}`KubeRay operator <kuberay-quickstart>`, Ray Dashboard will be launched but the dashboard port is not publicly exposed. Additional setup is needed if you want to access the Ray Dashboard from your laptop's browser.

### Port forwarding
::::{tab-set}

:::{tab-item} VM Cluster Launcher
You can securely port-forward local traffic to the dashboard via the ``ray
dashboard`` command.

```shell
$ ray dashboard [-p <port, 8265 by default>] <cluster config file>
```

The dashboard is now be visible at ``http://localhost:8265``.
:::

:::{tab-item} Kuberay
The KubeRay operator makes the dashboard available via a Service targeting the Ray head pod, named ``<RayCluster name>-head-svc``. You can access the
dashboard from within the Kubernetes cluster at ``http://<RayCluster name>-head-svc:8265``.

You can also view the dashboard from outside the Kubernetes cluster by using port-forwarding:

```shell
$ kubectl port-forward service/raycluster-autoscaler-head-svc 8265:8265
```
For more information about configuring network access to a Ray cluster on Kubernetes, see the {ref}`networking notes <kuberay-networking>`.

<!-- :::{note} -->
It's not recommended to use port forwarding if you need have specific network security requirements. Follow the instructions below to add ingress rules to expose Ray Dashboard
<!-- ::: -->

:::

::::

### Adding ingress rule for KubeRay
To be added


## Running Behind a Reverse Proxy

Ray dashboard should work out-of-the-box when accessed via a reverse proxy. API requests don't need to be proxied individually.

Always access the dashboard with a trailing ``/`` at the end of the URL.
For example, if your proxy is set up to handle requests to ``/ray/dashboard``, view the dashboard at ``www.my-website.com/ray/dashboard/``.

The dashboard now sends HTTP requests with relative URL paths. Browsers will handle these requests as expected when the ``window.location.href`` ends in a trailing ``/``.

This is a peculiarity of how many browsers handle requests with relative URLs, despite what [MDN](https://developer.mozilla.org/en-US/docs/Learn/Common_questions/What_is_a_URL#examples_of_relative_urls)` defines as the expected behavior.

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

## Disabling the Dashboard

Dashboard is included in the `ray[default]` installation by default and automatically started.

To disable the dashboard, use the following arguments `--include-dashboard`.

::::{tab-set}

:::{tab-item} Single-node local cluster

**Start the cluster explicitly with CLI** <br/>

```bash
ray start --include-dashboard=False
```

**Start the cluster implicitly with ray.init** <br/>

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
To disable the dashboard while using the "VM cluster launcher", include the "ray start --head --include-dashboard=False" argument
in the "head_start_ray_commands" section of the `cluster launcher's YAML file <https://github.com/ray-project/ray/blob/0574620d454952556fa1befc7694353d68c72049/python/ray/autoscaler/aws/example-full.yaml#L172>`_.
:::

:::{tab-item} Kuberay
TODO
:::
::::


## Viewing Built-in Dashboard API Metrics

The dashboard is powered by a server that serves both the UI code and the data about the cluster via API endpoints.
There are basic Prometheus metrics that are emitted for each of these API endpoints:

`ray_dashboard_api_requests_count_requests_total`: Collects the total count of requests. This is tagged by endpoint, method, and http_status.

`ray_dashboard_api_requests_duration_seconds_bucket`: Collects the duration of requests. This is tagged by endpoint and method.

For example, you can view the p95 duration of all requests with this query:

```text

histogram_quantile(0.95, sum(rate(ray_dashboard_api_requests_duration_seconds_bucket[5m])) by (le))
```

These metrics can be queried via Prometheus or Grafana UI. Instructions on how to set these tools up can be found {ref}`here <observability-visualization-setup>`.


(observability-visualization-setup)=
## Integrating with Prometheus and Grafana
Optional integration with Prometheus and Grafana enables 2 valuable features:
- Default Grafana dashboard templates provided by Ray that visualize the important system metrics for monitoring and debugging.
- View the time-series metrics in Ray Dashboard directly via embedded Grafana visualizations.


For better Ray Dashboard experience, i.e., viewing time-series metrics, users need to set up Prometheus and Grafana and integrate them with Ray Dashboard.

### Setting up Prometheus
Prometheus is needed to scrape metrics from Ray clusters for rendering Grafana visualizations. Follow {ref}`the instructions <prometheus-setup>` to set up your Prometheus server and starts to scrape system and application metrics from Ray clusters.


(grafana)=
### Setting up Grafana
Grafana is a tool that supports more advanced visualizations of prometheus metrics and allows you to create custom dashboards with your favorite metrics. Ray exports some default configurations which include a default Grafana dashboard showing some of the most valuable metrics for debugging ray applications.

::::{tab-set}

:::{tab-item} Single-node local cluster

```{admonition} Note
:class: note
The instructions below describe one way of setting up Grafana on your local machine. View Grafana documentation for more comprehensive info.
```

First, [download Grafana](https://grafana.com/grafana/download). Follow the instructions on the download page to download the right binary for your operating system.

Then go to to the location of the binary and run grafana using the built in configuration found in `/tmp/ray/session_latest/metrics/grafana` folder.

```shell
./bin/grafana-server --config /tmp/ray/session_latest/metrics/grafana/grafana.ini web
```

Now, you can access grafana using the default grafana url, `http://localhost:3000`.
You can then see the default dashboard by going to dashboards -> manage -> Ray -> Default Dashboard. The same {ref}`metric graphs <system-metrics>` are also accessible via {ref}`Ray Dashboard <observability-getting-started>` after you integrate Grafan with Ray Dashboard.

```{admonition} Note
:class: note
If this is your first time using Grafana, you can login with the username: `admin` and password `admin`.
```

![grafana login](images/graphs.png){align=center}

:::

:::{tab-item} VM Cluster Launcher & KubeRay
View Grafana documentation for the best strategy to set up your Grafana Server (e.g., whether to use a central Grafana instance).

After your Grafana serve is running, find the Ray-provided default Grafana dashboard JSON at `/tmp/ray/session_latest/metrics/grafana/dashboards/default_grafana_dashboard.json`. [Import this dashboard](https://grafana.com/docs/grafana/latest/dashboards/manage-dashboards/#import-a-dashboard) to your Grafana.

If Grafana reports that datasource is not found, you can [add a datasource variable](https://grafana.com/docs/grafana/latest/dashboards/variables/add-template-variables/?pg=graf&plcmt=data-sources-prometheus-btn-1#add-a-data-source-variable) and using [JSON model view](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/modify-dashboard-settings/#view-dashboard-json-model) change all values of `datasource` key in the imported `default_grafana_dashboard.json` to the name of the variable. For example, if the variable name is `data_source`, all `"datasource"` mappings should be:

```json
"datasource": {
  "type": "prometheus",
  "uid": "$data_source"
  }
```
:::

::::


### Embed Grafana visualizations into Ray Dashboard
In order to view embedded time-series visualizations in Ray Dashboard, additional setup is needed so that:

1. The head node of the cluster is able to access Prometheus and Grafana
2. The browser of the dashboard user is able to access Grafana. 

Configure these settings using the `RAY_GRAFANA_HOST`, `RAY_PROMETHEUS_HOST`, and `RAY_GRAFANA_IFRAME_HOST` environment variables when you start the Ray clusters.

* Set `RAY_GRAFANA_HOST` to an address that the head node can use to access Grafana. Head node does health checks on Grafana on the backend.
* Set `RAY_PROMETHEUS_HOST` to an address the head node can use to access Prometheus.
* Set`RAY_GRAFANA_IFRAME_HOST` to an address that the user's browsers can use to access Grafana and embed visualizations. If not set, `RAY_GRAFANA_IFRAME_HOST` uses the value of `RAY_GRAFANA_HOST`.

For example, if the IP of the head node is 55.66.77.88 and Grafana is hosted on port 3000. Set the value to `RAY_GRAFANA_HOST=55.66.77.88:3000`.

If all the env vars are set properly, you should be able to see time-series metrics in {ref}`Ray Dashboard <observability-getting-started>`.

#### Alternate Prometheus host location
By default, we assume Prometheus is hosted at `localhost:9090`. You can choose to run Prometheus on a non-default port or on a different machine. When doing so, you should make sure that Prometheus can scrape the metrics from your ray nodes following instructions {ref}`here <scrape-metrics>`.

Then, configure `RAY_PROMETHEUS_HOST` env var properly as stated above. For example, if Prometheus is hosted at port 9000 on a node with ip 55.66.77.88, One should set the value to `RAY_PROMETHEUS_HOST=http://55.66.77.88:9000`.


#### Alternate Grafana host location
By default, we assume Grafana is hosted at `localhost:3000` You can choose to run Grafana on a non-default port or on a different machine as long as the head node and the browsers of dashboard users can access it.

If Grafana is exposed via nginx ingress on a Kubernetes cluster, the following line should be present in the Grafana ingress annotation:

```yaml
  nginx.ingress.kubernetes.io/configuration-snippet: |
      add_header X-Frame-Options SAMEORIGIN always;
```

When both Grafana and Ray cluster are on the same Kubernetes cluster, it is important to set `RAY_GRAFANA_HOST` to the external URL of the Grafana ingress.



#### User authentication for Grafana
When the Grafana instance requires user authentication, the following settings have to be in its `configuration file <https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana/>`_ to correctly embed in Ray dashboard:

```ini
  [security]
  allow_embedding = true
  cookie_secure = true
  cookie_samesite = none
```

#### Troubleshooting

##### Getting Grafana to use the Ray configurations when installed via homebrew on macOS X

With homebrew, Grafana is installed as a service that is automatically launched for you.
Therefore, to configure these services, you cannot simply pass in the config files as command line arguments.

Instead, update `/usr/local/etc/grafana/grafana.ini` file so that it matches the contents of `/tmp/ray/session_latest/metrics/grafana/grafana.ini`.

You can then start or restart the services with `brew services start grafana` and `brew services start prometheus`.

##### Grafana dashboards are not embedded in the Ray dashboard
If you're getting an error that says `RAY_GRAFANA_HOST` is not setup despite having set it up, check that:
You've included the protocol in the URL (e.g., `http://your-grafana-url.com` instead of `your-grafana-url.com`).
The URL doesn't have a trailing slash (e.g., `http://your-grafana-url.com` instead of `http://your-grafana-url.com/`).

##### Certificate Authority (CA error)
You may see a CA error if your Grafana instance is hosted behind HTTPS. Contact the Grafana service owner to properly enable HTTPS traffic.

