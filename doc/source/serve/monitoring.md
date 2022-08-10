(serve-monitoring)=

# Monitoring Ray Serve

This section should help you understand how to debug and monitor your Serve application.
There are three main ways to do this:
Using the Ray Dashboard, using Ray logging, and using built-in Ray Serve metrics.

## Ray Dashboard

A high-level way to monitor your Ray Serve application is via the Ray Dashboard.
See the [Ray Dashboard documentation](ray-dashboard) for a detailed overview, including instructions on how to view the dashboard.

Below is an example of what the "Actors" tab of the Ray Dashboard might look like for a running Serve application:

```{image} https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/serve-dashboard-2-replicas.png
:align: center
```

Here you can see the Serve controller actor, an HTTP proxy actor, and all of the replicas for each Serve deployment.
To learn about the function of the controller and proxy actors, see the [Serve Architecture page](serve-architecture).
In this example pictured above, we have a single-node cluster with a deployment named `Translator` with `num_replicas=2`.

## Logging

:::{note}
For an overview of logging in Ray, see [Ray Logging](ray-logging).
:::

Ray Serve uses Python's standard `logging` facility with the logger named `"ray.serve"`.
By default, logs are emitted from actors both to `stderr` and on disk on each node at `/tmp/ray/session_latest/logs/serve/`.
This includes both system-level logs from the Serve controller and HTTP proxy as well as access logs and custom user logs produced from within deployment replicas.

In development, logs are streamed to the driver Ray program (the Python script that calls `serve.run()` or the `serve run` CLI command), so it's most convenient to keep the driver running for debugging.
For example, let's run a basic Serve application and view the logs that are emitted.
You can run this in an interactive shell like IPython to follow along.

First we call `serve.start()`:

```python
from ray import serve

serve.start()
```

This produces a few INFO-level log messages about startup from the Serve controller.

```bash
2022-04-02 09:10:49,906 INFO services.py:1460 -- View the Ray dashboard at http://127.0.0.1:8265
(ServeController pid=67312) INFO 2022-04-02 09:10:51,386 controller 67312 checkpoint_path.py:17 - Using RayInternalKVStore for controller checkpoint and recovery.
(ServeController pid=67312) INFO 2022-04-02 09:10:51,492 controller 67312 http_state.py:108 - Starting HTTP proxy with name 'SERVE_CONTROLLER_ACTOR:xlehoa:SERVE_PROXY_ACTOR-node:127.0.0.1-0' on node 'node:127.0.0.1-0' listening on '127.0.0.1:8000'
```

Next, let's create a simple deployment that logs a custom log message when it's queried:

```{literalinclude} ../serve/doc_code/monitoring.py
:start-after: __start_monitoring__
:end-before: __end_monitoring__
:language: python
:linenos: true
```

Running this code block, we first get some log messages from the controller saying that a new replica of the deployment is being created:

```bash
$ python monitoring.py
2022-08-02 16:16:21,498 INFO scripts.py:294 -- Deploying from import path: "monitoring:say_hello".
2022-08-02 16:16:24,141 INFO worker.py:1481 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265.
(ServeController pid=71139) INFO 2022-08-02 16:16:28,542 controller 71139 http_state.py:123 - Starting HTTP proxy with name 'SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR-f68e29b2048526b2d6ef5ba69e2b35d44b44d12ff3c85fc6132e9381' on node 'f68e29b2048526b2d6ef5ba69e2b35d44b44d12ff3c85fc6132e9381' listening on '127.0.0.1:8000'
(HTTPProxyActor pid=71150) INFO:     Started server process [71150]
(ServeController pid=71139) INFO 2022-08-02 16:16:29,881 controller 71139 deployment_state.py:1232 - Adding 1 replicas to deployment 'SayHello'.
2022-08-02 16:16:31,889 SUCC scripts.py:315 -- Deployed successfully.
```

Then when we query the deployment, we get both a default access log as well as our custom `"Hello world!"` message.

```python
import requests
requests.get("http://localhost:8000/")
```

```
(HTTPProxyActor pid=71150) INFO 2022-08-02 16:17:02,502 http_proxy 127.0.0.1 http_proxy.py:315 - GET / 200 3.5ms
(ServeReplica:SayHello pid=71162) INFO 2022-08-02 16:17:02,501 SayHello SayHello#kKGBoa monitoring.py:13 - Hello world!
(ServeReplica:SayHello pid=71162) INFO 2022-08-02 16:17:02,501 SayHello SayHello#kKGBoa replica.py:482 - HANDLE __call__ OK 0.3ms
```

Note that these log lines are tagged with the deployment name followed by a unique identifier for the specific replica.
These can be parsed by a logging stack such as ELK or Loki to enable searching logs by deployment and replica.  These logs are stored at `/tmp/ray/session_latest/logs/serve/`.

To silence the replica-level logs or otherwise configure logging, configure the `"ray.serve"` logger *from inside the deployment constructor:*

```python
import logging

logger = logging.getLogger("ray.serve")

@serve.deployment
class Silenced:
    def __init__(self):
        logger.setLevel(logging.ERROR)
```

This will prevent the replica INFO-level logs from being written to STDOUT or to files on disk.
You can also use your own custom logger, in which case you'll need to configure the behavior to write to STDOUT/STDERR, files on disk, or both.

### Tutorial: Ray Serve with Loki

Here is a quick walkthrough of how to explore and filter your logs using [Loki](https://grafana.com/oss/loki/).
Setup and configuration is very easy on Kubernetes, but in this tutorial we'll just set things up manually.

First, install Loki and Promtail using the instructions on <https://grafana.com>.
It will be convenient to save the Loki and Promtail executables in the same directory, and to navigate to this directory in your terminal before beginning this walkthrough.

Now let's get our logs into Loki using Promtail.

Save the following file as `promtail-local-config.yaml`:

```yaml
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://localhost:3100/loki/api/v1/push

scrape_configs:
- job_name: ray
static_configs:
  - labels:
    job: ray
    __path__: /tmp/ray/session_latest/logs/serve/*.*
```

The relevant part for Ray is the `static_configs` field, where we have indicated the location of our log files with `__path__`.
The expression `*.*` will match all files, but not directories, which cause an error with Promtail.

We will run Loki locally.  Grab the default config file for Loki with the following command in your terminal:

```shell
wget https://raw.githubusercontent.com/grafana/loki/v2.1.0/cmd/loki/loki-local-config.yaml
```

Now start Loki:

```shell
./loki-darwin-amd64 -config.file=loki-local-config.yaml
```

Here you may need to replace `./loki-darwin-amd64` with the path to your Loki executable file, which may have a different name depending on your operating system.

Start Promtail and pass in the path to the config file we saved earlier:

```shell
./promtail-darwin-amd64 -config.file=promtail-local-config.yaml
```

As above, you may need to replace `./promtail-darwin-amd64` with the appropriate filename and path.

Run the following Python script to deploy a basic Serve deployment with a Serve deployment logger and make some requests:

```{literalinclude} ../../../python/ray/serve/examples/doc/deployment_logger.py
```

Now [install and run Grafana](https://grafana.com/docs/grafana/latest/installation/) and navigate to `http://localhost:3000`, where you can log in with the default username "admin" and default password "admin".
On the welcome page, click "Add your first data source" and click "Loki" to add Loki as a data source.

Now click "Explore" in the left-side panel.  You are ready to run some queries!

To filter all these Ray logs for the ones relevant to our deployment, use the following [LogQL](https://grafana.com/docs/loki/latest/logql/) query:

```shell
{job="ray"} |= "Counter"
```

You should see something similar to the following:

```{image} https://raw.githubusercontent.com/ray-project/Images/master/docs/serve/loki-serve.png
:align: center
```

## Metrics

Ray Serve exposes important system metrics like the number of successful and
errored requests through the [Ray metrics monitoring infrastructure](ray-metrics). By default,
the metrics are exposed in Prometheus format on each node.

The following metrics are exposed by Ray Serve:

```{eval-rst}
.. list-table::
   :header-rows: 1

   * - Name
     - Description
   * - ``serve_deployment_request_counter``
     - The number of queries that have been processed in this replica.
   * - ``serve_deployment_error_counter``
     - The number of exceptions that have occurred in the deployment.
   * - ``serve_deployment_replica_starts``
     - The number of times this replica has been restarted due to failure.
   * - ``serve_deployment_processing_latency_ms``
     - The latency for queries to be processed.
   * - ``serve_replica_processing_queries``
     - The current number of queries being processed.
   * - ``serve_num_http_requests``
     - The number of HTTP requests processed.
   * - ``serve_num_http_error_requests``
     - The number of non-200 HTTP responses.
   * - ``serve_num_router_requests``
     - The number of requests processed by the router.
   * - ``serve_handle_request_counter``
     - The number of requests processed by this ServeHandle.
   * - ``serve_deployment_queued_queries``
     - The number of queries for this deployment waiting to be assigned to a replica.
   * - ``serve_num_deployment_http_error_requests``
     - The number of non-200 HTTP responses returned by each deployment.
```

To see this in action, first run the following command to start Ray and set up the metrics export port:

```bash
ray start --head --metrics-export-port=8080
```

Then run the following script:

```{literalinclude} ../../../python/ray/serve/examples/doc/snippet_metrics.py
```

The requests will loop and can be canceled with `Ctrl-C`. 

While this is running, in your web browser navigate to `localhost:8080`.
In the output there, you can search for `serve_` to locate the metrics above.
The metrics are updated once every ten seconds, and you will need to refresh the page to see the new values.

For example, after running the script for some time and refreshing `localhost:8080` you should be able to find metrics similar to the following:

```
ray_serve_deployment_processing_latency_ms_count{..., replica="sleeper#jtzqhX"} 48.0
ray_serve_deployment_processing_latency_ms_sum{..., replica="sleeper#jtzqhX"} 48160.6719493866
```

which indicates that the average processing latency is just over one second, as expected.

You can even define a [custom metric](application-level-metrics) to use in your deployment, and tag it with the current deployment or replica.
Here's an example:

```{literalinclude} ../../../python/ray/serve/examples/doc/snippet_custom_metric.py
:end-before: __custom_metrics_deployment_end__
:start-after: __custom_metrics_deployment_start__
```

And the emitted logs:

```
# HELP ray_my_counter The number of odd-numbered requests to this deployment.
# TYPE ray_my_counter gauge
ray_my_counter{..., deployment="MyDeployment"} 5.0
```

See the
[Ray Metrics documentation](ray-metrics) for more details, including instructions for scraping these metrics using Prometheus.
