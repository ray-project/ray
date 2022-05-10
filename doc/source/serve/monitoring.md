(serve-monitoring)=

# Debugging & Monitoring

This section should help you understand how to debug and monitor your Serve application.

## Ray Dashboard

A high-level way to monitor your Ray Serve application is via the Ray Dashboard.
See the [Ray Dashboard documentation](ray-dashboard) for a detailed overview, including instructions on how to view the dashboard.

Below is an example of what the Ray Dashboard might look like for a Serve deployment:

```{image} https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/serve-dashboard.png
:align: center
```

Here you can see the Serve controller actor, an HTTP proxy actor, and all of the replicas for each Serve deployment.
To learn about the function of the controller and proxy actors, see the [Serve Architecture page](serve-architecture).
In this example pictured above, we have a single-node cluster with a deployment named Counter with `num_replicas=2`.

## Logging

:::{note}
For an overview of logging in Ray, see [Ray Logging](ray-logging).
:::

Ray Serve uses Python's standard `logging` facility with the `"ray.serve"` named logger.
By default, logs are emitted from actors both to `stderr` and on disk on each node at `/tmp/ray/session_latest/logs/serve/`.
This includes both system-level logs from the Serve controller and HTTP proxy as well as access logs and custom user logs produced from within deployment replicas.

In development, logs are streamed to the driver Ray program (the program that calls `.deploy()` or `serve.run`, or the `serve run` CLI command) that deployed the deployments, so it's most convenient to keep the driver running for debugging.
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

```python
import logging

logger = logging.getLogger("ray.serve")

@serve.deployment(route_prefix="/")
class SayHello:
    def __call__(self, *args):
        logger.info("Hello world!")
        return "hi"

SayHello.deploy()
```

Running this code block, we first get some log messages from the controller saying that a new replica of the deployment is being created:

```bash
(ServeController pid=67312) INFO 2022-04-02 09:16:13,323 controller 67312 deployment_state.py:1198 - Adding 1 replicas to deployment 'SayHello'.
```

Then when we query the deployment, we get both a default access log as well as our custom `"Hello world!"` message.
Note that these log lines are tagged with the deployment name followed by a unique identifier for the specific replica.
These can be parsed by a logging stack such as ELK or Loki to enable searching logs by deployment and replica.

```bash
handle = SayHello.get_handle()
ray.get(handle.remote())
(SayHello pid=67352) INFO 2022-04-02 09:20:08,975 SayHello SayHello#LBINMh <ipython-input-4-1e8854e5c9ba>:8 - Hello world!
(SayHello pid=67352) INFO 2022-04-02 09:20:08,975 SayHello SayHello#LBINMh replica.py:466 - HANDLE __call__ OK 0.3ms
```

Querying the deployment over HTTP produces a similar access log message from the HTTP proxy:

```bash
curl -X GET http://localhost:8000/
(HTTPProxyActor pid=67315) INFO 2022-04-02 09:20:08,976 http_proxy 127.0.0.1 http_proxy.py:310 - GET / 200 2.6ms
(SayHello pid=67352) INFO 2022-04-02 09:20:08,975 SayHello SayHello#LBINMh <ipython-input-4-1e8854e5c9ba>:8 - Hello world!
(SayHello pid=67352) INFO 2022-04-02 09:20:08,975 SayHello SayHello#LBINMh replica.py:466 - HANDLE __call__ OK 0.3ms
```

You can also be able to view all of these log messages in the files in `/tmp/ray/session_latest/logs/serve/`.

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

Now we are ready to start our Ray Serve deployment.  Start a long-running Ray cluster and Ray Serve instance in your terminal:

```shell
ray start --head
serve start
```

Now run the following Python script to deploy a basic Serve deployment with a Serve deployment logger:

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
   * - ``serve_deployment_queuing_latency_ms``
     - The latency for queries in the replica's queue waiting to be processed.
   * - ``serve_deployment_processing_latency_ms``
     - The latency for queries to be processed.
   * - ``serve_replica_queued_queries``
     - The current number of queries queued in the deployment replicas.
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

To see this in action, run `ray start --head --metrics-export-port=8080` in your terminal, and then run the following script:

```{literalinclude} ../../../python/ray/serve/examples/doc/snippet_metrics.py
```

In your web browser, navigate to `localhost:8080`.
In the output there, you can search for `serve_` to locate the metrics above.
The metrics are updated once every ten seconds, and you will need to refresh the page to see the new values.

For example, after running the script for some time and refreshing `localhost:8080` you might see something that looks like:

```
ray_serve_deployment_processing_latency_ms_count{...,deployment="f",...} 99.0
ray_serve_deployment_processing_latency_ms_sum{...,deployment="f",...} 99279.30498123169
```

which indicates that the average processing latency is just over one second, as expected.

You can even define a [custom metric](application-level-metrics) to use in your deployment, and tag it with the current deployment or replica.
Here's an example:

```{literalinclude} ../../../python/ray/serve/examples/doc/snippet_custom_metric.py
:end-before: __custom_metrics_deployment_end__
:start-after: __custom_metrics_deployment_start__
```

See the
[Ray Metrics documentation](ray-metrics) for more details, including instructions for scraping these metrics using Prometheus.
