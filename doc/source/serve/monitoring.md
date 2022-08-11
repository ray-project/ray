(serve-monitoring)=

# Monitoring Ray Serve

This section should help you debug and monitor your Serve applications by:

* viewing the Ray dashboard
* using Ray logging and Loki
* inspecting built-in Ray Serve metrics

## Ray dashboard

You can use the Ray dashboard to get a high-level overview of your Ray cluster and Ray Serve application's states.
This includes details such as:
* The number of deployment replicas currently running
* Logs for your Serve controller, deployment replicas, and HTTP proxies
* The Ray nodes (i.e. machines) running in your Ray cluster

You can access the Ray dashboard at port 8265 at your cluster's URI.
For example, if you're running Ray Serve on a local Ray cluster, you can access the dashboard by going to this address in your browser:

```
http://localhost:8265
```

We can view important information about our application here.
For example, we can inspect our deployment replicas by navigating to the Ray dashboard's "Actors" tab while our Serve application is running:

```{image} https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/serve-dashboard-2-replicas.png
:align: center
```

In this example, there's a single-node cluster running a deployment named `Translator`. The Serve application uses four [Ray actors](actor-guide):

* 1 Serve controller
* 1 HTTP proxy
* 2 `Translator` deployment replicas

This page includes additional useful information like each actor's process ID (PID) and a link to each actor's logs, which includes their `logging` and print statments. You can also see whether any particular actor is alive or dead to help you debug potential cluster failures.

:::{tip}
To learn more about the Serve controller actor, the HTTP proxy actor(s), the deployment replicas, and how they all work together, check out the [Serve Architecture](serve-architecture) documentation.
:::

For a detailed overview of the Ray dashboard, see the [dashboard documentation](ray-dashboard).

## Ray logging

Ray Serve uses Python's standard `logging` module with a logger named `"ray.serve"`.
By default, logs are emitted from actors both to `stderr` and on disk on each node at `/tmp/ray/session_latest/logs/serve/`.
This includes both system-level logs from the Serve controller and HTTP proxy as well as access logs and custom user logs produced from within deployment replicas.

In development, logs are streamed to the driver Ray program (the Python script that calls `serve.run()` or the `serve run` CLI command), so it's convenient to keep the driver running while debugging.

For example, let's run a basic Serve application and view the logs that it emits.

Next, let's create a simple deployment that logs a custom log message when it's queried:

```{literalinclude} doc_code/monitoring/monitoring.py
:start-after: __start__
:end-before: __end__
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

For a detailed overview of logging in Ray, see [Ray Logging](ray-logging).

### Filtering logs with Loki

You can explore and filter your logs using [Loki](https://grafana.com/oss/loki/).
Setup and configuration is straightforward on Kubernetes, but as a tutorial, let's set up Loki manually.

For this walkthrough, you need both Loki and Promtail, which are both supported by [Grafana Labs](https://grafana.com). Follow the installation instructions at Grafana's website to get executables for [Loki](https://grafana.com/docs/loki/latest/installation/) and [Promtail](https://grafana.com/docs/loki/latest/clients/promtail/).
For convenience, save the Loki and Promtail executables in the same directory, and then navigate to this directory in your terminal.

Now let's get your logs into Loki using Promtail.

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

The relevant part for Ray Serve is the `static_configs` field, where we have indicated the location of our log files with `__path__`.
The expression `*.*` will match all files, but not directories, which cause an error with Promtail.

We'll run Loki locally.  Grab the default config file for Loki with the following command in your terminal:

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

Once again, you may need to replace `./promtail-darwin-amd64` with your Promtail executable.

Run the following Python script to deploy a basic Serve deployment with a Serve deployment logger and to make some requests:

```{literalinclude} doc_code/monitoring/deployment_logger.py
:start-after: __start__
:end-before: __end__
:language: python
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

You can use Loki to filter your Ray Serve logs and gather insights quicker.

## Built-in Ray Serve metrics

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

```{literalinclude} doc_code/monitoring/metrics_snippet.py
:start-after: __start__
:end-before: __end__
:language: python
```

The requests will loop and can be canceled with `ctrl-c`. 

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

```{literalinclude} doc_code/monitoring/custom_metric_snippet.py
:start-after: __start__
:end-before: __end__
```

And the emitted logs:

```
# HELP ray_my_counter The number of odd-numbered requests to this deployment.
# TYPE ray_my_counter gauge
ray_my_counter{..., deployment="MyDeployment"} 5.0
```

See the
[Ray Metrics documentation](ray-metrics) for more details, including instructions for scraping these metrics using Prometheus.
