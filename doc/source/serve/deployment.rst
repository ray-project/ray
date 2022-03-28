.. _serve-deploy-tutorial:

===================
Deploying Ray Serve
===================
This section should help you:

- understand how Ray Serve runs on a Ray cluster beyond the basics mentioned in :doc:`core-apis`
- deploy and update your Serve application over time
- monitor your Serve application using the Ray Dashboard and logging

.. contents:: Deploying Ray Serve

.. _ray-serve-instance-lifetime:

Lifetime of a Ray Serve Instance
================================

Ray Serve instances run on top of Ray clusters and are started using :mod:`serve.start <ray.serve.start>`.
Once :mod:`serve.start <ray.serve.start>` has been called, further API calls can be used to create and update the deployments that will be used to serve your Python code (including ML models).
The Serve instance will be torn down when the script exits.

When running on a long-lived Ray cluster (e.g., one started using ``ray start`` and connected
to using ``ray.init(address="auto", namespace="serve")``, you can also deploy a Ray Serve instance as a long-running
service using ``serve.start(detached=True)``. In this case, the Serve instance will continue to
run on the Ray cluster even after the script that calls it exits. If you want to run another script
to update the Serve instance, you can run another script that connects to the same Ray cluster and makes further API calls (e.g., to create, update, or delete a deployment). Note that there can only be one detached Serve instance on each Ray cluster.

All non-detached Serve instances will be started in the current namespace that was specified when connecting to the cluster. If a namespace is specified for a detached Serve instance, it will be used. Otherwise if the current namespace is anonymous, the Serve instance will be started in the ``serve`` namespace.

If ``serve.start()`` is called again in a process in which there is already a running Serve instance, Serve will re-connect to the existing instance (regardless of whether the original instance was detached or not). To reconnect to a Serve instance that exists in the Ray cluster but not in the current process, connect to the cluster with the same namespace that was specified when starting the instance and run ``serve.start()``.

Deploying on a Single Node
==========================

While Ray Serve makes it easy to scale out on a multi-node Ray cluster, in some scenarios a single node may suit your needs.
There are two ways you can run Ray Serve on a single node, shown below.
In general, **Option 2 is recommended for most users** because it allows you to fully make use of Serve's ability to dynamically update running deployments.

1. Start Ray and deploy with Ray Serve all in a single Python file.

.. code-block:: python

  import ray
  from ray import serve
  import time

  # This will start Ray locally and start Serve on top of it.
  serve.start()

  @serve.deployment
  def my_func(request):
    return "hello"

  my_func.deploy()

  # Serve will be shut down once the script exits, so keep it alive manually.
  while True:
      time.sleep(5)
      print(serve.list_deployments())

2. First running ``ray start --head`` on the machine, then connecting to the running local Ray cluster using ``ray.init(address="auto", namespace="serve")`` in your Serve script(s) (this is the Ray namespace, not Kubernetes namespace, and you can specify any namespace that you like). You can run multiple scripts to update your deployments over time.

.. code-block:: bash

  ray start --head # Start local Ray cluster.
  serve start # Start Serve on the local Ray cluster.

.. code-block:: python

  import ray
  from ray import serve

  # This will connect to the running Ray cluster.
  ray.init(address="auto", namespace="serve")

  @serve.deployment
  def my_func(request):
    return "hello"

  my_func.deploy()


Deploying on Kubernetes
=======================

In order to deploy Ray Serve on Kubernetes, we need to do the following:

1. Start a Ray cluster on Kubernetes.
2. Expose the head node of the cluster as a `Service`_.
3. Start Ray Serve on the cluster.

There are multiple ways to start a Ray cluster on Kubernetes, see :ref:`ray-k8s-deploy` for more information.
Here, we will be using the :ref:`Ray Cluster Launcher <cluster-cloud>` tool, which has support for Kubernetes as a backend.

The cluster launcher takes in a yaml config file that describes the cluster.
Here, we'll be using the `Kubernetes default config`_ with a few small modifications.
First, we need to make sure that the head node of the cluster, where Ray Serve will run its HTTP server, is exposed as a Kubernetes `Service`_.
There is already a default head node service defined in the ``services`` field of the config, so we just need to make sure that it's exposing the right port: 8000, which Ray Serve binds on by default.

.. code-block:: yaml

  # Service that maps to the head node of the Ray cluster.
  - apiVersion: v1
    kind: Service
    metadata:
        name: ray-head
    spec:
        # Must match the label in the head pod spec below.
        selector:
            component: ray-head
        ports:
            - protocol: TCP
              # Port that this service will listen on.
              port: 8000
              # Port that requests will be sent to in pods backing the service.
              targetPort: 8000

Then, we also need to make sure that the head node pod spec matches the selector defined here and exposes the same port:

.. code-block:: yaml

  head_node:
    apiVersion: v1
    kind: Pod
    metadata:
      # Automatically generates a name for the pod with this prefix.
      generateName: ray-head-

      # Matches the selector in the service definition above.
      labels:
          component: ray-head

    spec:
      # ...
      containers:
      - name: ray-node
        # ...
        ports:
            - containerPort: 8000 # Ray Serve default port.
      # ...

The rest of the config remains unchanged for this example, though you may want to change the container image or the number of worker pods started by default when running your own deployment.
Now, we just need to start the cluster:

.. code-block:: shell

    # Start the cluster.
    $ ray up ray/python/ray/autoscaler/kubernetes/example-full.yaml

    # Check the status of the service pointing to the head node. If configured
    # properly, you should see the 'Endpoints' field populated with an IP
    # address like below. If not, make sure the head node pod started
    # successfully and the selector/labels match.
    $ kubectl -n ray describe service ray-head
      Name:              ray-head
      Namespace:         ray
      Labels:            <none>
      Annotations:       <none>
      Selector:          component=ray-head
      Type:              ClusterIP
      IP:                10.100.188.203
      Port:              <unset>  8000/TCP
      TargetPort:        8000/TCP
      Endpoints:         192.168.73.98:8000
      Session Affinity:  None
      Events:            <none>

With the cluster now running, we can run a simple script to start Ray Serve and deploy a "hello world" deployment:

  .. code-block:: python

    import ray
    from ray import serve

    # Connect to the running Ray cluster.
    ray.init(address="auto", namespace="serve")
    # Bind on 0.0.0.0 to expose the HTTP server on external IPs.
    serve.start(detached=True, http_options={"host": "0.0.0.0"})


    @serve.deployment(route_prefix="/hello")
    def hello(request):
        return "hello world"

    hello.deploy()

Save this script locally as ``deploy.py`` and run it on the head node using ``ray submit``:

  .. code-block:: shell

    $ ray submit ray/python/ray/autoscaler/kubernetes/example-full.yaml deploy.py

Now we can try querying the service by sending an HTTP request to the service from within the Kubernetes cluster.

  .. code-block:: shell

    # Get a shell inside of the head node.
    $ ray attach ray/python/ray/autoscaler/kubernetes/example-full.yaml

    # Query the Ray Serve deployment. This can be run from anywhere in the
    # Kubernetes cluster.
    $ curl -X GET http://$RAY_HEAD_SERVICE_HOST:8000/hello
    hello world

In order to expose the Ray Serve deployment externally, we would need to deploy the Service we created here behind an `Ingress`_ or a `NodePort`_.
Please refer to the Kubernetes documentation for more information.

.. _`Kubernetes default config`: https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/example-full.yaml
.. _`Service`: https://kubernetes.io/docs/concepts/services-networking/service/
.. _`Ingress`: https://kubernetes.io/docs/concepts/services-networking/ingress/
.. _`NodePort`: https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types



Health Checking
===============
By default, each actor making up a Serve deployment is health checked and restarted on failure.


.. note::

   User-defined health checks are experimental and may be subject to change before the interface is stabilized. If you have any feedback or run into any issues or unexpected behaviors, please file an issue on GitHub.

You can customize this behavior to perform an application-level health check or to adjust the frequency/timeout.
To define a custom healthcheck, define a ``check_health`` method on your deployment class.
This method should take no arguments and return no result, raising an exception if the replica should be considered unhealthy.
You can also customize how frequently the health check is run and the timeout when a replica will be deemed unhealthy if it hasn't responded in the deployment options.

  .. code-block:: python

    @serve.deployment(_health_check_period_s=10, _health_check_timeout_s=30)
    class MyDeployment:
        def __init__(self, db_addr: str):
            self._my_db_connection = connect_to_db(db_addr)

        def __call__(self, request):
            return self._do_something_cool()

        # Will be called by Serve to check the health of the replica.
        def check_health(self):
            if not self._my_db_connection.is_connected():
                # The specific type of exception is not important.
                raise RuntimeError("uh-oh, DB connection is broken.")

.. tip::

    You can use the Serve CLI command ``serve status`` to get status info
    about your live deployments. The CLI was included with Serve when you did
    ``pip install "ray[serve]"``. If you're checking your deployments on a
    remote Ray cluster, make sure to include the Ray cluster's dashboard address
    in the command: ``serve status --address [dashboard_address]``.

Failure Recovery
================
Ray Serve is resilient to any component failures within the Ray cluster out of the box.
You can checkout the detail of how process and worker node failure handled at :ref:`serve-ft-detail`.
However, when the Ray head node goes down, you would need to recover the state by creating a new
Ray cluster and re-deploys all Serve deployments into that cluster.

.. note::
  Ray currently cannot survive head node failure and we recommend using application specific
  failure recovery solutions. Although Ray is not currently highly available (HA), it is on
  the long term roadmap and being actively worked on.

Ray Serve added an experimental feature to help recovering the state.
This features enables Serve to write all your deployment configuration and code into a storage location.
Upon Ray cluster failure and restarts, you can simply call Serve to reconstruct the state.

Here is how to use it:

.. warning::
  The API is experimental and subject to change. We welcome you to test it out
  and leave us feedback through github issues or discussion forum!


You can use both the start argument and the CLI to specify it:

.. code-block:: python

    serve.start(_checkpoint_path=...)

or

.. code-block:: shell

    serve start --checkpoint-path ...


The checkpoint path argument accepts the following format:

- ``file://local_file_path``
- ``s3://bucket/path``
- ``gs://bucket/path``
- ``custom://importable.custom_python.Class/path``

While we have native support for on disk, AWS S3, and Google Cloud Storage (GCS), there is no reason we cannot support more.

In Kubernetes environment, we recommend using `Persistent Volumes`_ to create a disk and mount it into the Ray head node.
For example, you can provision Azure Disk, AWS Elastic Block Store, or GCP Persistent Disk using the K8s `Persistent Volumes`_ API.
Alternatively, you can also directly write to object store like S3.

You can easily try to plug into your own implementation using the ``custom://`` path and inherit the `KVStoreBase`_ class.
Feel free to open new github issues and contribute more storage backends!

.. _`Persistent Volumes`: https://kubernetes.io/docs/concepts/storage/persistent-volumes/

.. _`KVStoreBase`: https://github.com/ray-project/ray/blob/master/python/ray/serve/storage/kv_store_base.py

.. _serve-monitoring:

Monitoring
==========

Ray Dashboard
-------------

A high-level way to monitor your Ray Serve deployment (or any Ray application) is via the Ray Dashboard.
See the `Ray Dashboard documentation <../ray-dashboard.html>`__ for a detailed overview, including instructions on how to view the dashboard.

Below is an example of what the Ray Dashboard might look like for a Serve deployment:

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/serve-dashboard.png
    :align: center

Here you can see the Serve controller actor, an HTTP proxy actor, and all of the replicas for each Serve deployment.
To learn about the function of the controller and proxy actors, see the `Serve Architecture page <architecture.html>`__.
In this example pictured above, we have a single-node cluster with a deployment named Counter with ``num_replicas=2``.

Logging
-------

Logging in Ray Serve uses Python's standard logging facility.

.. note::

  For an general overview of logging in Ray, see `Ray Logging <../ray-logging.html>`__.

Tracing Backends and Replicas
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When looking through log files of your Ray Serve application, it is useful to know which deployment and replica each log line originated from.
To automatically include the current deployment and replica in your logs, simply call
``logger = logging.getLogger("ray")``, and use ``logger`` within your deployment code:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_logger.py

Querying the above deployment will produce a log line like the following:

.. code-block:: bash

  (pid=42161) 2021-02-26 11:05:21,709     INFO snippet_logger.py:13 -- Some info! component=serve deployment=f replica=f#jZlnUI

To write your own custom logger using Python's ``logging`` package, use the following method:

.. autofunction:: ray.serve.get_replica_context

Ray Serve logs can be ingested by your favorite external logging agent.  Ray logs from the current session are exported to the directory `/tmp/ray/session_latest/logs` and remain there until the next session starts.

Tutorial: Ray Serve with Loki
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Here is a quick walkthrough of how to explore and filter your logs using `Loki <https://grafana.com/oss/loki/>`__.
Setup and configuration is very easy on Kubernetes, but in this tutorial we'll just set things up manually.

First, install Loki and Promtail using the instructions on https://grafana.com.
It will be convenient to save the Loki and Promtail executables in the same directory, and to navigate to this directory in your terminal before beginning this walkthrough.

Now let's get our logs into Loki using Promtail.

Save the following file as ``promtail-local-config.yaml``:

.. code-block:: yaml

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
      __path__: /tmp/ray/session_latest/logs/*.*

The relevant part for Ray is the ``static_configs`` field, where we have indicated the location of our log files with ``__path__``.
The expression ``*.*`` will match all files, but not directories, which cause an error with Promtail.

We will run Loki locally.  Grab the default config file for Loki with the following command in your terminal:

.. code-block:: shell

  wget https://raw.githubusercontent.com/grafana/loki/v2.1.0/cmd/loki/loki-local-config.yaml

Now start Loki:

.. code-block:: shell

  ./loki-darwin-amd64 -config.file=loki-local-config.yaml

Here you may need to replace ``./loki-darwin-amd64`` with the path to your Loki executable file, which may have a different name depending on your operating system.

Start Promtail and pass in the path to the config file we saved earlier:

.. code-block:: shell

  ./promtail-darwin-amd64 -config.file=promtail-local-config.yaml

As above, you may need to replace ``./promtail-darwin-amd64`` with the appropriate filename and path.


Now we are ready to start our Ray Serve deployment.  Start a long-running Ray cluster and Ray Serve instance in your terminal:

.. code-block:: shell

  ray start --head
  serve start

Now run the following Python script to deploy a basic Serve deployment with a Serve deployment logger:

.. literalinclude:: ../../../python/ray/serve/examples/doc/deployment_logger.py

Now `install and run Grafana <https://grafana.com/docs/grafana/latest/installation/>`__ and navigate to ``http://localhost:3000``, where you can log in with the default username "admin" and default password "admin".
On the welcome page, click "Add your first data source" and click "Loki" to add Loki as a data source.

Now click "Explore" in the left-side panel.  You are ready to run some queries!

To filter all these Ray logs for the ones relevant to our deployment, use the following `LogQL <https://grafana.com/docs/loki/latest/logql/>`__ query:

.. code-block:: shell

  {job="ray"} |= "deployment=Counter"

You should see something similar to the following:

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/serve/loki-serve.png
    :align: center

Metrics
-------

Ray Serve exposes important system metrics like the number of successful and
errored requests through the `Ray metrics monitoring infrastructure <../ray-metrics.html>`__. By default,
the metrics are exposed in Prometheus format on each node.

The following metrics are exposed by Ray Serve:

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

To see this in action, run ``ray start --head --metrics-export-port=8080`` in your terminal, and then run the following script:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_metrics.py

In your web browser, navigate to ``localhost:8080``.
In the output there, you can search for ``serve_`` to locate the metrics above.
The metrics are updated once every ten seconds, and you will need to refresh the page to see the new values.

For example, after running the script for some time and refreshing ``localhost:8080`` you might see something that looks like::

  ray_serve_deployment_processing_latency_ms_count{...,deployment="f",...} 99.0
  ray_serve_deployment_processing_latency_ms_sum{...,deployment="f",...} 99279.30498123169

which indicates that the average processing latency is just over one second, as expected.

You can even define a :ref:`custom metric <application-level-metrics>` to use in your deployment, and tag it with the current deployment or replica.
Here's an example:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_custom_metric.py
  :start-after: __custom_metrics_deployment_start__
  :end-before: __custom_metrics_deployment_end__

See the
:ref:`Ray Metrics documentation <ray-metrics>` for more details, including instructions for scraping these metrics using Prometheus.
