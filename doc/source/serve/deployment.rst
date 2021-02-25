===================
Deploying Ray Serve
===================

In the :doc:`core-apis`, you saw some of the basics of how to write serve applications.
This section will dive deeper into how Ray Serve runs on a Ray cluster and how you're able
to deploy and update your serve application over time.

.. contents:: Deploying Ray Serve

.. _serve-deploy-tutorial:

Lifetime of a Ray Serve Instance
================================

Ray Serve instances run on top of Ray clusters and are started using :mod:`serve.start <ray.serve.start>`.
:mod:`serve.start <ray.serve.start>` returns a :mod:`Client <ray.serve.api.Client>` object that can be used to create the backends and endpoints
that will be used to serve your Python code (including ML models).
The Serve instance will be torn down when the client object goes out of scope or the script exits.

When running on a long-lived Ray cluster (e.g., one started using ``ray start`` and connected
to using ``ray.init(address="auto")``, you can also deploy a Ray Serve instance as a long-running
service using ``serve.start(detached=True)``. In this case, the Serve instance will continue to
run on the Ray cluster even after the script that calls it exits. If you want to run another script
to update the Serve instance, you can run another script that connects to the Ray cluster and then
calls :mod:`serve.connect <ray.serve.connect>`. Note that there can only be one detached Serve instance on each Ray cluster.

Deploying on a Single Node
==========================

While Ray Serve makes it easy to scale out on a multi-node Ray cluster, in some scenarios a single node may suite your needs.
There are two ways you can run Ray Serve on a single node, shown below.
In general, **Option 2 is recommended for most users** because it allows you to fully make use of Serve's ability to dynamically update running backends.

1. Start Ray and deploy with Ray Serve all in a single Python file.

.. code-block:: python

  import ray
  from ray import serve

  # This will start Ray locally and start Serve on top of it.
  client = serve.start()

  def my_backend_func(request):
    return "hello"

  client.create_backend("my_backend", my_backend_func)

  # Serve will be shut down once the script exits, so keep it alive manually.
  while True:
      time.sleep(5)
      print(serve.list_backends())

2. First running ``ray start --head`` on the machine, then connecting to the running local Ray cluster using ``ray.init(address="auto")`` in your Serve script(s). You can run multiple scripts to update your backends over time.

.. code-block:: bash

  ray start --head # Start local Ray cluster.
  serve start # Start Serve on the local Ray cluster.

.. code-block:: python

  import ray
  from ray import serve

  # This will connect to the running Ray cluster.
  ray.init(address="auto")
  client = serve.connect()

  def my_backend_func(request):
    return "hello"

  client.create_backend("my_backend", my_backend_func)

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

With the cluster now running, we can run a simple script to start Ray Serve and deploy a "hello world" backend:

  .. code-block:: python

    import ray
    from ray import serve

    # Connect to the running Ray cluster.
    ray.init(address="auto")
    # Bind on 0.0.0.0 to expose the HTTP server on external IPs.
    client = serve.start(detached=True, http_options={"host": "0.0.0.0"})

    def hello():
        return "hello world"

    client.create_backend("hello_backend", hello)
    client.create_endpoint("hello_endpoint", backend="hello_backend", route="/hello")

Save this script locally as ``deploy.py`` and run it on the head node using ``ray submit``:

  .. code-block:: shell

    $ ray submit ray/python/ray/autoscaler/kubernetes/example-full.yaml deploy.py

Now we can try querying the service by sending an HTTP request to the service from within the Kubernetes cluster.

  .. code-block:: shell

    # Get a shell inside of the head node.
    $ ray attach ray/python/ray/autoscaler/kubernetes/example-full.yaml

    # Query the Ray Serve endpoint. This can be run from anywhere in the
    # Kubernetes cluster.
    $ curl -X GET http://$RAY_HEAD_SERVICE_HOST:8000/hello
    hello world

In order to expose the Ray Serve endpoint externally, we would need to deploy the Service we created here behind an `Ingress`_ or a `NodePort`_.
Please refer to the Kubernetes documentation for more information.

.. _`Kubernetes default config`: https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/example-full.yaml
.. _`Service`: https://kubernetes.io/docs/concepts/services-networking/service/
.. _`Ingress`: https://kubernetes.io/docs/concepts/services-networking/ingress/
.. _`NodePort`: https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types


Monitoring
==========

Ray Dashboard
-------------

A high-level way to monitor your Ray Serve deployment (or any Ray application) is via the Ray Dashboard.
See the `Ray Dashboard documentation <../ray-dashboard.html>`__ for a detailed overview, including instructions on how to view the dashboard.

Below is an example of what the Ray Dashboard might look like for a Serve deployment:

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/serve-dashboard.png
    :align: center

Here you can see the Serve controller actor, an HTTP proxy actor, and all of the replicas for each Serve backend in the deployment.
To learn about the function of the controller and proxy actors, see the `Serve Architecture page <architecture.html>`__.
In this example pictured above, we have a single-node cluster with a backend class called Counter with ``num_replicas=2`` in its :class:`~ray.serve.BackendConfig`.

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
   * - ``serve_backend_request_counter``
     - The number of queries that have been processed in this replica.
   * - ``serve_backend_error_counter``
     - The number of exceptions that have occurred in the backend.
   * - ``serve_backend_replica_starts``
     - The number of times this replica has been restarted due to failure.
   * - ``serve_backend_queuing_latency_ms``
     - The latency for queries in the replica's queue waiting to be processed or batched.
   * - ``serve_backend_processing_latency_ms``
     - The latency for queries to be processed.
   * - ``serve_replica_queued_queries``
     - The current number of queries queued in the backend replicas.
   * - ``serve_replica_processing_queries``
     - The current number of queries being processed.
   * - ``serve_num_http_requests``
     - The number of HTTP requests processed.
   * - ``serve_num_router_requests``
     - The number of requests processed by the router.
   * - ``serve_handle_request_counter``
     - The number of requests processed by this ServeHandle.
   * - ``backend_queued_queries`` 
     - The number of queries for this backend waiting to be assigned to a replica.

To see this in action, run ``ray start --head --metrics-export-port=8080`` in your terminal, and then run the following script:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_metrics.py

In your web browser, navigate to ``localhost:8080``.
In the output there, you can search for ``serve_`` to locate the metrics above.
The metrics are updated once every ten seconds, and you will need to refresh the page to see the new values.

For example, after running the script for some time and refreshing ``localhost:8080`` you might see something that looks like::

  ray_serve_backend_processing_latency_ms_count{...,backend="f",...} 99.0
  ray_serve_backend_processing_latency_ms_sum{...,backend="f",...} 99279.30498123169

which indicates that the average processing latency is just over one second, as expected.

You can even define a `custom metric <..ray-metrics.html#custom-metrics>`__ to use in your backend, and tag it with the current backend or replica.
Here's an example:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_custom_metric.py
  :lines: 11-23

See the
`Ray Metrics documentation <../ray-metrics.html>`__ for more details, including instructions for scraping these metrics using Prometheus.
