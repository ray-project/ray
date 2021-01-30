===================
Deploying Ray Serve
===================

In the :doc:`key-concepts`, you saw some of the basics of how to write serve applications.
This section will dive a bit deeper into how Ray Serve runs on a Ray cluster and how you're able
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

Deploying a Model with Ray Serve
================================

Setup: Training a Model
-----------------------

Make sure you install `Scikit-learn <https://scikit-learn.org/stable/>`_.

Place the following in a python script and run it. In this example we're training
a model and saving it to disk for us to load into our Ray Serve app.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_import_train_begin__
    :end-before: __doc_import_train_end__

As discussed in other :doc:`tutorials/index`, we can use any framework to build these models. In general,
you'll just want to have the ability to persist these models to disk.

Now that we've trained that model and saved it to disk (keep in mind this could also be a service like S3),
we'll need to create a backend to serve the model.

Creating a Model and Serving it
-------------------------------

In the following snippet we will complete two things:

1. Define a servable model by instantiating a class and defining the ``__call__`` method.
2. Start a local Ray cluster and a Ray Serve instance on top of it
   (:mod:`serve.start(...) <ray.serve.start>`).


You can see that defining the model is straightforward and simple, we're simply instantiating
the model like we might a typical Python class.

Configuring our model to accept traffic is specified via :mod:`client.set_traffic <ray.serve.api.Client.set_traffic>` after we created
a backend in serve for our model (and versioned it with a string).

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_create_deploy_begin__
    :end-before: __doc_create_deploy_end__

What serve does when we run this code is store the model as a Ray actor
and route traffic to it as the endpoint is queried, in this case over HTTP.
Note that in order for this endpoint to be accessible from other machines, we
need to specify ``http_options={"host": "0.0.0.0"}`` in :mod:`serve.start <ray.serve.start>` like we did here.

Now let's query our endpoint to see the result.

Querying our Endpoint
---------------------

We'll use the requests library to query our endpoint and be able to get a result.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__


Now that we defined a model and have it running on our Ray cluster. Let's proceed with updating
this model with a new set of code.

Updating Your Model Over Time
=============================

Updating our model is as simple as deploying the first one. While the code snippet includes
a lot of information, all that we're doing is we are defining a new model, saving it, then loading
it into serve. The key lines are at the end.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_create_deploy_2_begin__
    :end-before: __doc_create_deploy_2_end__

Consequentially, since Ray Serve runs as a service, all we need to tell it is that (a) there's a new model
and (b) how much traffic we should send to that model (and from what endpoint).

We do that with the line at the end of the code snippet, which allows us to split traffic between
these two models.

.. code::

    client.set_traffic("iris_classifier", {"lr:v2": 0.25, "lr:v1": 0.75})

While this is a simple operation, you may want to see :ref:`serve-split-traffic` for more information.
One thing you may want to consider as well is
:ref:`session-affinity` which gives you the ability to ensure that queries from users/clients always get mapped to the same backend.

Now that we're up and running serving two models in production, let's query
our results several times to see some results. You'll notice that we're now splitting
traffic between these two different models.

Querying our Endpoint
---------------------

We'll use the requests library to query our endpoint and be able to get a result.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__

If you run this code several times, you'll notice that the output will change - this
is due to us running the two models in parallel that we created above.

Upon concluding the above tutorial, you'll want to run ``ray stop`` to
shutdown the Ray cluster on your local machine.

Deploying as a Kubernetes Service
=================================

In order to deploy Ray Serve on Kubernetes, we need to do the following:

1. Start a Ray cluster on Kubernetes.
2. Expose the head node of the cluster as a `Service`_.
3. Start Ray Serve on the cluster.

There are multiple ways to start a Ray cluster on Kubernetes, see :ref:`ray-k8s-deploy` for more information.
Here, we will be using the :ref:`Ray Cluster Launcher <ref-automatic-cluster>` tool, which has support for Kubernetes as a backend.

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

Deployment FAQ
==============

Best practices for local development
------------------------------------

One thing you may notice is that we never have to declare a ``while True`` loop or
something to keep the Ray Serve process running. In general, we don't recommend using forever loops and therefore
opt for launching a Ray Cluster locally. Specify a Ray cluster like we did in :ref:`serve-deploy-tutorial`.
To learn more, in general, about Ray Clusters see :ref:`cluster-index`.
