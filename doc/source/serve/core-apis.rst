=========
Core APIs
=========

.. contents::

Deploying a Backend
===================

Backends define the implementation of your business logic or models that will handle incoming requests.
In order to support seamless scalability backends can have many replicas, which are individual processes running in the Ray cluster to handle requests.
To define a backend, you must first define the "handler" or the business logic you'd like to respond with.
The handler should take as input a `Starlette Request object <https://www.starlette.io/requests/>`_ and return any JSON-serializable object as output.  For a more customizable response type, the handler may return a
`Starlette Response object <https://www.starlette.io/responses/>`_.

A backend is defined using :mod:`client.create_backend <ray.serve.api.Client.create_backend>`, and the implementation can be defined as either a function or a class.
Use a function when your response is stateless and a class when you might need to maintain some state (like a model).
When using a class, you can specify arguments to be passed to the constructor in :mod:`client.create_backend <ray.serve.api.Client.create_backend>`, shown below.

A backend consists of a number of *replicas*, which are individual copies of the function or class that are started in separate Ray Workers (processes).

.. code-block:: python

  def handle_request(starlette_request):
    return "hello world"

  class RequestHandler:
    # Take the message to return as an argument to the constructor.
    def __init__(self, msg):
        self.msg = msg

    def __call__(self, starlette_request):
        return self.msg

  client.create_backend("simple_backend", handle_request)
  # Pass in the message that the backend will return as an argument.
  # If we call this backend, it will respond with "hello, world!".
  client.create_backend("simple_backend_class", RequestHandler, "hello, world!")

We can also list all available backends and delete them to reclaim resources.
Note that a backend cannot be deleted while it is in use by an endpoint because then traffic to an endpoint may not be able to be handled.

.. code-block:: python

  >> client.list_backends()
  {
      'simple_backend': {'num_replicas': 1},
      'simple_backend_class': {'num_replicas': 1},
  }
  >> client.delete_backend("simple_backend")
  >> client.list_backends()
  {
      'simple_backend_class': {'num_replicas': 1},
  }

Exposing a Backend
==================

While backends define the implementation of your request handling logic, endpoints allow you to expose them via HTTP.
Endpoints are "logical" and can have one or multiple backends that serve requests to them.
To create an endpoint, we simply need to specify a name for the endpoint, the name of a backend to handle requests to the endpoint, and the route and methods where it will be accesible.
By default endpoints are serviced only by the backend provided to :mod:`client.create_endpoint <ray.serve.api.Client.create_endpoint>`, but in some cases you may want to specify multiple backends for an endpoint, e.g., for A/B testing or incremental rollout.
For information on how to do this, please see :ref:`serve-split-traffic`.

.. code-block:: python

  client.create_endpoint("simple_endpoint", backend="simple_backend", route="/simple", methods=["GET"])

After creating the endpoint, it is now exposed by the HTTP server and handles requests using the specified backend.
We can query the model to verify that it's working.

.. code-block:: python

  import requests
  print(requests.get("http://127.0.0.1:8000/simple").text)

We can also query the endpoint using the :mod:`ServeHandle <ray.serve.handle.RayServeHandle>` interface.

.. code-block:: python

  handle = client.get_handle("simple_endpoint")
  print(ray.get(handle.remote()))

To view all of the existing endpoints that have created, use :mod:`client.list_endpoints <ray.serve.api.Client.list_endpoints>`.

.. code-block:: python

  >>> client.list_endpoints()
  {'simple_endpoint': {'route': '/simple', 'methods': ['GET'], 'traffic': {}}}

You can also delete an endpoint using :mod:`client.delete_endpoint <ray.serve.api.Client.delete_endpoint>`.
Endpoints and backends are independent, so deleting an endpoint will not delete its backends.
However, an endpoint must be deleted in order to delete the backends that serve its traffic.

.. code-block:: python

  client.delete_endpoint("simple_endpoint")

.. _configuring-a-backend:

Configuring a Backend
=====================

There are a number of things you'll likely want to do with your serving application including
scaling out, splitting traffic, or configuring the maximum number of in-flight requests for a backend.
All of these options are encapsulated in a ``BackendConfig`` object for each backend.

The ``BackendConfig`` for a running backend can be updated using
:mod:`client.update_backend_config <ray.serve.api.Client.update_backend_config>`.

Scaling Out
-----------

To scale out a backend to many processes, simply configure the number of replicas.

.. code-block:: python

  config = {"num_replicas": 10}
  client.create_backend("my_scaled_endpoint_backend", handle_request, config=config)

  # scale it back down...
  config = {"num_replicas": 2}
  client.update_backend_config("my_scaled_endpoint_backend", config)

This will scale up or down the number of replicas that can accept requests.

.. _`serve-cpus-gpus`:

Resource Management (CPUs, GPUs)
--------------------------------

To assign hardware resources per replica, you can pass resource requirements to
``ray_actor_options``.
By default, each replica requires one CPU.
To learn about options to pass in, take a look at :ref:`Resources with Actor<actor-resource-guide>` guide.

For example, to create a backend where each replica uses a single GPU, you can do the
following:

.. code-block:: python

  config = {"num_gpus": 1}
  client.create_backend("my_gpu_backend", handle_request, ray_actor_options=config)

Fractional Resources
--------------------

The resources specified in ``ray_actor_options`` can also be *fractional*.
This allows you to flexibly share resources between replicas.
For example, if you have two models and each doesn't fully saturate a GPU, you might want to have them share a GPU by allocating 0.5 GPUs each.
The same could be done to multiplex over CPUs.

.. code-block:: python

  half_gpu_config = {"num_gpus": 0.5}
  client.create_backend("my_gpu_backend_1", handle_request, ray_actor_options=half_gpu_config)
  client.create_backend("my_gpu_backend_2", handle_request, ray_actor_options=half_gpu_config)

Configuring Parallelism with OMP_NUM_THREADS
--------------------------------------------

Deep learning models like PyTorch and Tensorflow often use multithreading when performing inference.
The number of CPUs they use is controlled by the OMP_NUM_THREADS environment variable.
To :ref:`avoid contention<omp-num-thread-note>`, Ray sets ``OMP_NUM_THREADS=1`` by default because Ray workers and actors use a single CPU by default.
If you *do* want to enable this parallelism in your Serve backend, just set OMP_NUM_THREADS to the desired value either when starting Ray or in your function/class definition:

.. code-block:: bash

  OMP_NUM_THREADS=12 ray start --head
  OMP_NUM_THREADS=12 ray start --address=$HEAD_NODE_ADDRESS

.. code-block:: python

  class MyBackend:
      def __init__(self, parallelism):
          os.environ["OMP_NUM_THREADS"] = parallelism
          # Download model weights, initialize model, etc.

  client.create_backend("parallel_backend", MyBackend, 12)


.. note::
  Some other libraries may not respect ``OMP_NUM_THREADS`` and have their own way to configure parallelism.
  For example, if you're using OpenCV, you'll need to manually set the number of threads using ``cv2.setNumThreads(num_threads)`` (set to 0 to disable multi-threading).
  You can check the configuration using ``cv2.getNumThreads()`` and ``cv2.getNumberOfCPUs()``.

User Configuration (Experimental)
---------------------------------

Suppose you want to update a parameter in your model without creating a whole
new backend.  You can do this by writing a `reconfigure` method for the class
underlying your backend.  At runtime, you can then pass in your new parameters
by setting the `user_config` field of :mod:`BackendConfig <ray.serve.BackendConfig>`.

The following simple example will make the usage clear:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_reconfigure.py

The `reconfigure` method is called when the class is created if `user_config`
is set.  In particular, it's also called when new replicas are created in the
future if scale up your backend later.  The `reconfigure` method is also  called
each time `user_config` is updated via
:mod:`client.update_backend_config <ray.serve.api.Client.update_backend_config>`.

Dependency Management
=====================

Ray Serve supports serving backends with different (possibly conflicting)
python dependencies.  For example, you can simultaneously serve one backend
that uses legacy Tensorflow 1 and another backend that uses Tensorflow 2.

Currently this is supported using `conda <https://docs.conda.io/en/latest/>`_
via Ray's built-in ``runtime_env`` option for actors.
As with all other actor options, pass these in via ``ray_actor_options`` in
your call to
:mod:`client.create_backend <ray.serve.api.Client.create_backend>`.
You must have a conda environment set up for each set of
dependencies you want to isolate.  If using a multi-node cluster, the
desired conda environment must be present on all nodes.  
See :ref:`conda-environments-for-tasks-and-actors` for details.

Here's an example script.  For it to work, first create a conda
environment named ``ray-tf1`` with Ray Serve and Tensorflow 1 installed,
and another named ``ray-tf2`` with Ray Serve and Tensorflow 2.  The Ray and
Python versions must be the same in both environments.

.. literalinclude:: ../../../python/ray/serve/examples/doc/conda_env.py

.. note::
  If a conda environment is not specified, your backend will be started in the
  same conda environment as the client (the process calling
  :mod:`client.create_backend <ray.serve.api.Client.create_backend>`) by
  default.  (When using :ref:`ray-client`, your backend will be started in the
  conda environment that the Serve controller is running in, which by default is the
  conda environment the remote Ray cluster was started in.)

The dependencies required in the backend may be different than
the dependencies installed in the driver program (the one running Serve API
calls). In this case, you can pass the backend in as an import path that will
be imported in the Python environment in the workers, but not the driver.
Example:

.. literalinclude:: ../../../python/ray/serve/examples/doc/imported_backend.py
