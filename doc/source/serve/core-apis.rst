=====================
Core API: Deployments
=====================

.. contents::

Creating a Deployment
=====================

Deployments are the central concept in Ray Serve.
They allow you to define and update your business logic or models that will handle incoming requests as well as how this is exposed over HTTP or in Python.

A deployment is defined using :mod:`@serve.deployment <ray.serve.api.deployment>` on a Python class (or function for simple use cases).
You can specify arguments to be passed to the constructor when you call ``Deployment.deploy()``, shown below.

A deployment consists of a number of *replicas*, which are individual copies of the function or class that are started in separate Ray Actors (processes).

.. code-block:: python

  @serve.deployment
  class MyFirstDeployment:
    # Take the message to return as an argument to the constructor.
    def __init__(self, msg):
        self.msg = msg

    def __call__(self, request):
        return self.msg

    def other_method(self, arg):
        return self.msg

  MyFirstDeployment.deploy("Hello world!")

Deployments can be exposed in two ways: over HTTP or in Python via the :ref:`servehandle-api`.
By default, HTTP requests will be forwarded to the ``__call__`` method of the class (or the function) and a ``Starlette Request`` object will be the sole argument.
You can also define a deployment that wraps a FastAPI app for more flexible handling of HTTP requests. See :ref:`serve-fastapi-http` for details.

We can also list all available deployments and dynamically get a reference to them:

.. code-block:: python

  >> serve.list_deployments()
  {'A': Deployment(name=A,version=None,route_prefix=/A)}
  {'MyFirstDeployment': Deployment(name=MyFirstDeployment,version=None,route_prefix=/MyFirstDeployment}

  # Returns the same object as the original MyFirstDeployment object.
  # This can be used to redeploy, get a handle, etc.
  deployment = serve.get_deployment("MyFirstDeployment")

Exposing a Deployment
=====================

By default, deployments are exposed over HTTP at ``http://localhost:8000/<deployment_name>``.
The HTTP path that the deployment is available at can be changed using the ``route_prefix`` option.
All requests to ``/{route_prefix}`` and any subpaths will be routed to the deployment (using a longest-prefix match for overlapping route prefixes).

Here's an example:

.. code-block:: python

  @serve.deployment(name="http_deployment", route_prefix="/api")
  class HTTPDeployment:
    def __call__(self, request):
        return "Hello world!"

After creating the endpoint, it is now exposed by the HTTP server and handles requests using the specified class.
We can query the model to verify that it's working.

.. code-block:: python

  import requests
  print(requests.get("http://127.0.0.1:8000/api").text)

We can also query the endpoint using the :mod:`ServeHandle <ray.serve.handle.RayServeHandle>` interface.

.. code-block:: python

  # To get a handle from the same script, use the Deployment object directly:
  handle = HTTPDeployment.get_handle()

  # To get a handle from a different script, reference it by name:
  handle = serve.get_deployment("http_deployment").get_handle()

  print(ray.get(handle.remote()))

Updating a Deployment
=====================

Often you want to be able to update your code or configuration options for a deployment over time.
Deployments can be updated simply by updating the code or configuration options and calling ``deploy()`` again.

.. code-block:: python

  @serve.deployment(name="my_deployment", num_replicas=1)
  class SimpleDeployment:
      pass

  # Creates one initial replica.
  SimpleDeployment.deploy()

  # Re-deploys, creating an additional replica.
  # This could be the SAME Python script, modified and re-run.
  @serve.deployment(name="my_deployment", num_replicas=2)
  class SimpleDeployment:
      pass

  SimpleDeployment.deploy()

  # You can also use Deployment.options() to change options without redefining
  # the class. This is useful for programmatically updating deployments.
  SimpleDeployment.options(num_replicas=2).deploy()


By default, each call to ``.deploy()`` will cause a redeployment, even if the underlying code and options didn't change.
This could be detrimental if you have many deployments in a script and and only want to update one: if you re-run the script, all of the deployments will be redeployed, not just the one you updated.
To prevent this, you may provide a ``version`` string for the deployment as a keyword argument in the decorator or ``Deployment.options()``.
If provided, the replicas will only be updated if the value of ``version`` is updated; if the value of ``version`` is unchanged, the call to ``.deploy()`` will be a no-op."
When a redeployment happens, Serve will perform a rolling update, bringing down at most 20% of the replicas at any given time.

.. _configuring-a-deployment:

Configuring a Deployment
========================

There are a number of things you'll likely want to do with your serving application including
scaling out or configuring the maximum number of in-flight requests for a deployment.
All of these options can be specified either in :mod:`@serve.deployment <ray.serve.api.deployment>` or in ``Deployment.options()``.

To update the config options for a running deployment, simply redeploy it with the new options set.

Scaling Out
-----------

To scale out a deployment to many processes, simply configure the number of replicas.

.. code-block:: python

  # Create with a single replica.
  @serve.deployment(num_replicas=1)
  def func(*args):
      pass

  func.deploy()

  # Scale up to 10 replicas.
  func.options(num_replicas=10).deploy()

  # Scale back down to 1 replica.
  func.options(num_replicas=1).deploy()

.. _`serve-cpus-gpus`:

Resource Management (CPUs, GPUs)
--------------------------------

To assign hardware resources per replica, you can pass resource requirements to
``ray_actor_options``.
By default, each replica requires one CPU.
To learn about options to pass in, take a look at :ref:`Resources with Actor<actor-resource-guide>` guide.

For example, to create a deployment where each replica uses a single GPU, you can do the
following:

.. code-block:: python

  @serve.deployment(ray_actor_options={"num_gpus": 1})
  def func(*args):
      return do_something_with_my_gpu()

Fractional Resources
--------------------

The resources specified in ``ray_actor_options`` can also be *fractional*.
This allows you to flexibly share resources between replicas.
For example, if you have two models and each doesn't fully saturate a GPU, you might want to have them share a GPU by allocating 0.5 GPUs each.
The same could be done to multiplex over CPUs.

.. code-block:: python

  @serve.deployment(name="deployment1", ray_actor_options={"num_gpus": 0.5})
  def func(*args):
      return do_something_with_my_gpu()

  @serve.deployment(name="deployment2", ray_actor_options={"num_gpus": 0.5})
  def func(*args):
      return do_something_with_my_gpu()

Configuring Parallelism with OMP_NUM_THREADS
--------------------------------------------

Deep learning models like PyTorch and Tensorflow often use multithreading when performing inference.
The number of CPUs they use is controlled by the OMP_NUM_THREADS environment variable.
To :ref:`avoid contention<omp-num-thread-note>`, Ray sets ``OMP_NUM_THREADS=1`` by default because Ray workers and actors use a single CPU by default.
If you *do* want to enable this parallelism in your Serve deployment, just set OMP_NUM_THREADS to the desired value either when starting Ray or in your function/class definition:

.. code-block:: bash

  OMP_NUM_THREADS=12 ray start --head
  OMP_NUM_THREADS=12 ray start --address=$HEAD_NODE_ADDRESS

.. code-block:: python

  @serve.deployment
  class MyDeployment:
      def __init__(self, parallelism):
          os.environ["OMP_NUM_THREADS"] = parallelism
          # Download model weights, initialize model, etc.

  MyDeployment.deploy()


.. note::
  Some other libraries may not respect ``OMP_NUM_THREADS`` and have their own way to configure parallelism.
  For example, if you're using OpenCV, you'll need to manually set the number of threads using ``cv2.setNumThreads(num_threads)`` (set to 0 to disable multi-threading).
  You can check the configuration using ``cv2.getNumThreads()`` and ``cv2.getNumberOfCPUs()``.

User Configuration (Experimental)
---------------------------------

Suppose you want to update a parameter in your model without needing to restart
the replicas in your deployment.  You can do this by writing a `reconfigure` method
for the class underlying your deployment.  At runtime, you can then pass in your
new parameters by setting the `user_config` option.

The following simple example will make the usage clear:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_reconfigure.py

The `reconfigure` method is called when the class is created if `user_config`
is set.  In particular, it's also called when new replicas are created in the
future if scale up your deployment later.  The `reconfigure` method is also  called
each time `user_config` is updated.

Dependency Management
=====================

Ray Serve supports serving deployments with different (possibly conflicting)
python dependencies.  For example, you can simultaneously serve one deployment
that uses legacy Tensorflow 1 and another that uses Tensorflow 2.

Currently this is supported on Mac OS and Linux using `conda <https://docs.conda.io/en/latest/>`_
via Ray's built-in ``runtime_env`` option for actors.
As with all other actor options, pass these in via ``ray_actor_options`` in
your deployment.
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
  If a conda environment is not specified, your deployment will be started in the
  same conda environment as the client (the process creating the deployment) by
  default.  (When using :ref:`ray-client`, your deployment will be started in the
  conda environment that the Serve controller is running in, which by default is the
  conda environment the remote Ray cluster was started in.)

The dependencies required in the deployment may be different than
the dependencies installed in the driver program (the one running Serve API
calls). In this case, you should use a delayed import within the class to avoid
importing unavailable packages in the driver.
Example:

.. literalinclude:: ../../../python/ray/serve/examples/doc/imported_backend.py
