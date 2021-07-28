===================
End-to-End Tutorial
===================

By the end of this tutorial, you will have learned the basics of Ray Serve and will be ready to pick and choose from the advanced topics in the sidebar.

First, install Ray Serve and all of its dependencies by running the following command in your terminal:

.. code-block:: bash

  pip install "ray[serve]"

Now we will write a Python script to serve a simple "Counter" class over HTTP.  You may open an interactive Python terminal and copy in the lines below as we go.

First, import Ray and Ray Serve:

.. code-block:: python

  import ray
  from ray import serve

Ray Serve runs on top of a Ray cluster, so the next step is to start a local Ray cluster:

.. code-block:: python

  ray.init()

.. note::

  ``ray.init()`` will start a single-node Ray cluster on your local machine, which will allow you to use all your CPU cores to serve requests in parallel.  To start a multi-node cluster, see :doc:`../cluster/index`.

Next, start the Ray Serve runtime:

.. code-block:: python

  serve.start()

.. warning::

  When the Python script exits, Ray Serve will shut down.  
  If you would rather keep Ray Serve running in the background you can use ``serve.start(detached=True)`` (see :doc:`deployment` for details).

Now we will define a simple Counter class. The goal is to serve this class behind an HTTP endpoint using Ray Serve.  

By default, Ray Serve offers a simple HTTP proxy that will send requests to the class' ``__call__`` method. The argument to this method will be a Starlette ``Request`` object.

.. code-block:: python

  @serve.deployment
  class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, request):
        self.count += 1
        return {"count": self.count}

.. note::

  Besides classes, you can also serve standalone functions with Ray Serve in the same way.

Notice that we made this class into a ``Deployment`` with the :mod:`@serve.deployment <ray.serve.api.deployment>` decorator.
This decorator is where we could set various configuration options such as the number of replicas, unique name of the deployment (it defaults to the class name), or the HTTP route prefix to expose the deployment at.
See the :mod:`Deployment package reference <ray.serve.api.Deployment>` for more details.
In order to deploy this, we simply need to call ``Counter.deploy()``.

.. code-block:: python

  Counter.deploy()

.. note::

  Deployments can be configured to improve performance, for example by increasing the number of replicas of the class being served in parallel.  For details, see :ref:`configuring-a-deployment`.

Now that our deployment is up and running, let's test it out by making a query over HTTP.  
In your browser, simply visit ``http://127.0.0.1:8000/Counter``, and you should see the output ``{"count": 1"}``.
If you keep refreshing the page, the count should increase, as expected.

Now let's say we want to update this deployment to add another method to decrement the counter.
Here, because we want more flexible HTTP configuration we'll use Serve's FastAPI integration.
For more information on this, please see :ref:`serve-fastapi-http`.

.. code-block:: python

  from fastapi import FastAPI

  app = FastAPI()

  @serve.deployment
  @serve.ingress(app)
  class Counter:
    def __init__(self):
        self.count = 0

    @app.get("/")
    def get(self):
        return {"count": self.count}

    @app.get("/incr")
    def incr(self):
        self.count += 1
        return {"count": self.count}

    @app.get("/decr")
    def decr(self):
        self.count -= 1
        return {"count": self.count}

We've now redefined the ``Counter`` class to wrap a ``FastAPI`` application.
This class is exposing three HTTP routes: ``/Counter`` will get the current count, ``/Counter/incr`` will increment the count, and ``/Counter/decr`` will decrement the count.

To redeploy this updated version of the ``Counter``, all we need to do is run ``Counter.deploy()`` again.
Serve will perform a rolling update here to replace the existing replicas with the new version we defined.

.. code-block:: python

  Counter.deploy()

If we test out the HTTP endpoint again, we can see this in action.
Note that the count has been reset to zero because the new version of ``Counter`` was deployed.

.. code-block:: bash

  > curl -X GET localhost:8000/Counter/
  {"count": 0}
  > curl -X GET localhost:8000/Counter/incr
  {"count": 1}
  > curl -X GET localhost:8000/Counter/decr
  {"count": 0}

Congratulations, you just built and ran your first Ray Serve application! You should now have enough context to dive into the :doc:`core-apis` to get a deeper understanding of Ray Serve.
For more interesting example applications, including integrations with popular machine learning frameworks and Python web servers, be sure to check out :doc:`tutorials/index`.
