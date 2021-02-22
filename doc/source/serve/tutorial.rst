==============
Basic Tutorial
==============

By the end of this tutorial, you will have learned how to use Ray Serve and will be ready to pick and choose from the advanced topics in the sidebar.

First, install Ray Serve and all of its dependencies by running the following command in your terminal:

.. code-block:: bash

  pip install "ray[serve]"

Now you are ready to use Ray Serve.  

Now we will write a Python script to serve a simple "Counter" class.  You may open an interactive Python terminal and copy in the lines below as we go.

First, import Ray and Ray Serve:

.. code-block:: python

  import ray
  from ray import serve

Ray Serve runs on top of a Ray cluster, so the next step is to start a local Ray cluster:

.. code-block:: python

  ray.init()

.. note

  This will start a single-node Ray cluster on your local machine, which will allow you to use all your CPU cores to serve requests in parallel.  To start a multi-node cluster, see TODO(deployments)

Next, start the Ray Serve runtime:

.. code-block:: python

  client = serve.start()

Here, `client` is a Ray Serve Client object.  (TODO: link).  You will access most of the Ray Serve API via methods of this object.

.. warning

  When `client` goes out of scope, for example when you exit the interactive terminal or when you exit a Python script, Ray Serve will shut down.  
  If you would rather keep Ray Serve running in the background, see TODO.

Now we will define a simple Counter class. The goal is to serve this class behind an HTTP endpoint using Ray Serve.  

For our Counter class to work with Ray Serve, it needs to be a *callable* class, so we will need to implement a `__call__` method as shown:

.. code-block:: python

  class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, request):
        self.count += 1
        return {"count": self.count}

.. note
  
  In addition to callable classes, you can also serve functions using Ray Serve.

Now we are ready to deploy our class using Ray Serve.  First, create a Ray Serve backend (TODO: link) and pass in the Counter class:

.. code-block:: python

  client.create_backend("my_backend", Counter)

Here we have assigned a tag `"my_backend"` to this backend, which we can use to identify this backend in the future.   

.. note

  Ray Serve Backends can be configured to improve performance, for example by increasing the number of replicas of the class being served in parallel.  For details, see TODO

To complete the deployment, we will expose this backend over HTTP by creating a Ray Serve endpoint:

.. code-block:: python

  client.create_endpoint("my_endpoint", backend="my_backend", route="/counter")

Here `"my_endpoint"` is a tag used to identify this endpoint, and we have specified the backend to place behind the endpoint via the `backend` parameter.  
The last parameter, `route`, is the path at which our endpoint will be available over HTTP.  

Now that our deployment is up and running, let's test it out by making a query over HTTP.  
In your browser, simply visit http://127.0.0.1:8000/counter, and you should see the output {"count": 1"}.  
If you keep refreshing the page, the count should increase, as expected.
