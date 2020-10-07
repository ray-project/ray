.. _serve-faq:

Ray Serve FAQ
=============

This page answers some common questions about Ray Serve. If you have more
questions, feel free to ask them in the `Ray Github Discussions page <https://github.com/ray-project/ray/discussions>`_.

.. contents::

How do I deploy serve?
----------------------

See :doc:`deployment` for information about how to deploy serve.

How do I delete backends and endpoints?
---------------------------------------

To delete a backend, you can use :mod:`client.delete_backend <ray.serve.api.Client.delete_backend>`.
Note that the backend must not be use by any endpoints in order to be delete.
Once a backend is deleted, its tag can be reused.

.. code-block:: python

  client.delete_backend("simple_backend")


To delete a endpoint, you can use :mod:`client.delete_endpoint <ray.serve.api.Client.delete_endpoint>`.
Note that the endpoint will no longer work and return a 404 when queried.
Once a endpoint is deleted, its tag can be reused.

.. code-block:: python

  client.delete_endpoint("simple_endpoint")

How do I call an endpoint from Python code?
-------------------------------------------

Use :mod:`client.get_handle <ray.serve.api.Client.get_handle>` to get a handle to the endpoint,
then use :mod:`handle.remote <ray.serve.handle.RayServeHandle.remote>` to send requests to that
endpoint. This returns a Ray ObjectRef whose result can be waited for or retrieved using
``ray.wait`` or ``ray.get``, respectively.

.. code-block:: python

    handle = client.get_handle("api_endpoint")
    ray.get(handle.remote(request))


How do I call a method on my backend class besides __call__?
-------------------------------------------------------------

To call a method via HTTP use the header field ``X-SERVE-CALL-METHOD``.

To call a method via Python, use :mod:`handle.options <ray.serve.handle.RayServeHandle.options>`:

.. code-block:: python

    class StatefulProcessor:
        def __init__(self):
            self.count = 1

        def __call__(self, request):
            return {"current": self.count}

        def other_method(self, inc):
            self.count += inc
            return True

    handle = client.get_handle("backend_name")
    handle.options(method_name="other_method").remote(5)

How do I enable CORS and other HTTP features?
---------------------------------------------

Serve supports arbitrary `Starlette middlewares <https://www.starlette.io/middleware/>`_
and custom middlewares in Starlette format. The example below shows how to enable
`Cross-Origin Resource Sharing (CORS) <https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS>`_.
You can follow the same pattern for other Starlette middlewares.

.. note::

  Serve does not list ``Starlette`` as one of its dependencies. To utilize this feature,
  you will need to:

  .. code-block:: bash

    pip install starlette

.. code-block:: python

    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware

    client = serve.start(
        http_middlewares=[
            Middleware(
                CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
        ])


.. _serve-handle-explainer:

How do ``ServeHandle`` and ``ServeRequest`` work?
---------------------------------------------------

Ray Serve enables you to query models both from HTTP and Python. This feature
enables seamless :ref:`model composition<serve-model-composition>`. You can
get a ``ServeHandle`` corresponding to an ``endpoint``, similar how you can
reach an endpoint through HTTP via a specific route. When you issue a request
to an endpoint through ``ServeHandle``, the request goes through the same code
path as an HTTP request would: choosing backends through :ref:`traffic
policies <serve-split-traffic>`, finding the next available replica, and
batching requests together.

When the request arrives in the model, you can access the data similarly to how
you would with HTTP request. Here are some examples how ServeRequest mirrors Flask.Request:

.. list-table::
   :header-rows: 1

   * - HTTP
     - ServeHandle
     - | Request
       | (Flask.Request and ServeRequest)
   * - ``requests.get(..., headers={...})``
     - ``handle.options(http_headers={...})``
     - ``request.headers``
   * - ``requests.post(...)``
     - ``handle.options(http_method="POST")``
     - ``requests.method``
   * - ``request.get(..., json={...})``
     - ``handle.remote({...})``
     - ``request.json``
   * - ``request.get(..., form={...})``
     - ``handle.remote({...})``
     - ``request.form``
   * - ``request.get(..., params={"a":"b"})``
     - ``handle.remote(a="b")``
     - ``request.args``
   * - ``request.get(..., data="long string")``
     - ``handle.remote("long string")``
     - ``request.data``
   * - ``N/A``
     - ``handle.remote(python_object)``
     - ``request.data``

.. note::

    You might have noticed that the last row of the table shows that ServeRequest supports
    Python object pass through the handle. This is not possible in HTTP. If you
    need to distinguish if the origin of the request is from Python or HTTP, you can do an ``isinstance``
    check:

    .. code-block:: python

        import flask

        if isinstance(request, flask.Request):
            print("Request coming from web!")
        elif isinstance(request, ServeRequest):
            print("Request coming from Python!")

.. note::

    Once special case is when you pass a web request to a handle.

    .. code-block:: python

        handle.remote(flask_request)

    In this case, Serve will `not` wrap it in ServeRequest. You can directly
    process the request as a ``flask.Request``.

How fast is ray serve? 
----------------------
We are continuously benchmark Ray Serve. We can confidently say Ray Serve's
overhead is single digit millisecond, often times just 1-2 milliseconds. So
that's the latency. What about throughput? Serve is horizontally scalable. This
means you can just add more machines. If you double the machine, you can double
your queries per second. On a single machine with few cores, Serve can achieve
about 3-4k qps.

How does Ray Serve make projects scalable without Flask?
--------------------------------------------------------
Flask is only used as a web request object for servable to consume the data.
We actually use the fastest Python web server: uvicorn as our web server,
alongside with the power of Python asyncio. To emphasize Flask is just the request
object and data type. Not the web server.

Can I use asyncio along with Ray Serve?
---------------------------------------
Yes! You can make your servable methods ``async def`` and Serve will run them
concurrently inside a Python asyncio event loop.

Are there any other similar frameworks?
---------------------------------------
Yes and no. We truly believe Serve is unique as it gives you end to end control
over the API while delivering scalability and high performance. To achieve
something like what Serve offers, you often need to glue together multiple
frameworks like Tensorflow Serving, SageMaker, or even rolling your own
batching server.

How does Serve compare to TFServing, TorchServe, ONNXRuntime, and others?
-------------------------------------------------------------------------
Ray Serve is *framework agnostic*, you can use any Python framework and libraries.
We believe data scientists are not bounded a particular machine learning framework.
They use the best tool available for the job.

Compare to these framework specific solution, Ray Serve doesn't perform any optimization
to make your ML model run faster. However, you can still optimize the models yourself
and run them in Ray Serve: for example, you can run a model compiled by
`PyTorch JIT <https://pytorch.org/docs/stable/jit.html>`_.

How does Serve compare to AWS SageMaker, Azure ML, Google AI Platform?
----------------------------------------------------------------------
Ray Serve brings the scalability and parallelism of these hosted offering to
your own infrastructure. You can use our :ref:`cluster launcher <cluster-cloud>`
to deploy Ray Serve to all major public clouds, K8s, as well as on premise.

Compare to these offerings, Ray Serve lacks unified user interface and functionality
let you manage the lifecycle of the models, visualize it's performance, etc. Ray
Serve focuses on just model serving. You can build your own ML platforms by running
on top of Ray Serve.

How does Serve compare to Seldon, KFServing, Cortex?
----------------------------------------------------
You can develop Ray Serve on your laptop, deploy it on a dev box, and scale it out
to multiple machines or K8s cluster without changing one lines of code. It's a lot
easier to get started with when you don't need to provision and manage K8s cluster.
When it's time to deploy, you can use Ray :ref:`cluster launcher <cluster-cloud>`
to transparently put your Ray Serve application in K8s.

Compare to these frameworks letting you deploy ML models on K8s, Ray Serve lacks
the ability to declaratively configure your ML application via YAML files. In
Ray Serve, you configure everything by Python code.

How does Ray Serve scale behave on spikes?
------------------------------------------
Ray Serve has an experimental autoscaler that scales up your model replicas
based on load. We can improve it and welcome any feedback! We also rely on the
Ray cluster launcher for adding more machines.

Is Ray Serve only for ML models?
-------------------------------------------
Nope. Ray Serve can handle arbitrary Python programs.
(This means it can handle arbitrary Ray programs as well!)
