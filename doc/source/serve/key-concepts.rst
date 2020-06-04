============
Key Concepts
============

Ray Serve focuses on **simplicity** and only has two core concepts: endpoints and backends.

To follow along, you'll need to make the necessary imports.

.. code-block:: python

  from ray import serve
  serve.init() # Initializes Ray and Ray Serve.

.. _serve-endpoint:

Endpoints
=========

Endpoints allow you to name the "entity" that you'll be exposing, 
the HTTP path that your application will expose. 
Endpoints are "logical" and decoupled from the business logic or 
model that you'll be serving. To create one, we'll simply specify the name, route, and methods.

.. code-block:: python

  serve.create_endpoint("simple_endpoint", "/simple", methods=["GET"])

To view all of the existing endpoints that have created, use `serve.list_endpoints`.

.. code-block:: python

  >>> serve.list_endpoints()
  {'simple_endpoint': {'route': '/simple', 'methods': ['GET'], 'traffic': {}}}

You can also delete an endpoint using ``serve.delete_endpoint``.
Note that this will not delete any associated backends, which can be reused for other endpoints.

.. code-block:: python

  serve.delete_endpoint("simple_endpoint")

.. _serve-backend:

Backends
========

Backends are the logical structures for your business logic or models and 
how you specify what should happen when an endpoint is queried.
To define a backend, first you must define the "handler" or the business logic you'd like to respond with. 
The input to this request will be a `Flask Request object <https://flask.palletsprojects.com/en/1.1.x/api/?highlight=request#flask.Request>`_.
Use a function when your response is stateless and a class when you
might need to maintain some state (like a model). 
For both functions and classes (that take as input Flask Requests), you'll need to 
define them as backends to Ray Serve.
You can specify arguments to be passed to class constructors in ``serve.create_backend``, shown below.

It's important to note that Ray Serve places these backends in individual worker processes, which are replicas of the model.

.. code-block:: python
  
  def handle_request(flask_request):
    return "hello world"

  class RequestHandler:
    # Take the message to return as an argument to the constructor.
    def __init__(self, msg):
        self.msg = msg

    def __call__(self, flask_request):
        return self.msg

  serve.create_backend("simple_backend", handle_request)
  # Pass in the message that the backend will return as an argument.
  # If we call this backend, it will respond with "hello, world!".
  serve.create_backend("simple_backend_class", RequestHandler, "hello, world!")

We can also list all available backends and delete them to reclaim resources.

.. code-block:: python

  >> serve.list_backends()
  {
      'simple_backend': {'accepts_batches': False, 'num_replicas': 1, 'max_batch_size': None},
      'simple_backend_class': {'accepts_batches': False, 'num_replicas': 1, 'max_batch_size': None},
  }
  >> serve.delete_backend("simple_backend")
  >> serve.list_backends()
  {
      'simple_backend_class': {'accepts_batches': False, 'num_replicas': 1, 'max_batch_size': None},
  }

Setting Traffic
===============

Lastly, we need to route traffic the particular backend to the server endpoint. 
To do that we'll use the ``set_traffic`` capability.
A link is essentially a load-balancer and allow you to define queuing policies 
for how you would like backends to be served via an endpoint.
For instance, you can route 50% of traffic to Model A and 50% of traffic to Model B.

.. code-block:: python

  serve.set_traffic("simple_backend", {"simple_endpoint": 1.0})

Once we've done that, we can now query our endpoint via HTTP (we use `requests` to make HTTP calls here).

.. code-block:: python
  
  import requests
  print(requests.get("http://127.0.0.1:8000/-/routes", timeout=0.5).text)
