============
Key Concepts
============

Ray Serve focuses on **simplicity** and only has two core concepts: backends and endpoints.

To follow along, you'll need to make the necessary imports.

.. code-block:: python

  from ray import serve
  serve.init() # Initializes Ray and Ray Serve.

.. _`serve-backend`:

Backends
========

Backends define the implementation of your business logic or models that will handle requests when queries come in to :ref:`serve-endpoint`.
To define a backend, first you must define the "handler" or the business logic you'd like to respond with.
The handler should take as input a `Flask Request object <https://flask.palletsprojects.com/en/1.1.x/api/?highlight=request#flask.Request>`_ and return any JSON-serializable object as output.
A backend is defined using :mod:`serve.create_backend <ray.serve.create_backend>`, and the implementation can be defined as either a function or a class.
Use a function when your response is stateless and a class when you might need to maintain some state (like a model).
When using a class, you can specify arguments to be passed to the constructor in :mod:`serve.create_backend <ray.serve.create_backend>`, shown below.

A backend consists of a number of *replicas*, which are individual copies of the function or class that are started in separate worker processes.

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
Note that a backend cannot be deleted while it is in use by an endpoint because then traffic to an endpoint may not be able to be handled.

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

.. _`serve-endpoint`:

Endpoints
=========

While backends define the implementation of your request handling logic, endpoints allow you to expose them via HTTP.
Endpoints are "logical" and can have one or multiple backends that serve requests to them.
To create an endpoint, we simply need to specify a name for the endpoint, the name of a backend to handle requests to the endpoint, and the route and methods where it will be accesible.
By default endpoints are serviced only by the backend provided to :mod:`serve.create_endpoint <ray.serve.create_endpoint>`, but in some cases you may want to specify multiple backends for an endpoint, e.g., for A/B testing or incremental rollout.
For information on how to do this, please see :ref:`serve-split-traffic`.

.. code-block:: python

  serve.create_endpoint("simple_endpoint", backend="simple_backend", route="/simple", methods=["GET"])

After creating the endpoint, it is now exposed by the HTTP server and handles requests using the specified backend.
We can query the model to verify that it's working.

.. code-block:: python

  import requests
  print(requests.get("http://127.0.0.1:8000/simple").text)

To view all of the existing endpoints that have created, use :mod:`serve.list_endpoints <ray.serve.list_endpoints>`.

.. code-block:: python

  >>> serve.list_endpoints()
  {'simple_endpoint': {'route': '/simple', 'methods': ['GET'], 'traffic': {}}}

You can also delete an endpoint using :mod:`serve.delete_endpoint <ray.serve.delete_endpoint>`.
Endpoints and backends are independent, so deleting an endpoint will not delete its backends.
However, an endpoint must be deleted in order to delete the backends that serve its traffic.

.. code-block:: python

  serve.delete_endpoint("simple_endpoint")
