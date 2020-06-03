============
Key Concepts
============

Ray Serve focuses on **simplicity** and only has two core concepts: backends and endpoints.

To follow along, you'll need to make the necessary imports.

.. code-block:: python

  from ray import serve
  serve.init() # Initializes Ray and Ray Serve.

.. _serve-endpoint:

.. _serve-backend:

Backends
========

Backends define the implementation of your business logic or models that will handle requests when queries come in to :ref:`serve-endpoints`.
To define a backend, first you must define the "handler" or the business logic you'd like to respond with. 
The handler should take as input a `Flask Request object <https://flask.palletsprojects.com/en/1.1.x/api/?highlight=request#flask.Request>`_ and return any JSON-serializable object as output.
A backend is defined using ``serve.create_backend``, and the implementation can be defined as either a function or a class.
Use a function when your response is stateless and a class when you might need to maintain some state (like a model). 

A backend consists of a number of *replicas*, which are individual copies of the function or class that are started in separate worker processes.

.. code-block:: python
  
  def handle_request(flask_request):
    return "hello world"

  class RequestHandler:
    def __init__(self):
        self.msg = "hello, world!"

    def __call__(self, flask_request):
        return self.msg

  serve.create_backend("simple_backend", handle_request)
  serve.create_backend("simple_backend_class", RequestHandler)

.. _`serve-endpoints`:

Endpoints
=========

While backends define the implementation of your request handling logic, endpoints allow you to expose them via HTTP.
Endpoints are "logical" and can have one or multiple backends that serve requests to them
To create an endpoint, we simply need to specify a name for the endpoint, the name of a backend to handle requests to the endpoint, and the route and methods where it will be accesible.

.. code-block:: python

  serve.create_endpoint("simple_endpoint", "simple_backend", route="/simple", methods=["GET"])

After creating the endpoint, it is now exposed by the HTTP server and handles requests using the specified backend.
We can query the model to verify that it's working.

.. code-block:: python
  
  import requests
  print(requests.get("http://127.0.0.1:8000/simple").text)

An endpoint can also be serviced by multiple backends, e.g., for A/B testing or incremental rollout.
For more information, please see :ref:`serve-split-traffic`.
