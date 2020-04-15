RayServe: Scalable and Programmable Serving
============================================

.. image:: logo.svg
    :align: center

.. note::
	If you want to try out Serve, join our `community slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`_
	and discuss in ``#serve`` channel.

There are generally two ways of serving machine learning applications at scale.
The first is wrapping your application in a traditional web server. This approach
is easy but hard to scale each component, and easily leading to high memory usage
as well as concurrency issue. The other approach is to use a cloud-hosted solution
like SageMaker or TFServing. These solutions have high learning costs and lead to
vendor lock-in.

Serve is a serving library built on top of ray. It is easy-to-use and flexible.

- Serve is **framework agnostics** and extensible. You can serve your scikit-learn,
  PyTorch, and TensorFlow models in the same framework.
- Serve gives you end-to-end control over your API. Your input is just a **Flask
  request** instead of arrays.
- Serve scales to many machines. With a single API call, you can run scale your
  models to hundreds of GPUs.
- Serve decouples routing and handling so you can update or **A/B test** your models
  with zero downtime.
- Serve uses **Python as the configuration language**. Tired of writing repetitive YAMLs
  or JSON to configure your services? Serve can be configured directly using the
  Python API.
- Serve has built-in **batching and SLO awareness**. This means Serve will maximally
  utilize the hardware and reorder queries to meet your latency objective.
- With Ray Autoscaler, you can deploy Serve to **any cloud** (or Kubernetes).


Quick start
-----------
Serve a stateless function:

.. literalinclude:: ../../../python/ray/serve/examples/doc/quickstart_function.py

Serve a stateful class:

.. literalinclude:: ../../../python/ray/serve/examples/doc/quickstart_class.py


``@serve.route`` decorator is similar to the Flask ``route`` decorator. You can
decorate a function or a class. It specifies how request for HTTP is routed to
your function.

To make your function servable, the function just need to take in a flask
request as first argument. Your input for web request are just flask request
object, you don't need to learn new API. To make your class servable, implement
``__call__`` method taking in the flask request as well.

Learn more
----------
- Serve architecture in depth
- Serve how-to guides

	- Scikit-learn serving with composition
	- PyTorch serving with batching

- Serve deployment guides