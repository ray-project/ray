Ray Serve: Scalable and Programmable Serving
============================================

.. image:: logo.svg
    :align: center

Serve is a serving library built on top of ray. It is easy-to-use and flexible.

- Serve allows you to write *stateless* logic in serve functions and *stateful* logic in serve actors.
- Serve has *Flask-inspired API*. You directly work with a Flask request object on each request.
- Serve grows as your project grows, it can *scale to a cluster* without changing any lines of code.
- *Everything is in Python*. Instead of writing configuration YAML, the language to configure the serving system is the same as the language to write the application logic.
- *Batching* and *SLO aware scheduling* are built-in features. Serve can automatically batch queries and reorder them to meet SLO target.
- Deploy to any cloud (or Kubernetes) with Ray Autoscaler.


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