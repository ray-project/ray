===============================
Serving Machine Learning Models
===============================

.. contents::

.. _serve-batching:

Request Batching
================

You can also have Ray Serve batch requests for performance, which is especially important for some ML models that run on GPUs. In order to use this feature, you need to do the following two things:

1. Use ``async def`` for your request handling logic to process queries concurrently.
2. Use the ``@serve.batch`` decorator to batch individual queries that come into the replica. The method/function that's decorated should handle a list of requests and return a list of the same length.


.. code-block:: python

  class BatchingExample:
      def __init__(self):
          self.count = 0

      @serve.batch
      async def handle_batch(self, requests):
          responses = []
          for request in requests:
              responses.append(request.json())

          return responses

      async def __call__(self, request):
          return await self.handle_batch(request)

  client.create_backend("counter1", BatchingExample)
  client.create_endpoint("counter1", backend="counter1", route="/increment")

Please take a look at :ref:`Batching Tutorial<serve-batch-tutorial>` for a deep
dive.


.. _serve-model-composition:

Model Composition
=================

Ray Serve supports composing individually scalable models into a single model
out of the box. For instance, you can combine multiple models to perform
stacking or ensembles.

To define a higher-level composed model you need to do three things:

1. Define your underlying models (the ones that you will compose together) as
   Ray Serve backends
2. Define your composed model, using the handles of the underlying models
   (see the example below).
3. Define an endpoint representing this composed model and query it!

In order to avoid synchronous execution in the composed model (e.g., it's very
slow to make calls to the composed model), you'll need to make the function
asynchronous by using an ``async def``. You'll see this in the example below.

That's it. Let's take a look at an example:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_model_composition.py


Framework-Specific Tutorials
============================

Ray Serve seamlessly integrates with popular Python ML libraries.
Below are tutorials with some of these frameworks to help get you started.

- :ref:`PyTorch Tutorial<serve-pytorch-tutorial>`
- :ref:`Scikit-Learn Tutorial<serve-sklearn-tutorial>`
- :ref:`Keras and Tensorflow Tutorial<serve-tensorflow-tutorial>`
- :ref:`RLlib Tutorial<serve-rllib-tutorial>`
