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

  serve.create_backend("counter1", BatchingExample)
  serve.create_endpoint("counter1", backend="counter1", route="/increment")

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

Integration with Model Registries
=================================

Ray Serve is flexible.  If you can load your model as a Python
function or class, then you can scale it up and serve it with Ray Serve.

For example, if you are using the 
`MLflow Model Registry <https://www.mlflow.org/docs/latest/model-registry.html>`_
to manage your models, the following wrapper
class will allow you to load a model using its MLflow `Model URI`: 

.. code-block:: python

  import pandas as pd
  import mlflow.pyfunc

  class MLflowBackend:
      def __init__(self, model_uri):
          self.model = mlflow.pyfunc.load_model(model_uri=model_uri)

      async def __call__(self, request):
          csv_text = await request.body() # The body contains just raw csv text.
          df = pd.read_csv(csv_text)
          return self.model.predict(df)

A Serve backend can then be created to serve the model as follows:

.. code-block:: python

  client.create_backend("my_backend", MLflowBackend, my_model_uri)

.. tip:: 

  The above approach will work for any model registry, not just MLflow.
  Namely, load the model from the registry in ``__init__``, and forward the request to the model in ``__call__``.

For an even more hands-off and seamless integration with MLflow, check out the 
`Ray Serve MLflow deployment plugin <https://github.com/ray-project/mlflow-ray-serve>`__.  A full
tutorial is available `here <https://github.com/mlflow/mlflow/tree/master/examples/ray_serve>`__.

Framework-Specific Tutorials
============================

Ray Serve seamlessly integrates with popular Python ML libraries.
Below are tutorials with some of these frameworks to help get you started.

- :ref:`PyTorch Tutorial<serve-pytorch-tutorial>`
- :ref:`Scikit-Learn Tutorial<serve-sklearn-tutorial>`
- :ref:`Keras and Tensorflow Tutorial<serve-tensorflow-tutorial>`
- :ref:`RLlib Tutorial<serve-rllib-tutorial>`
