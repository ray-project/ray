.. _serve-rllib-tutorial:

RLlib Tutorial
=============================

In this guide, we will train and deploy a simple Ray RLlib PPO model.
In particular, we show:

- How to load the model from checkpoint
- How to parse the JSON request and evaluate payload in RLlib

Please see the :doc:`../core-apis` to learn more general information about Ray Serve.


Let's import Ray Serve and some other helpers.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_rllib.py
    :start-after: __doc_import_begin__
    :end-before: __doc_import_end__

We will train and checkpoint a simple PPO model with ``CartPole-v0`` environment.
We are just writing to local disk for now. In production, you might want to consider
loading the weights from a cloud storage (S3) or shared file system.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_rllib.py
    :start-after: __doc_train_model_begin__
    :end-before: __doc_train_model_end__

Services are just defined as normal classes with ``__init__`` and ``__call__`` methods.
The ``__call__`` method will be invoked per request. For each request, the method
retrieves the ``request.json()["observation"]`` as input.

.. tip::
    
    Although we used a single input and ``trainer.compute_action(...)`` here, you
    can process a batch of input using Ray Serve's :ref:`batching<serve-batching>` feature
    and use ``trainer.compute_actions(...)`` (notice the plural!) to process a 
    batch.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_rllib.py
    :start-after: __doc_define_servable_begin__
    :end-before: __doc_define_servable_end__

Now that we've defined our services, let's deploy the model to Ray Serve. We will
define a Serve deployment that will be exposed over an HTTP route.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_rllib.py
    :start-after: __doc_deploy_begin__
    :end-before: __doc_deploy_end__

Let's query it!

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_rllib.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__
