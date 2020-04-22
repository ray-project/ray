Tensorflow Guide
================

In this guide, we will train and deploy a simple Tensorflow neural net. 
In particular, we show:

- How to load the model from file system in your RayServe definition
- How to parse the JSON request and evaluated in Tensorflow

This tutorial requires tensorflow 2. Please make sure you have tensorflow 2 installed.

.. code-block:: bash
   
    pip install -U tensorflow

Let's import RayServe and some other helpers.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_import_begin__
    :end-before: __doc_import_end__

We will train a simple MNIST model using Keras.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_train_model_begin__
    :end-before: __doc_train_model_end__

Services are just defined as normal classes with ``__init__`` and ``__call__``.
The ``__call__`` method will be invoked per request.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_define_servable_begin__
    :end-before: __doc_define_servable_end__

Let's deploy the model to RayServe. We will define an endpoint for the route representing
the digit classifier task, a backend correspond the physical implementation, and connect
them together.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_deploy_begin__
    :end-before: __doc_deploy_end__

Let's query it!

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__