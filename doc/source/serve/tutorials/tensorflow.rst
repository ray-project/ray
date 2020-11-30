.. _serve-tensorflow-tutorial:

Keras and Tensorflow Tutorial
=============================

In this guide, we will train and deploy a simple Tensorflow neural net.
In particular, we show:

- How to load the model from file system in your Ray Serve definition
- How to parse the JSON request and evaluated in Tensorflow

Please see the :doc:`../key-concepts` to learn more general information about Ray Serve.

Ray Serve is framework agnostic -- you can use any version of Tensorflow.
However, for this tutorial, we use Tensorflow 2 and Keras. Please make sure you have
Tensorflow 2 installed.


.. code-block:: bash

    pip install "tensorflow>=2.0"

Let's import Ray Serve and some other helpers.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_import_begin__
    :end-before: __doc_import_end__

We will train a simple MNIST model using Keras.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_train_model_begin__
    :end-before: __doc_train_model_end__

Services are just defined as normal classes with ``__init__`` and ``__call__`` methods.
The ``__call__`` method will be invoked per request.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_define_servable_begin__
    :end-before: __doc_define_servable_end__

Now that we've defined our services, let's deploy the model to Ray Serve. We will
define an :ref:`endpoint <serve-endpoint>` for the route representing the digit classifier task, a
:ref:`backend <serve-backend>` correspond the physical implementation, and connect them together.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_deploy_begin__
    :end-before: __doc_deploy_end__

Let's query it!

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_tensorflow.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__