.. _serve-sklearn-tutorial:

Scikit-Learn Tutorial
=====================

In this guide, we will train and deploy a simple Scikit-Learn classifier.
In particular, we show:

- How to load the model from file system in your Ray Serve definition
- How to parse the JSON request and evaluated in sklearn model

Please see the :doc:`../key-concepts` to learn more general information about Ray Serve.

Ray Serve is framework agnostic. You can use any version of sklearn.

.. code-block:: bash

    pip install scikit-learn

Let's import Ray Serve and some other helpers.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_sklearn.py
    :start-after: __doc_import_begin__
    :end-before: __doc_import_end__

We will train a logistic regression with the iris dataset.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_sklearn.py
    :start-after: __doc_train_model_begin__
    :end-before: __doc_train_model_end__

Services are just defined as normal classes with ``__init__`` and ``__call__`` methods.
The ``__call__`` method will be invoked per request.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_sklearn.py
    :start-after: __doc_define_servable_begin__
    :end-before: __doc_define_servable_end__

Now that we've defined our services, let's deploy the model to Ray Serve. We will
define an :ref:`endpoint <serve-endpoint>` for the route representing the classifier task, a
:ref:`backend <serve-backend>` correspond the physical implementation, and connect them together.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_sklearn.py
    :start-after: __doc_deploy_begin__
    :end-before: __doc_deploy_end__

Let's query it!

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_sklearn.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__