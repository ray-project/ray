.. _serve-pytorch-tutorial:

PyTorch Tutorial
================

In this guide, we will load and serve a PyTorch Resnet Model.
In particular, we show:

- How to load the model from PyTorch's pre-trained modelzoo.
- How to parse the JSON request, transform the payload and evaluated in the model.

Please see the :ref:`overview <rayserve-overview>` to learn more general information about RayServe.

This tutorial requires Pytorch and Torchvision installed in your system. RayServe
is :ref:`framework agnostic <serve_frameworks>` and work with any version of PyTorch.

.. code-block:: bash

    pip install torch torchvision

Let's import RayServe and some other helpers.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_pytorch.py
    :start-after: __doc_import_begin__
    :end-before: __doc_import_end__


Services are just defined as normal classes with ``__init__`` and ``__call__`` methods.
The ``__call__`` method will be invoked per request.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_pytorch.py
    :start-after: __doc_define_servable_begin__
    :end-before: __doc_define_servable_end__

Now that we've defined our services, let's deploy the model to RayServe. We will
define an :ref:`endpoint <serve-endpoint>` for the route representing the digit classifier task, a
:ref:`backend <serve-backend>` correspond the physical implementation, and connect them together.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_pytorch.py
    :start-after: __doc_deploy_begin__
    :end-before: __doc_deploy_end__

Let's query it!

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_pytorch.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__