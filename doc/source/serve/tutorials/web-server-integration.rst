.. _serve-web-server-integration-tutorial:

Web Server Integration Tutorial
===============================

In this guide, you will learn how to use Ray Serve to scale up your existing web application.  The key feature of Ray Serve that makes this possible is the Python-native ServeHandle API, which allows you to offload your heavy computation to Ray Serve.

We give two examples, one using a FastAPI web server and another using a simple AIOHTTP web server.

Scaling Up a FastAPI Application
--------------------------------

For this example, you must have either Pytorch or Tensorflow installed, as well as Huggingface Transformers and FastAPI.

Here’s a simple FastAPI web server. It uses Huggingface Transformers to auto-generate text based on a short initial input using OpenAI’s GPT-2 model, and serves this to the user.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/fastapi.py

To scale this up, we define a Ray Serve backend containing our text model and call it from Python using a ServeHandle:

.. literalinclude:: ../../../../python/ray/serve/examples/doc/servehandle_fastapi.py

To run this example, save it as ``main.py`` and then run the following in your terminal to start a local Ray cluster and run the FastAPI application:

.. code-block:: console

  ray start --head
  uvicorn main:app

Now you can query your web server, for example by running the following in another terminal:

.. code-block:: console

  curl "http://127.0.0.1:8000/generate?query=Hello%20friend%2C%20how"
  
The terminal should then output the generated text:

.. code-block:: console

  [{"generated_text":"Hello friend, how's your morning?\n\nSven: Thank you.\n\nMRS. MELISSA: I feel like it really has done to you.\n\nMRS. MELISSA: The only thing I"}]%

According to the configuration parameter ``num_replicas``, Ray Serve will place multiple replicas of your model across multiple CPU cores and multiple machines (provided you have started a multi-node Ray cluster), which will increase your throughput.

Scaling Up an AIOHTTP Application
---------------------------------

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