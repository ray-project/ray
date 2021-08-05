.. _serve-web-server-integration-tutorial:

Integration with Existing Web Servers
=====================================

In this guide, you will learn how to use Ray Serve to scale up your existing web application.  The key feature of Ray Serve that makes this possible is the Python-native :ref:`servehandle-api`, which allows you keep using your same Python web server while offloading your heavy computation to Ray Serve.

We give two examples, one using a `FastAPI <https://fastapi.tiangolo.com/>`__ web server and another using an `AIOHTTP <https://docs.aiohttp.org/en/stable/>`__ web server, but the same approach will work with any Python web server.


Scaling Up a FastAPI Application
--------------------------------

.. warning::

  With the introduction of the new Deployments API in Ray 1.4.0, this tutorial no longer describes the best practice for integrating Ray Serve with FastAPI, and will soon be removed.
  For details on the new and improved FastAPI integration, please see :ref:`serve-fastapi-http`.

For this example, you must have either `Pytorch <https://pytorch.org/>`_ or `Tensorflow <https://www.tensorflow.org/>`_ installed, as well as `Huggingface Transformers <https://github.com/huggingface/transformers>`_ and `FastAPI <https://fastapi.tiangolo.com/>`_.  For example:

.. code-block:: bash

  pip install "ray[serve]" tensorflow transformers fastapi

Here’s a simple FastAPI web server. It uses Huggingface Transformers to auto-generate text based on a short initial input using `OpenAI’s GPT-2 model <https://openai.com/blog/better-language-models/>`_.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/fastapi/fastapi_simple.py

To scale this up, we define a Ray Serve deployment containing our text model and call it from Python:

.. literalinclude:: ../../../../python/ray/serve/examples/doc/fastapi/servehandle_fastapi.py

To run this example, save it as ``main.py`` and then in the same directory, run the following commands to start a local Ray cluster on your machine and run the FastAPI application:

.. code-block:: bash

  ray start --head
  uvicorn main:app

Now you can query your web server, for example by running the following in another terminal:

.. code-block:: bash

  curl "http://127.0.0.1:8000/generate?query=Hello%20friend%2C%20how"

The terminal should then print the generated text:

.. code-block:: bash

  [{"generated_text":"Hello friend, how's your morning?\n\nSven: Thank you.\n\nMRS. MELISSA: I feel like it really has done to you.\n\nMRS. MELISSA: The only thing I"}]%

To clean up the Ray cluster, run ``ray stop`` in the terminal.

.. tip::
  According to the deployment configuration parameter ``num_replicas``, Ray Serve will place multiple replicas of your model across multiple CPU cores and multiple machines (provided you have :ref:`started a multi-node Ray cluster <cluster-index>`), which will correspondingly multiply your throughput.

Scaling Up an AIOHTTP Application
---------------------------------

In this section, we'll integrate Ray Serve with an `AIOHTTP <https://docs.aiohttp.org/en/stable/>`_ web server run using `Gunicorn <https://gunicorn.org/>`_.  You'll need to install AIOHTTP and gunicorn with the command ``pip install aiohttp gunicorn``.

First, here is the script that deploys Ray Serve:

.. literalinclude:: ../../../../python/ray/serve/examples/doc/aiohttp/aiohttp_deploy_serve.py

Next is the script that defines the AIOHTTP server:

.. literalinclude:: ../../../../python/ray/serve/examples/doc/aiohttp/aiohttp_app.py

Here's how to run this example:

1. Run ``ray start --head`` to start a local Ray cluster in the background.

2. In the directory where the example files are saved, run ``python aiohttp_deploy_serve.py`` to deploy our Ray Serve endpoint.

3. Run ``gunicorn aiohttp_app:app --worker-class aiohttp.GunicornWebWorker`` to start the AIOHTTP app using gunicorn.

4. To test out the server, run ``curl localhost:8000/dummy-model``.  This should output ``Model received data: dummy input``.

5. For cleanup, you can press Ctrl-C to stop the Gunicorn server, and run ``ray stop`` to stop the background Ray cluster.
