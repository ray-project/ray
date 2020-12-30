.. _serve-web-server-integration-tutorial:

Web Server Integration Tutorial
===============================

In this guide, you will learn how to use Ray Serve to scale up your existing web application.  The key feature of Ray Serve that makes this possible is the Python-native ServeHandle API, which allows you keep using your same Python web server while offloading your heavy computation to Ray Serve in a few lines of code.

We give two examples, one using a FastAPI web server and another using an AIOHTTP web server.

TODO(architkulkarni): Add all hyperlinks

Scaling Up a FastAPI Application
--------------------------------

For this example, you must have either Pytorch or Tensorflow installed, as well as Huggingface Transformers and FastAPI.

Here’s a simple FastAPI web server. It uses Huggingface Transformers to auto-generate text based on a short initial input using OpenAI’s GPT-2 model, and serves this to the user.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/fastapi.py

To scale this up, we define a Ray Serve backend containing our text model and call it from Python using a ServeHandle:

.. literalinclude:: ../../../../python/ray/serve/examples/doc/servehandle_fastapi.py

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

To clean up the Ray cluster, you may use the command ``ray stop`` in the terminal.

According to the configuration parameter ``num_replicas``, Ray Serve will place multiple replicas of your model across multiple CPU cores and multiple machines (provided you have started a multi-node Ray cluster), which will increase your throughput.

Scaling Up an AIOHTTP Application
---------------------------------

In this section, we'll integrate Ray Serve with an AIOHTTP server.

First, here is the script that defines the AIOHTTP server:

.. literalinclude:: ../../../../python/ray/serve/examples/doc/aiohttp_app.py

Finally, here is the script that deploys Ray Serve:

.. literalinclude:: ../../../../python/ray/serve/examples/doc/aiohttp_deploy.py


Here are the steps to run this example:

1. Run ``ray start --head`` to start a local Ray cluster in the background.

2. In the directory where the example files are saved, run ``python deploy_serve.py`` to deploy our Ray Serve endpoint.  

.. note::
  Because we have omitted the keyword argument ``route`` in ``client.create_endpoint()``, our endpoint will not be exposed over HTTP by Ray Serve.

3. Run ``gunicorn aiohttp_app:app --worker-class aiohttp.GunicornWebWorker --bind localhost:8001`` to start the AIOHTTP app using gunicorn. We bind to port 8001 because the Ray Dashboard is already using port 8000 by default.

.. tip::
  You can change the Ray Dashboard port with the command ``ray start --dashboard-port XXXX``.

4. To test out the server, run ``curl localhost:8001/single``.  This should output ``Model received data: dummy input``.

5. To clean up, you can press Ctrl-C to stop the Gunicorn server, and run `ray stop` to stop the background Ray cluster.
