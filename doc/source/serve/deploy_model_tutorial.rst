====================================
End-to-End Model Deployment Tutorial
====================================

By the end of this tutorial you will have learned how to deploy a machine
learning model locally via Ray Serve.

First, install Ray Serve and all of its dependencies by running the following
command in your terminal:

.. code-block:: bash

  pip install "ray[serve]"

For this tutorial, we'll use `HuggingFace's SummarizationPipeline <https://huggingface.co/docs/transformers/main_classes/pipelines#transformers.SummarizationPipeline>`_
to access a model that summarizes text. Let's take a look at how it works.
This is the code for the model:

.. code-block:: python

  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_local.py
    :language: python
    :start-after: __local_model_start__
    :end-before: __local_model_end__

The Python file, called ``local_model.py`` uses the ``summarize`` function to
generate summaries of text. The ``summarizer`` variable inside ``summarize``
points to a function that uses the
`t5-small <https://huggingface.co/t5-small>_` model to summarize text.
When ``summarizer`` is called on a Python String, it returns summarized text
encapsulated within a dictionary and a list. ``summarize`` then removes the
dictionary and list, and it returns only the summary. The file can be run
locally by executing the Python script, which uses the model to summarize an
article about the Apollo 11 moon landing.

.. code-block:: bash
  $ python local_model.py

  two astronauts steered their fragile lunar module safely and smoothly to the
  historic landing . the first men to reach the moon -- Armstrong and his 
  co-pilot, col. Edwin E. Aldrin Jr. of the air force -- brought their ship to 
  rest on a level, rock-strewn plain .

This tutorial's goal is to deploy this model using Ray Serve, so it can be
queried over HTTP. We'll start by converting the above Python function into a
Ray Serve deployment that can be launched locally on a laptop.

We start by opening a new Python file. First, we need to import ``ray`` and
``ray serve``, to use features in Ray Serve such as ``deployments``, which
provide HTTP access to our model.

.. code-block:: python

  import ray
  from ray import serve
  import requests

After these imports, we can include our model code from above. 
We won't call our ``summarize`` function just yet though! 
We will soon add logic to handle HTTP requests, so the ``summarize`` function 
can operate on article text sent via HTTP request.

.. code-block:: python

  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_deployment.py
    :language: python
    :start-after: __local_model_start__
    :end-before: __local_model_end__

Ray Serve needs to run on top of a Ray Cluster, so we connect to a local one:

.. code-block:: python

  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_deployment.py
    :language: python
    :start-after: __start_ray_cluster_start__
    :end-before: __start_ray_cluster_end__

The ``address`` parameter in ``ray.init()`` connects your Serve script to a
running local Ray cluster. Later, we'll discuss how to start a local ray
cluster. The ``namespace`` parameter determines what Ray objects are accessible
in this script. Setting it to ``serve`` ensures that the Ray Serve objects we
create are accessible by default.

.. note::

  ``ray.init()`` will connect to or start a single-node Ray cluster on your
  local machine,  which will allow you to use all your CPU cores to serve
  requests in parallel. To start a multi-node cluster, see
  :doc:`../cluster/index`.

Next, we start the Ray Serve runtime:

.. code-block:: python

  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_deployment.py
    :language: python
    :start-after: __start_serve_start__
    :end-before: __start_serve_end__

.. note::

  ``detached=True`` means Ray Serve will continue running even when the Python
  script exits. If you would rather stop Ray Serve after the script exits, use
  ``serve.start()`` instead (see :doc:`deployment` for details).

Now that we have defined our ``summarize`` function, connected to a Ray
Cluster, and started the Ray Serve runtime, we can define a function that
accepts HTTP requests and routes them to the ``summarize`` function. We
define a function called ``router`` that takes in a Starlette ``request``
object [#f1]:

.. code-block:: python

  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_deployment.py
    :language: python
    :start-after: __router_start__
    :end-before: __router_end__

``router`` uses the ``txt`` parameter in the ``request`` to find the article
text to summarize. It then passes the article text into the ``summarize``
function and returns the value. We also add the decorator ``@serve.deployment``
to the ``router`` function to turn the function into a Serve ``Deployment``
object.

.. tip::
  This routing function's name doesn't have to be ``router``. 
  It can be any function name as long as the corresponding name is present in 
  the HTTP request. If you want the function name to be different than the name 
  in the HTTP request, you can add the ``name`` keyword parameter to the
  ``@serve.deployment`` decorator to specify the name sent in the HTTP request.
  For example, if the decorator is ``@serve.deployment(name="responder")`` and
  the function signature is ``def request_manager(request)``, the HTTP request
  should use ``responder``, not ``request_manager``. If no ``name`` is passed
  into ``@serve.deployment``, the ``request`` uses the function's name by
  default. For example, if the decorator is ``@serve.deployment`` and the
  function's signature is ``def manager(request)``, the HTTP request should use
  ``manager``.

Since ``@serve.deployment`` makes ``router`` a ``Deployment`` object, it can be
deployed using ``router.deploy()``:

.. code-block:: python

  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_deployment.py
    :language: python
    :start-after: __router_deploy_start__
    :end-before: __router_deploy_end__

Once we deploy ``router``, we can query the model over HTTP. 
With that, we can run our model on Ray Serve! 
Here's the full Ray Serve deployment script that we built for our model:

.. code-block:: python
  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_deployment_full.py
    :language: python
    :start-after: __deployment_full_start__
    :end-before: __deployment_full_end__

To deploy ``router``, we first start a local Ray cluster:

.. code-block:: bash
  $ ray start --head

The Ray cluster that this command launches is the same Ray cluster that the
Python code connects to using ``ray.init(address="auto", namespace="serve")``.
It is also the same Ray cluster that keeps Ray Serve (and any deployments on
it, such as ``router``) alive even after the Python script exits as long as
``detached=True`` inside ``serve.start()``.

.. tip::
  To stop the Ray cluster, run the command ``ray stop``.

After starting the Ray cluster, we can run the Python file to deploy ``router``
and begin accepting HTTP requests:

.. code-block:: bash
  $ python model_on_ray_serve.py

We can now test our model over HTTP. The structure of our query will be:

``http://127.0.0.1:8000/[Deployment Name]?[Parameter Name-1]=[Parameter Value-1]&[Parameter Name-2]=[Parameter Value-2]&...&[Parameter Name-n]=[Parameter Value-n]``

Since the cluster is deployed locally in this tutorial, the ``127.0.0.1:8000`` 
refers to a localhost with port 8000. The ``[Deployment Name]`` refers to
either the name of the function that we called ``.deploy()`` on (in our case,
this is ``router``), or the ``name`` keyword parameter's value in
``@serve.deployment`` (see the Tip under the ``router`` function definition
above for more info). Each ``[Parameter Name]`` refers to a field's name in the
request's ``query_params`` dictionary for our deployed function. In our
example, the only parameter we need to pass in is ``txt``. This parameter is
referenced in the ``txt = request.query_params["txt"]`` line in the ``router``
function. Each [Parameter Name] object has a corresponding [Parameter Value]
object. The ``txt``'s [Parameter Value] will be a string containing the article
text to summarize. We can chain together any number of the name-value pairs
using the ``&`` symbol in the request URL.

Now that the ``summarize`` function is deployed on Ray Serve, we can make HTTP 
requests to it. Here's a client script that requests a summary from the same 
article as the original Python script:

.. code-block:: python
  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_client.py
    :language: python
    :start-after: __client_function_start__
    :end-before: __client_function_end__

We can run this script while the model is deployed to get a response over HTTP:

..code-block:: bash
  $ python router_client.py

  two astronauts steered their fragile lunar module safely and smoothly to the
  historic landing . the first men to reach the moon -- Armstrong and his 
  co-pilot, col. Edwin E. Aldrin Jr. of the air force -- brought their ship to 
  rest on a level, rock-strewn plain .

Our application still a bit inefficient though. In particular, the 
``summarize`` function loads the model on each call when it sets the
``summarizer`` variable. However, the model never changes, so it would be more
efficient to define ``summarizer`` only once and keep its value in memory
instead of reloading it for each HTTP query.

We can achieve this by converting our ``summarize`` function into a class:

.. code-block:: python
  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_class_deployment.py
    :language: python
    :start-after: __deployment_class_start__
    :end-before: __deployment_class_end__

In this configuration, we can query the ``Summarizer`` class directly. 
The ``Summarizer`` is initialized once (after calling ``Summarizer.deploy()``).
Its ``__init__`` function loads and stores the model in ``self.summarize``.
HTTP queries for the ``Summarizer`` class will by default get routed to its
``__call__`` method, which takes in the Starlette ``request`` object. The
``Summarizer`` class can then take the request's ``txt`` data and call the
``self.summarize`` function on it without loading the model on each query.

HTTP queries for the Ray Serve class deployments follow a similar format to Ray 
Serve function deployments. Here's an example client script for the
``Summarizer`` class. Notice that the only difference from the ``router``'s
client script is that the URL uses the ``Summarizer`` path instead of
``router``.

.. code-block:: python
  .. literalinclude:: ../../../python/ray/serve/examples/doc/e2e_client.py
    :language: python
    :start-after: __client_class_start__
    :end-before: __client_class_end__

We can deploy the class-based model on Serve without stopping the Ray cluster.
However, for the purposes of this tutorial, let's restart the cluster, deploy
the model, and query it over HTTP:

.. code-block:: bash
  $ ray stop
  $ ray start --head
  $ python summarizer_on_ray_serve.py
  $ python summarizer_client.py

Congratulations! You just built and deployed a machine learning model on Ray
Serve! You should now have enough context to dive into the :doc:`core-apis` to
get a deeper understanding of Ray Serve.

To learn more about how to start a multi-node cluster for your Ray Serve
deployments, see :doc:`../cluster/index`. For more interesting example
applications, including integrations with popular machine learning frameworks
and Python web servers, be sure to check out :doc:`tutorials/index`.

.. rubric:: Footnotes

.. [#f1] `Starlette <https://www.starlette.io/>_` is a web server framework
used by Ray Serve. Its `Request <https://www.starlette.io/requests/>_` class
provides a nice interface for incoming HTTP requests.