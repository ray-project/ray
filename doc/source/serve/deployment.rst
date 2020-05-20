===================
Deploying Ray Serve
===================

In the :doc:`key-concepts`, you saw some of the basics of how to write serve applications.
This section will dive a bit deeper into how Ray Serve runs on a Ray cluster and how you're able 
to deploy and update your serve application over time.

To deploy a Ray Serve application (and cluster) you're going to need several things.

1. A running Ray cluster (you can deploy one on your local machine for testing).
2. A Ray Serve cluster To learn more about Ray clusters see :doc:`../cluster-index`.
3. Your Ray Serve endpoint(s) and backend(s).

.. contents:: Deploying Ray Serve

.. _serve-deploy-tutorial:

Deploying a Model with Ray Serve
================================

Let's get started deploying our first Ray Serve application. The first thing you'll need
to do is start a Ray cluster. You can do that using the Ray autoscaler, but in our case
we'll create it on our local machine. To learn more about Ray Clusters see :doc:`../cluster-index`.

Starting the Cluster
--------------------
We do that by running:

.. code::

    ray start --head

That starts a cluster on our local machine. We can shut that down by running ``ray stop``. You should 
run this after we complete this tutorial. 

Setup: Training a Model
-----------------------

Make sure you install `Scikit-learn <https://scikit-learn.org/stable/>`_.

Place the following in a python script and run it. In this example we're training
a model and saving it to disk for us to load into our Ray Serve app.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_import_train_begin__
    :end-before: __doc_import_train_end__

As discussed in other :doc:`tutorials/index`, we can use any framework to build these models. In general,
you'll just want to have the ability to persist these models to disk.

Now that we've trained that model and saved it to disk (keep in mind this could also be a service like S3),
we'll need to create a backend to serve the model.

Creating a Model and Serving it
-------------------------------

In the following snippet we will complete two things:
1. Define a servable model by instantiating a class and defining the ``__call__`` method.
2. Connect to our running Ray cluster(``ray.init(...)``) and then start or connect to the Ray Serve service
on that cluster(``serve.init(...)``).


You can see that defining the model is straightforward and simple, we're simply instantiating
the model like we might a typical Python class.

Configuring our model to accept traffic is specified via ``.set_traffic`` after we created
a backend in serve for our model (and versioned it with a string).

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_create_deploy_begin__
    :end-before: __doc_create_deploy_end__

What serve does when we run this code is store the model as a Ray actor 
and route traffic to it as the endpoint is queried, in this case over HTTP.

Let's now query our endpoint to see the result.

Querying our Endpoint
---------------------

We'll use the requests library to query our endpoint and be able to get a result.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__


Now that we defined a model and have it running on our Ray cluster. Let's proceed with updating
this model with a new set of code.

Updating Your Model Over Time
=============================

Updating our model is as simple as deploying the first one. While the code snippet includes 
a lot of information, all that we're doing is we are defining a new model, saving it, then loading 
it into serve. The key lines are at the end.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_create_deploy_2_begin__
    :end-before: __doc_create_deploy_2_end__

Consequentially, since Ray Serve runs as a service, all we need to tell it is that (a) there's a new model
and (b) how much traffic we should send to that model (and from what endpoint).

We do that with the line at the end of the code snippet, which allows us to split traffic between
these two models.

.. code::

    serve.set_traffic("iris_classifier", {"lr:v2": 0.25, "lr:v1": 0.75})

While this is a simple operation, you may want to see :ref:`serve-split-traffic` for more information. 
One thing you may want to consider as well is
:ref:`session-affinity` which gives you the ability to ensure that queries from users/clients always get mapped to the same backend.
versions.

Now that we're up and running serving two models in production, let's query 
our results several times to see some results. You'll notice that we're now splitting
traffic between these two different models.

Querying our Endpoint
---------------------

We'll use the requests library to query our endpoint and be able to get a result.

.. literalinclude:: ../../../python/ray/serve/examples/doc/tutorial_deploy.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__

If you run this code several times, you'll notice that the output will change - this 
is due to us running the two models in parallel that we created above.

Upon concluding the above tutorial, you'll want to run ``ray stop`` to 
shutdown the Ray cluster on your local machine.

Deployment FAQ
==============

Best practices for local development
------------------------------------

One thing you may notice is that we never have to declare a ``while True`` loop or 
something to keep the Ray Serve process running. In general, we don't recommend using forever loops and therefore 
opt for launching a Ray Cluster locally. Specify a Ray cluster like we did in :ref:`serve-deploy-tutorial`.
To learn more, in general, about Ray Clusters see :doc:`../cluster-index`.


Deploying Multiple Serve Clusters on a Single Ray Cluster
---------------------------------------------------------

You can run multiple serve clusters on the same Ray cluster by providing a ``cluster_name`` to ``serve.init()``.

.. code-block:: python

  # Create a first cluster whose HTTP server listens on 8000.
  serve.init(cluster_name="cluster1", http_port=8000)
  serve.create_endpoint("counter1", "/increment")

  # Create a second cluster whose HTTP server listens on 8001.
  serve.init(cluster_name="cluster2", http_port=8001)
  serve.create_endpoint("counter1", "/increment")

  # Create a backend that will be served on the second cluster.
  serve.create_backend("counter1", function)
  serve.set_traffic("counter1", {"counter1": 1.0})

  # Switch back the the first cluster and create the same backend on it.
  serve.init(cluster_name="cluster1")
  serve.create_backend("counter1", function)
  serve.set_traffic("counter1", {"counter1": 1.0})
