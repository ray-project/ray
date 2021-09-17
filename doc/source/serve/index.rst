.. _rayserve:

========================================
Serve: Scalable and Programmable Serving
========================================

.. warning::
  You're probably running the latest pip release and not the nightly build, so please ensure you're viewing the correct version of this documentation.
  `Here's the documentation for the latest pip release of Ray Serve <https://docs.ray.io/en/latest/serve/index.html>`_.


.. image:: logo.svg
    :align: center
    :height: 250px
    :width: 400px

.. _rayserve-overview:

Ray Serve is an easy-to-use scalable model serving library built on Ray.  Ray Serve is:

- **Framework-agnostic**: Use a single toolkit to serve everything from deep learning models
  built with frameworks like :ref:`PyTorch <serve-pytorch-tutorial>`,
  :ref:`Tensorflow, and Keras <serve-tensorflow-tutorial>`, to :ref:`Scikit-Learn <serve-sklearn-tutorial>` models, to arbitrary Python business logic.
- **Python-first**: Configure your model serving declaratively in pure Python, without needing YAML or JSON configs.

Since Ray Serve is built on Ray, it allows you to easily scale to many machines, both in your datacenter and in the cloud.

Ray Serve can be used in two primary ways to deploy your models at scale:

1. Have Python functions and classes automatically placed behind HTTP endpoints.

2. Alternatively, call them from :ref:`within your existing Python web server <serve-web-server-integration-tutorial>` using the Python-native :ref:`servehandle-api`.



.. tip::
  Chat with Ray Serve users and developers on our `forum <https://discuss.ray.io/>`_!
 

Ray Serve Quickstart
====================

Ray Serve supports Python versions 3.6 through 3.8. To install Ray Serve, run the following command:

.. code-block:: bash

  pip install "ray[serve]"

Now you can serve a function...

.. literalinclude:: ../../../python/ray/serve/examples/doc/quickstart_function.py


...or serve a stateful class.

.. literalinclude:: ../../../python/ray/serve/examples/doc/quickstart_class.py


See :doc:`core-apis` for more exhaustive coverage about Ray Serve and its core concept of a ``Deployment``.
For a high-level view of the architecture underlying Ray Serve, see :doc:`architecture`.

Why Ray Serve?
==============

There are generally two ways of serving machine learning applications, both with serious limitations:
you can use a **traditional web server**---your own Flask app---or you can use a cloud-hosted solution.

The first approach is easy to get started with, but it's hard to scale each component. The second approach
requires vendor lock-in (SageMaker), framework-specific tooling (TFServing), and a general
lack of flexibility.

Ray Serve solves these problems by giving you a simple web server (and the ability to :ref:`use your own <serve-web-server-integration-tutorial>`) while still handling the complex routing, scaling, and testing logic
necessary for production deployments.

Beyond scaling up your deployments with multiple replicas, Ray Serve also enables:

- :ref:`serve-model-composition`---ability to flexibly compose multiple models and independently scale and update each.
- :ref:`serve-batching`---built in request batching to help you meet your performance objectives.
- :ref:`serve-cpus-gpus`---specify fractional resource requirements to fully saturate each of your GPUs with several models.

For more on the motivation behind Ray Serve, check out these `meetup slides <https://tinyurl.com/serve-meetup>`_ and this `blog post <https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f>`_.

When should I use Ray Serve?
----------------------------

Ray Serve is a flexible tool that's easy to use for deploying, operating, and monitoring Python-based machine learning applications.
Ray Serve excels when you want to mix business logic with ML models and scaling out in production is a necessity. This might be because of large-scale batch processing
requirements or because you want to scale up a model pipeline consisting of many individual models with different performance properties.

If you plan on running on multiple machines, Ray Serve will serve you well!

What's next?
============

Check out the :doc:`tutorial` and :doc:`core-apis`, look at the :ref:`serve-faq`,
or head over to the :doc:`tutorials/index` to get started building your Ray Serve applications.

For more, see the following blog posts about Ray Serve:

- `How to Scale Up Your FastAPI Application Using Ray Serve <https://medium.com/distributed-computing-with-ray/how-to-scale-up-your-fastapi-application-using-ray-serve-c9a7b69e786>`_ by Archit Kulkarni
- `Machine Learning is Broken <https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f>`_ by Simon Mo
- `The Simplest Way to Serve your NLP Model in Production with Pure Python <https://medium.com/distributed-computing-with-ray/the-simplest-way-to-serve-your-nlp-model-in-production-with-pure-python-d42b6a97ad55>`_ by Edward Oakes and Bill Chambers
