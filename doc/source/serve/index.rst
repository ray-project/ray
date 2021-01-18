.. _rayserve:

============================================
Ray Serve: Scalable and Programmable Serving
============================================

.. image:: logo.svg
    :align: center
    :height: 250px
    :width: 400px

.. _rayserve-overview:

Ray Serve is an easy-to-use scalable model serving library built on Ray.  Ray Serve is:

- **Framework-agnostic**: Use a single toolkit to serve everything from deep learning models
  built with frameworks like :ref:`PyTorch <serve-pytorch-tutorial>`,
  :ref:`Tensorflow, and Keras <serve-tensorflow-tutorial>`, to :ref:`Scikit-Learn <serve-sklearn-tutorial>` models, to arbitrary Python business logic.
- **Python-first**: Configure your model serving with pure Python code---no more YAML or
  JSON configs.

Since Ray Serve is built on Ray, it allows you to easily scale to many machines, both in your datacenter and in the cloud.

Ray Serve can be used in two primary ways to deploy your models at scale:

1. Have Python functions and classes automatically placed behind HTTP endpoints.

2. Alternatively, call them from :ref:`within your existing Python web server <serve-web-server-integration-tutorial>` using the Python-native :ref:`servehandle-api`.



.. tip::
  Chat with Ray Serve users and developers on our `community Slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`_ in the #serve channel and on our `forum <https://discuss.ray.io/>`_!

.. note::
  Starting with Ray version 1.2.0, Ray Serve backends take in a Starlette Request object instead of a Flask Request object.
  See the `migration guide <https://docs.google.com/document/d/1CG4y5WTTc4G_MRQGyjnb_eZ7GK3G9dUX6TNLKLnKRAc/edit?usp=sharing>`_ for details.

Ray Serve Quickstart
====================

Ray Serve supports Python versions 3.6 through 3.8. To install Ray Serve, run the following command:

.. code-block:: bash

  pip install "ray[serve]"

Now you can serve a function...

.. literalinclude:: ../../../python/ray/serve/examples/doc/quickstart_function.py


...or serve a stateful class.

.. literalinclude:: ../../../python/ray/serve/examples/doc/quickstart_class.py


See :doc:`key-concepts` for more exhaustive coverage about Ray Serve and its core concepts: backends and endpoints.
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

Beyond scaling up your backends with multiple replicas, Ray Serve also enables:

- :ref:`serve-split-traffic` with zero downtime, by decoupling routing logic from response-handling logic.
- :ref:`serve-batching`---built in to help you meet your performance objectives.
- :ref:`serve-cpus-gpus`---specify fractional resource requirements to fully saturate each of your GPUs with several models.

For more on the motivation behind Ray Serve, check out these `meetup slides <https://tinyurl.com/serve-meetup>`_ and this `blog post <https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f>`_.

When should I use Ray Serve?
----------------------------

Ray Serve is a simple (but flexible) tool for deploying, operating, and monitoring Python-based machine learning models.
Ray Serve excels when scaling out to serve models in production is a necessity. This might be because of large-scale batch processing
requirements or because you're going to serve a number of models behind different endpoints and may need to run A/B tests or control
traffic between different models.

If you plan on running on multiple machines, Ray Serve will serve you well.

What's next?
============

Check out the :doc:`key-concepts`, learn more about :doc:`advanced`, look at the :ref:`serve-faq`,
or head over to the :doc:`tutorials/index` to get started building your Ray Serve applications.

For more, see the following blog posts about Ray Serve:

- `How to Scale Up Your FastAPI Application Using Ray Serve <https://medium.com/distributed-computing-with-ray/how-to-scale-up-your-fastapi-application-using-ray-serve-c9a7b69e786>`_ by Archit Kulkarni
- `Machine Learning is Broken <https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f>`_ by Simon Mo
- `The Simplest Way to Serve your NLP Model in Production with Pure Python <https://medium.com/distributed-computing-with-ray/the-simplest-way-to-serve-your-nlp-model-in-production-with-pure-python-d42b6a97ad55>`_ by Edward Oakes and Bill Chambers


