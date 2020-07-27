.. _rayserve:

============================================
Ray Serve: Scalable and Programmable Serving
============================================

.. image:: logo.svg
    :align: center
    :height: 250px
    :width: 400px

.. _rayserve-overview:

Ray Serve is a scalable model-serving library built on Ray.

For users, Ray Serve is:

- **Framework Agnostic**: Use the same toolkit to serve everything from deep learning models
  built with frameworks like :ref:`PyTorch <serve-pytorch-tutorial>`,
  :ref:`Tensorflow, and Keras <serve-tensorflow-tutorial>`, to :ref:`Scikit-Learn <serve-sklearn-tutorial>` models, to arbitrary business logic.
- **Python First**: Configure your model serving with pure Python code - no more YAML or
  JSON configs.

As a library, Ray Serve enables:

- :ref:`serve-split-traffic` with zero downtime, by decoupling routing logic from response handling logic.
- :ref:`serve-batching` is built in to help you meet your performance objectives. You can also use your model for batch and online processing.
- Because Serve is a library, it's esay to integrate it with other tools in your environment, such as CI/CD.

Since Serve is built on Ray, it also allows you to scale to many machines, in your datacenter or in cloud environments, and it allows you to leverage all of the other Ray frameworks.

.. note::
  If you want to try out Serve, join our `community slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`_
  and discuss in the #serve channel.


Installation
============

Ray Serve supports Python versions 3.5 and higher. To install Ray Serve:

.. code-block:: bash

  pip install "ray[serve]"

Ray Serve in 90 Seconds
=======================

Serve a function by defining a function, an endpoint, and a backend (in this case a stateless function) then
connecting the two by setting traffic from the endpoint to the backend.

.. literalinclude:: ../../../python/ray/serve/examples/doc/quickstart_function.py

Serve a stateful class by defining a class (``Counter``), creating an endpoint and a backend, then connecting
the two by setting traffic from the endpoint to the backend.

.. literalinclude:: ../../../python/ray/serve/examples/doc/quickstart_class.py

See :doc:`key-concepts` for more exhaustive coverage about Ray Serve and its core concepts.

Why Ray Serve?
==============

There are generally two ways of serving machine learning applications, both with serious limitations:
you can build using a **traditional webserver** - your own Flask app or you can use a cloud hosted solution.

The first approach is easy to get started with, but it's hard to scale each component. The second approach
requires vendor lock-in (SageMaker), framework specific tooling (TFServing), and a general
lack of flexibility.

Ray Serve solves these problems by giving a user the ability to leverage the simplicity
of deployment of a simple webserver but handles the complex routing, scaling, and testing logic
necessary for production deployments.

For more on the motivation behind Ray Serve, check out these `meetup slides <https://tinyurl.com/serve-meetup>`_.

When should I use Ray Serve?
----------------------------

Ray Serve is a simple (but flexible) tool for deploying, operating, and monitoring Python based machine learning models.
Ray Serve excels when scaling out to serve models in production is a necessity. This might be because of large scale batch processing
requirements or because you're going to serve a number of models behind different endpoints and may need to run A/B tests or control
traffic between different models.

If you plan on running on multiple machines, Ray Serve will serve you well.

What's next?
============

Check out the :doc:`key-concepts`, learn more about :doc:`advanced`, look at the :ref:`serve-faq`,
or head over to the :doc:`tutorials/index` to get started building your Ray Serve Applications.


