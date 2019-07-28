Ray
===

.. raw:: html

  <embed>
    <a href="https://github.com/ray-project/ray"><img style="position: absolute; top: 0; right: 0; border: 0;" src="https://camo.githubusercontent.com/365986a132ccd6a44c23a9169022c0b5c890c387/68747470733a2f2f73332e616d617a6f6e6177732e636f6d2f6769746875622f726962626f6e732f666f726b6d655f72696768745f7265645f6161303030302e706e67" alt="Fork me on GitHub" data-canonical-src="https://s3.amazonaws.com/github/ribbons/forkme_right_red_aa0000.png"></a>
  </embed>

*Ray is a fast and simple framework for building and running distributed applications.*


Ray is easy to install: ``pip install ray``

Example Use
-----------

+------------------------------------------------+----------------------------------------------------+
| **Basic Python**                               | **Distributed with Ray**                           |
+------------------------------------------------+----------------------------------------------------+
|.. code-block:: python                          |.. code-block:: python                              |
|                                                |                                                    |
|  # Execute f serially.                         |  # Execute f in parallel.                          |
|                                                |                                                    |
|                                                |  @ray.remote                                       |
|  def f():                                      |  def f():                                          |
|      time.sleep(1)                             |      time.sleep(1)                                 |
|      return 1                                  |      return 1                                      |
|                                                |                                                    |
|                                                |                                                    |
|                                                |  ray.init()                                        |
|  results = [f() for i in range(4)]             |  results = ray.get([f.remote() for i in range(4)]) |
+------------------------------------------------+----------------------------------------------------+

To launch a Ray cluster, either privately, on AWS, or on GCP, `follow these instructions <autoscaling.html>`_.

View the `codebase on GitHub`_.

.. _`codebase on GitHub`: https://github.com/ray-project/ray

Ray comes with libraries that accelerate deep learning and reinforcement learning development:

- `Tune`_: Scalable Hyperparameter Search
- `RLlib`_: Scalable Reinforcement Learning
- `Distributed Training <distributed_training.html>`__

.. _`Tune`: tune.html
.. _`RLlib`: rllib.html


.. toctree::
   :maxdepth: 1
   :caption: Installation

   installation.rst
   deploy-on-kubernetes.rst
   install-on-docker.rst
   installation-troubleshooting.rst

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   tutorial.rst
   api.rst
   actors.rst
   using-ray-with-gpus.rst
   signals.rst
   async_api.rst

.. toctree::
   :maxdepth: 1
   :caption: Cluster Usage

   autoscaling.rst
   using-ray-on-a-cluster.rst

.. toctree::
   :maxdepth: 1
   :caption: Tune

   tune.rst
   tune-tutorial.rst
   tune-usage.rst
   tune-distributed.rst
   tune-schedulers.rst
   tune-searchalg.rst
   tune-package-ref.rst
   tune-design.rst
   tune-examples.rst
   tune-contrib.rst

.. toctree::
   :maxdepth: 1
   :caption: RLlib

   rllib.rst
   rllib-training.rst
   rllib-env.rst
   rllib-models.rst
   rllib-algorithms.rst
   rllib-offline.rst
   rllib-concepts.rst
   rllib-examples.rst
   rllib-dev.rst
   rllib-package-ref.rst

.. toctree::
   :maxdepth: 1
   :caption: Other Libraries

   distributed_training.rst
   pandas_on_ray.rst

.. toctree::
   :maxdepth: 1
   :caption: Examples

   example-rl-pong.rst
   example-policy-gradient.rst
   example-parameter-server.rst
   example-newsreader.rst
   example-resnet.rst
   example-a3c.rst
   example-lbfgs.rst
   example-evolution-strategies.rst
   example-cython.rst
   example-streaming.rst
   using-ray-with-tensorflow.rst

.. toctree::
   :maxdepth: 1
   :caption: Design

   internals-overview.rst
   serialization.rst
   fault-tolerance.rst
   plasma-object-store.rst
   resources.rst
   tempfile.rst

.. toctree::
   :maxdepth: 1
   :caption: Help

   troubleshooting.rst
   user-profiling.rst
   security.rst
   development.rst
   profiling.rst
   contact.rst
