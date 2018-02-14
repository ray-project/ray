Ray
===

.. raw:: html

  <embed>
    <a href="https://github.com/ray-project/ray"><img style="position: absolute; top: 0; right: 0; border: 0;" src="https://camo.githubusercontent.com/365986a132ccd6a44c23a9169022c0b5c890c387/68747470733a2f2f73332e616d617a6f6e6177732e636f6d2f6769746875622f726962626f6e732f666f726b6d655f72696768745f7265645f6161303030302e706e67" alt="Fork me on GitHub" data-canonical-src="https://s3.amazonaws.com/github/ribbons/forkme_right_red_aa0000.png"></a>
  </embed>

*Ray is a flexible, high-performance distributed execution framework.*

View the `codebase on GitHub`_.

.. _`codebase on GitHub`: https://github.com/ray-project/ray

Ray comes with libraries that accelerate deep learning and reinforcement learning development:

- `Ray Tune`_: Hyperparameter Optimization Framework
- `Ray RLlib`_: A Scalable Reinforcement Learning Library

.. _`Ray Tune`: tune.html
.. _`Ray RLlib`: rllib.html

Example Program
---------------

+------------------------------------------------+----------------------------------------------------+
| **Basic Python**                               | **Distributed with Ray**                           |
+------------------------------------------------+----------------------------------------------------+
|.. code:: python                                |.. code-block:: python                              |
|                                                |                                                    |
|  import time                                   |  import time                                       |
|                                                |  import ray                                        |
|                                                |                                                    |
|                                                |  ray.init()                                        |
|                                                |                                                    |
|                                                |  @ray.remote                                       |
|  def f():                                      |  def f():                                          |
|      time.sleep(1)                             |      time.sleep(1)                                 |
|      return 1                                  |      return 1                                      |
|                                                |                                                    |
|  # Execute f serially.                         |  # Execute f in parallel.                          |
|  results = [f() for i in range(4)]             |  results = ray.get([f.remote() for i in range(4)]) |
+------------------------------------------------+----------------------------------------------------+

.. toctree::
   :maxdepth: 1
   :caption: Installation

   install-on-ubuntu.rst
   install-on-macosx.rst
   install-on-docker.rst
   installation-troubleshooting.rst

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   tutorial.rst
   api.rst
   actors.rst
   using-ray-with-gpus.rst
   tune.rst
   rllib.rst
   rllib-dev.rst
   webui.rst

.. toctree::
   :maxdepth: 1
   :caption: Examples

   example-hyperopt.rst
   example-rl-pong.rst
   example-policy-gradient.rst
   example-parameter-server.rst
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

.. toctree::
   :maxdepth: 1
   :caption: Cluster Usage

   autoscaling.rst
   using-ray-on-a-cluster.rst
   using-ray-on-a-large-cluster.rst
   using-ray-and-docker-on-a-cluster.md

.. toctree::
   :maxdepth: 1
   :caption: Help

   troubleshooting.rst
   development.rst
   profiling.rst
   contact.rst
