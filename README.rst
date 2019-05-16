.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png

.. image:: https://travis-ci.com/ray-project/ray.svg?branch=master
    :target: https://travis-ci.com/ray-project/ray

.. image:: https://readthedocs.org/projects/ray/badge/?version=latest
    :target: http://ray.readthedocs.io/en/latest/?badge=latest

.. image:: https://img.shields.io/badge/pypi-0.6.6-blue.svg
    :target: https://pypi.org/project/ray/

|

**Ray is a flexible, high-performance distributed execution framework.**


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


Ray comes with libraries that accelerate deep learning and reinforcement learning development:

- `Tune`_: Hyperparameter Optimization Framework
- `RLlib`_: Scalable Reinforcement Learning
- `Distributed Training <http://ray.readthedocs.io/en/latest/distributed_sgd.html>`__

.. _`Tune`: http://ray.readthedocs.io/en/latest/tune.html
.. _`RLlib`: http://ray.readthedocs.io/en/latest/rllib.html

Installation
------------

Ray can be installed on Linux and Mac with ``pip install ray``.

To build Ray from source or to install the nightly versions, see the `installation documentation`_.

.. _`installation documentation`: http://ray.readthedocs.io/en/latest/installation.html

More Information
----------------

- `Documentation`_
- `Tutorial`_
- `Blog`_
- `Ray paper`_
- `Ray HotOS paper`_

.. _`Documentation`: http://ray.readthedocs.io/en/latest/index.html
.. _`Tutorial`: https://github.com/ray-project/tutorial
.. _`Blog`: https://ray-project.github.io/
.. _`Ray paper`: https://arxiv.org/abs/1712.05889
.. _`Ray HotOS paper`: https://arxiv.org/abs/1703.03924

Getting Involved
----------------

- `ray-dev@googlegroups.com`_: For discussions about development or any general
  questions.
- `StackOverflow`_: For questions about how to use Ray.
- `GitHub Issues`_: For reporting bugs and feature requests.
- `Pull Requests`_: For submitting code contributions.

.. _`ray-dev@googlegroups.com`: https://groups.google.com/forum/#!forum/ray-dev
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`Pull Requests`: https://github.com/ray-project/ray/pulls
