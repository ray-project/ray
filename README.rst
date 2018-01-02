Ray
===

.. image:: https://travis-ci.org/ray-project/ray.svg?branch=master
    :target: https://travis-ci.org/ray-project/ray

.. image:: https://readthedocs.org/projects/ray/badge/?version=latest
    :target: http://ray.readthedocs.io/en/latest/?badge=latest

|

Ray is a flexible, high-performance distributed execution framework.

Ray comes with libraries that accelerate deep learning and reinforcement learning development:

- `Ray.tune`_: Hyperparameter Optimization Framework
- `Ray RLlib`_: A Scalable Reinforcement Learning Library

.. _`Ray.tune`: http://ray.readthedocs.io/en/latest/tune.html
.. _`Ray RLlib`: http://ray.readthedocs.io/en/latest/rllib.html


Installation
------------

- Ray can be installed on Linux and Mac with ``pip install ray``.
- To build Ray from source, see the instructions for `Ubuntu`_ and `Mac`_.

.. _`Ubuntu`: http://ray.readthedocs.io/en/latest/install-on-ubuntu.html
.. _`Mac`: http://ray.readthedocs.io/en/latest/install-on-macosx.html


Example Program
---------------

+------------------------------------------------+----------------------------------------------+
| **Basic Python**                               | **Distributed with Ray**                     |
+------------------------------------------------+----------------------------------------------+
|.. code:: python                                |.. code-block:: python                        |
|                                                |                                              |
|  import time                                   |  import time                                 |
|                                                |  import ray                                  |
|                                                |                                              |
|                                                |  ray.init()                                  |
|                                                |                                              |
|                                                |  @ray.remote                                 |
|  def f():                                      |  def f():                                    |
|      time.sleep(1)                             |      time.sleep(1)                           |
|      return 1                                  |      return 1                                |
|                                                |                                              |
|  # Execute f serially.                         |  # Execute f in parallel.                    |
|  results = [f() for i in range(4)]             |  object_ids = [f.remote() for i in range(4)] |
|                                                |  results = ray.get(object_ids)               |
+------------------------------------------------+----------------------------------------------+


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

- Ask questions on our mailing list `ray-dev@googlegroups.com`_.
- Please report bugs by submitting a `GitHub issue`_.
- Submit contributions using `pull requests`_.

.. _`ray-dev@googlegroups.com`: https://groups.google.com/forum/#!forum/ray-dev
.. _`GitHub issue`: https://github.com/ray-project/ray/issues
.. _`pull requests`: https://github.com/ray-project/ray/pulls
