Ray
===

.. image:: https://travis-ci.org/ray-project/ray.svg?branch=master
    :target: https://travis-ci.org/ray-project/ray

.. image:: https://readthedocs.org/projects/ray/badge/?version=latest
    :target: http://ray.readthedocs.io/en/latest/?badge=latest

|

Ray is a flexible, high-performance distributed execution framework.


Overview
--------
+------------------------------------------------+--------------------------------------------+
| **Basic Python**                               | **Same code with Ray**                     |
+------------------------------------------------+--------------------------------------------+
| .. code:: python                               | .. code-block:: python                     |
|                                                |                                            |
|  import time                                   |   import time                              |
|                                                |   import ray                               |
|                                                |                                            |
|                                                |   ray.init(num_cpus=4)                     |
|                                                |                                            |
|                                                |   @ray.remote                              |
|  def f():                                      |   def f():                                 |
|      time.sleep(1)                             |       time.sleep(1)                        |
|      return True                               |       return True                          |
|                                                |                                            |
|  start = time.time()                           |   start = time.time()                      |
|                                                |                                            |
|  done = [f() for i in range(8)]                |   futures = [f.remote() for i in range(8)] |
|                                                |   done = ray.get(futures)                  |
|  end = time.time()                             |   end = time.time()                        |
|                                                |                                            |
|  print(end - start)                            |   print(end - start)                       |
|  # 8 seconds                                   |   # 2 seconds                              |
|                                                |                                            |
|                                                |                                            |
+------------------------------------------------+--------------------------------------------+


Installation
------------

- Ray can be installed on Linux and Mac with ``pip install ray``.
- To build Ray from source, see the instructions for `Ubuntu`_ and `Mac`_.

.. _`Ubuntu`: http://ray.readthedocs.io/en/latest/install-on-ubuntu.html
.. _`Mac`: http://ray.readthedocs.io/en/latest/install-on-macosx.html


More Information
----------------

- `Mailing List`_
- `Documentation`_
- `Blog`_
- `HotOS paper`_

.. _`Mailing List`: https://groups.google.com/forum/#!forum/ray-dev
.. _`Documentation`: http://ray.readthedocs.io/en/latest/index.html
.. _`Blog`: https://ray-project.github.io/
.. _`HotOS paper`: https://arxiv.org/abs/1703.03924
