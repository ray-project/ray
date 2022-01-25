.. _ray-multiprocessing:

Distributed multiprocessing.Pool
================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

Ray supports running distributed python programs with the `multiprocessing.Pool API`_
using `Ray Actors <actors.html>`__ instead of local processes. This makes it easy
to scale existing applications that use ``multiprocessing.Pool`` from a single node
to a cluster.

.. _`multiprocessing.Pool API`: https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool

Quickstart
----------

To get started, first `install Ray <installation.html>`__, then use
``ray.util.multiprocessing.Pool`` in place of ``multiprocessing.Pool``.
This will start a local Ray cluster the first time you create a ``Pool`` and
distribute your tasks across it. See the `Run on a Cluster`_ section below for
instructions to run on a multi-node Ray cluster instead.

.. code-block:: python

  from ray.util.multiprocessing import Pool

  def f(index):
      return index

  pool = Pool()
  for result in pool.map(f, range(100)):
      print(result)

The full ``multiprocessing.Pool`` API is currently supported. Please see the
`multiprocessing documentation`_ for details.

.. warning::
  The ``context`` argument in the ``Pool`` constructor is ignored when using Ray.

.. _`multiprocessing documentation`: https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool

Run on a Cluster
----------------

This section assumes that you have a running Ray cluster. To start a Ray cluster,
please refer to the `cluster setup <cluster/index.html>`__ instructions.

To connect a ``Pool`` to a running Ray cluster, you can specify the address of the
head node in one of two ways:

- By setting the ``RAY_ADDRESS`` environment variable.
- By passing the ``ray_address`` keyword argument to the ``Pool`` constructor.

.. code-block:: python

  from ray.util.multiprocessing import Pool

  # Starts a new local Ray cluster.
  pool = Pool()

  # Connects to a running Ray cluster, with the current node as the head node.
  # Alternatively, set the environment variable RAY_ADDRESS="auto".
  pool = Pool(ray_address="auto")

  # Connects to a running Ray cluster, with a remote node as the head node.
  # Alternatively, set the environment variable RAY_ADDRESS="<ip_address>:<port>".
  pool = Pool(ray_address="<ip_address>:<port>")

You can also start Ray manually by calling ``ray.init()`` (with any of its supported
configuration options) before creating a ``Pool``.
