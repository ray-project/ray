.. meta::
   :description: Scale multiprocessing.Pool programs to a Ray cluster with ray.util.multiprocessing.Pool as a drop-in replacement.

.. _ray-multiprocessing:

Distributed multiprocessing.Pool
================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

Ray supports running distributed Python programs with the `multiprocessing.Pool API`_
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

Experimental autoscaling
------------------------

By default, ``Pool`` eagerly creates a fixed number of actors. Supplying any of
``min_size``, ``max_size``, ``initial_size``, or ``idle_timeout_s`` instead
creates an autoscaling pool that creates actors on demand and retires them after
they have been idle:

.. code-block:: python

  from ray.util.multiprocessing import Pool

  pool = Pool(
      min_size=0,
      max_size=64,
      initial_size=0,
      idle_timeout_s=60,
  )
  results = pool.map(f, range(1000))

Autoscaling actors request one CPU by default. Pending actors therefore expose
resource demand to the Ray autoscaler, while work is submitted only to actors
that are ready. ``max_size`` defaults to ``processes`` when it is given,
otherwise to the number of cluster CPUs; set ``max_size`` explicitly when
connecting to a cluster whose head node has zero CPUs and whose workers start
at zero. Actors that crash are replaced automatically.

``min_size`` is the minimum number of actors retained, ``max_size`` caps the
pool, ``initial_size`` controls how many actors are pre-warmed, and
``idle_timeout_s`` controls when idle actors are retired. The feature is
experimental and the default fixed-pool behavior is unchanged.

The :ref:`Ray joblib backend <ray-joblib>` exposes the same options.

Run on a Cluster
----------------

This section assumes that you have a running Ray cluster. To start a Ray cluster,
see the :ref:`cluster setup <cluster-index>` instructions.

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
