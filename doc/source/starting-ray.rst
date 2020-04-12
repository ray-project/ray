Starting Ray
============

This page covers how to start Ray on your single machine or cluster of machines.

.. contents:: :local:

Installation
------------

Install Ray with ``pip install -U ray``. For the latest wheels (a snapshot of the ``master`` branch), you can use the instructions at :ref:`install-nightlies`.

Starting Ray on a single machine
--------------------------------

You can start Ray by calling ``ray.init()`` in your Python script. This will start the local services that Ray uses to schedule remote tasks and actors and then connect to them. Note that you must initialize Ray before any tasks or actors are called (i.e., ``function.remote()`` will not work until `ray.init()` is called).

.. code-block:: python

  import ray
  ray.init()

To stop or restart Ray, use ``ray.shutdown()``.

.. code-block:: python

  import ray
  ray.init()
  ... # ray program
  ray.shutdown()


To check if Ray is initialized, you can call ``ray.is_initialized()``:

.. code-block:: python

  import ray
  ray.init()
  assert ray.is_initialized() == True

  ray.shutdown()
  assert ray.is_initialized() == False

See the `Configuration <configure.html>`__ documentation for the various ways to configure Ray.

Using Ray on a cluster
----------------------

There are two steps needed to use Ray in a distributed setting:

    1. You must first start the Ray cluster.
    2. You need to add the ``address`` parameter to ``ray.init`` (like ``ray.init(address=...)``). This causes Ray to connect to the existing cluster instead of starting a new one on the local node.

If you have a Ray cluster specification (:ref:`ref-automatic-cluster`), you can launch a multi-node cluster with Ray initialized on each node with ``ray up``. **From your local machine/laptop**:

.. code-block:: bash

    ray up cluster.yaml

You can monitor the Ray cluster status with ``ray monitor cluster.yaml`` and ssh into the head node with ``ray attach cluster.yaml``.

Your Python script **only** needs to execute on one machine in the cluster (usually the head node). To connect your program to the Ray cluster, add the following to your Python script:

.. code-block:: python

    ray.init(address="auto")

.. note:: Without ``ray.init(address...)``, your Ray program will only be parallelized across a single machine!

Manual cluster setup
~~~~~~~~~~~~~~~~~~~~

You can also use the manual cluster setup (:ref:`ref-cluster-setup`) by running initialization commands on each node.

**On the head node**:

.. code-block:: bash

    # If the ``--redis-port`` argument is omitted, Ray will choose a port at random.
    $ ray start --head --redis-port=6379

The command will print out the address of the Redis server that was started (and some other address information).

**Then on all of the other nodes**, run the following. Make sure to replace ``<address>`` with the value printed by the command on the head node (it should look something like ``123.45.67.89:6379``).

.. code-block:: bash

    $ ray start --address=<address>


Turning off parallelism
-----------------------

.. caution:: This feature is maintained solely to help with debugging, so it's possible you may encounter some issues. If you do, please `file an issue <https://github.com/ray-project/ray/issues>`_.

By default, Ray will parallelize its workload. However, if you need to debug your Ray program, it may be easier to do everything on a single process. You can force all Ray functions to occur on a single process with ``local_mode`` by calling the following:

.. code-block:: python

    ray.init(local_mode=True)

Note that some behavior such as setting global process variables may not work as expected.

What's next?
------------

Check out our `Deployment section <cluster-index.html>`_ for more information on deploying Ray in different settings, including Kubernetes, YARN, and SLURM.
