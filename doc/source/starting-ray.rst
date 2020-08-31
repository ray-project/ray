Starting Ray
============

This page covers how to start Ray on your single machine or cluster of machines.

.. contents:: :local:

Installation
------------

.. tabs::
  .. group-tab:: Python

    Install Ray with ``pip install -U ray``. For the latest wheels (a snapshot of the ``master`` branch), you can use the instructions at :ref:`install-nightlies`.

  .. group-tab:: Java

    Add `Ray API <https://mvnrepository.com/artifact/io.ray/ray-api>`_ and `Ray Runtime <https://mvnrepository.com/artifact/io.ray/ray-runtime>`_ as dependencies. We don't publish snapshot versions right now. Note that to start a multi-node Ray cluster, you'll also need to follow the installation instructions of Python version.

Starting Ray on a single machine
--------------------------------

You can start Ray the init API. It will start the local services that Ray uses to schedule remote tasks and actors and then connect to them. Note that you must initialize Ray before any tasks or actors are called.

.. tabs::
  .. code-tab:: python

    import ray
    # `function.remote()` will not work until `ray.init()` is called.
    ray.init()

  .. code-tab:: java
    import io.ray.Ray;

    public class MyRayApp {

      public static void main(String[] args) {
        // `Ray.task(FooClass::bar).remote()` will not work until `Ray.init()` is called.
        Ray.init();
        ...
      }
    }

To stop or restart Ray, use the shutdown API.

.. tabs::
  .. code-tab:: python

    import ray
    ray.init()
    ... # ray program
    ray.shutdown()

  .. code-tab:: java
    import io.ray.Ray;

    public class MyRayApp {

      public static void main(String[] args) {
        Ray.init();
        ... // ray program
        Ray.shutdown();
      }
    }

.. tabs::
  .. group-tab:: Python

    To check if Ray is initialized, you can call ``ray.is_initialized()``:

    .. code-block:: python

      import ray
      ray.init()
      assert ray.is_initialized() == True

      ray.shutdown()
      assert ray.is_initialized() == False

  .. group-tab:: Java

    Checking if Ray is initialized hasn't been implemented in Java yet.

See the `Configuration <configure.html>`__ documentation for the various ways to configure Ray.

Using Ray on a cluster
----------------------

There are two steps needed to use Ray in a distributed setting:

    1. You must first start the Ray cluster.
    2. Specify the address of the Ray cluster when initializing Ray in your code. This causes Ray to connect to the existing cluster instead of starting a new one on the local node.

If you have a Ray cluster specification (:ref:`ref-automatic-cluster`), you can launch a multi-node cluster with Ray initialized on each node with ``ray up``. **From your local machine/laptop**:

.. code-block:: bash

    ray up cluster.yaml

You can monitor the Ray cluster status with ``ray monitor cluster.yaml`` and ssh into the head node with ``ray attach cluster.yaml``.

.. tabs::
  .. group-tab:: Python

    You need to add the ``address`` parameter to ``ray.init`` (like ``ray.init(address=...)``). To connect your program to the Ray cluster, add the following to your Python script:

    .. code-block:: python

        ray.init(address="auto")

  .. group-tab:: Java

    Your jar files must be distributed manually to all the nodes of the Ray cluster before running your code. You also need to make sure the paths of jar files are the same between nodes. Let's say your jar files are located in ``/path/to/jars/``, all files under this path will be loaded by worker processes.

    To connect your program to the Ray cluster, run it like this:

        .. code-block:: bash

            java -classpath /path/to/jars/ \
              -Dray.job.resource-path=/path/to/jars/ \
              -Dray.redis.address=<ADDRESS> \
              <CLASS_NAME> <ARGS>

    .. note:: Specifying ``auto`` as the Redis address hasn't been implemented in Java yet. You need to provide the actual Redis address. You can find the address of the Redis server from the output of the ``ray up`` command.

Your Python script or Java code **only** needs to execute on one machine in the cluster (usually the head node).

.. note:: Without the address parameter, your Ray program will only be parallelized across a single machine!

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


Turning off parallelism (Python)
--------------------------------

.. caution:: This feature is maintained solely to help with debugging, so it's possible you may encounter some issues. If you do, please `file an issue <https://github.com/ray-project/ray/issues>`_.

By default, Ray will parallelize its workload. However, if you need to debug your Ray program, it may be easier to do everything on a single process. You can force all Ray functions to occur on a single process with ``local_mode`` by calling the following:

.. code-block:: python

    ray.init(local_mode=True)

Note that some behavior such as setting global process variables may not work as expected.

.. note:: This feature is not supported in Java. But we have the single process mode in Java to help with debugging. See `Single process mode (Java)`_ for details.

Single process mode (Java)
--------------------------

.. caution:: This feature is maintained solely to help with debugging, so it's possible you may encounter some issues. If you do, please `file an issue <https://github.com/ray-project/ray/issues>`_.

Single process mode for Java has some benefits:

    1. You don't need to install Python in your dev machine if you don't need to test or run your code in cluster mode.
    2. You can debug remote methods in any IDE you prefer.

Note that this is different from local mode for Python. The main differences are:

    1. In single process mode, tasks still run in parallel.
    2. In single process mode, you can't connect to an exisiting Ray cluster or start a new cluster. Everything runs in the Java process you created.

To run or debug your code in single process mode, you need to set the ``ray.run-mode`` parameter to ``SINGLE_PROCESS``. And you should not set the ``ray.redis.address`` parameter. e.g.

.. code-block:: bash

    java -classpath <CLASSPATH> \
      -Dray.run-mode=SINGLE_PROCESS \
      <CLASS_NAME> <ARGS>

Note that some behavior such as resource management may not work as expected.

.. note:: This feature is not supported in Python. But we have the local mode in Python to help with debugging. See `Turning off parallelism (Python)`_ for details.

What's next?
------------

Check out our `Deployment section <cluster-index.html>`_ for more information on deploying Ray in different settings, including Kubernetes, YARN, and SLURM.
