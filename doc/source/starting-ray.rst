Starting Ray
============

This page covers how to start Ray on your single machine or cluster of machines.

.. contents:: :local:

Installation
------------

Install Ray with ``pip install -U ray``. For the latest wheels (a snapshot of the ``master`` branch), you can use the instructions at :ref:`install-nightlies`.

.. note:: This step is not required if you are writing a Ray application in Java and you don't have the need of running your Java application in a Ray cluster at the development stage. See `Local mode`_ for more details.

Build your Java code
--------------------

If your application is written in Java, you need to add Ray dependencies to your project in order to build it.

.. code-block:: xml

  <dependencies>
    <dependency>
      <groupId>io.ray</groupId>
      <artifactId>ray-api</artifactId>
      <version>...</version>
    </dependency>
    <dependency>
      <groupId>io.ray</groupId>
      <artifactId>ray-runtime</artifactId>
      <version>...</version>
    </dependency>
  </dependencies>

.. note::

  When you run ``pip install`` to install Ray, Java jars are installed as well. The above dependencies are only used to build your Java code and to run your code in local mode.

  If you want to run your Java code in a multi-node Ray cluster, it's better to exclude Ray jars when packaging your code to avoid jar conficts if the versions (installed Ray with ``pip install`` and maven dependencies) don't match.

Starting Ray on a single machine
--------------------------------

You can start Ray with the ``init`` API (see the code snippet below). It will start the local services that Ray uses to schedule remote tasks and actors and then connect to them. Note that you must initialize Ray before any tasks or actors are called.

.. tabs::
  .. code-tab:: python

    import ray
    # Other Ray APIs will not work until `ray.init()` is called.
    ray.init()

  .. code-tab:: java

    import io.ray.api.Ray;

    public class MyRayApp {

      public static void main(String[] args) {
        // Other Ray APIs will not work until `Ray.init()` is called.
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

    import io.ray.api.Ray;

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

    To check if Ray is initialized, you can call ``Ray.isInitialized()``:

    .. code-block:: java

      import io.ray.api.Ray;

      public class MyRayApp {

        public static void main(String[] args) {
          Ray.init();
          Assert.assertTrue(Ray.isInitialized());
          Ray.shutdown();
          Assert.assertFalse(Ray.isInitialized());
        }
      }

See the `Configuration <configure.html>`__ documentation for the various ways to configure Ray.

.. _using-ray-on-a-cluster:

Using Ray on a cluster
----------------------

There are two steps needed to use Ray in a distributed setting:

    1. You must first start the Ray cluster.

      If you have a Ray cluster specification (:ref:`ref-automatic-cluster`), you can launch a multi-node cluster with Ray initialized on each node with ``ray up``. **From your local machine/laptop**:

      .. code-block:: bash

          ray up cluster.yaml

      To configure the Ray cluster to run Java code, you need to add the ``--code-search-path`` option. It's used to specify classpath for workers in the cluster. Your jar files must be distributed to all the nodes of the Ray cluster before running your code. You also need to make sure the paths of jar files are the same among nodes.

      You can monitor the Ray cluster status with ``ray monitor cluster.yaml`` and ssh into the head node with ``ray attach cluster.yaml``.

    2. Specify the address of the Ray cluster when initializing Ray in your code. This causes Ray to connect to the existing cluster instead of starting a new one on the local node.

      .. tabs::
        .. group-tab:: Python

          You need to add the ``address`` parameter to ``ray.init`` (like ``ray.init(address=...)``). To connect your program to the Ray cluster, add the following to your Python script:

          .. code-block:: python

              ray.init(address="auto")

        .. group-tab:: Java

          You need to add the ``ray.redis.address`` parameter to your command line (like ``-Dray.redis.address=...``).

          To connect your program to the Ray cluster, run it like this:

              .. code-block:: bash

                  java -classpath /path/to/jars/ \
                    -Dray.redis.address=<address> \
                    <classname> <args>

          .. note:: Specifying ``auto`` as the Redis address hasn't been implemented in Java yet. You need to provide the actual Redis address. You can find the address of the Redis server from the output of the ``ray up`` command.

      Your driver code **only** needs to execute on one machine in the cluster (usually the head node).

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

If you want to run Java code, you need to specify the classpath via the ``--code-search-path`` option.

.. code-block:: bash

  $ ray start ... --code-search-path=/path/to/jars

Local mode
----------

.. caution:: This feature is maintained solely to help with debugging, so it's possible you may encounter some issues. If you do, please `file an issue <https://github.com/ray-project/ray/issues>`_.

By default, Ray will parallelize its workload and run tasks on multiple processes and multiple nodes. However, if you need to debug your Ray program, it may be easier to do everything on a single process. You can force all Ray functions to occur on a single process by enabling local mode as the following:

.. tabs::

  .. code-tab:: python

    ray.init(local_mode=True)

  .. group-tab:: Java

    .. code-block:: bash

      java -classpath <classpath> \
        -Dray.local-mode=true \
        <classname> <args>

Note that some behavior such as setting global process variables may not work as expected.

.. note:: If you just want to run your Java code in local mode, you can run it without Ray or even Python installed.

What's next?
------------

Check out our `Deployment section <cluster-index.html>`_ for more information on deploying Ray in different settings, including Kubernetes, YARN, and SLURM.
