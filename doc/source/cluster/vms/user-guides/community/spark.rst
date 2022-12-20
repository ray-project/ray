.. _ray-Spark-deploy:

Deploying on Spark Standalone cluster
=====================================

This document describes a couple high-level steps to run Ray clusters on `Spark Standalone cluster <https://spark.apache.org/docs/latest/spark-standalone.html>`_.

Running a basic example
-----------------------

This is a spark application example code that starts Ray cluster on spark,
and then execute ray application code, then shut down initiated ray cluster.

1) Create a python file that contains a spark application code,
Assuming the python file name is 'ray-on-spark-example1.py'.

.. code-block:: python

    from pyspark.sql import SparkSession
    from ray.util.spark import init_ray_cluster, shutdown_ray_cluster, MAX_NUM_WORKER_NODES
    if __name__ == "__main__":
        spark = SparkSession \
            .builder \
            .appName("Ray on spark example 1") \
            .config("spark.task.cpus", "4") \
            .getOrCreate()

        # initiate a ray cluster on this spark application, it creates a background
        # spark job that each spark task launches one ray worker node.
        # ray head node is launched in spark application driver side.
        # Resources (CPU / GPU / memory) allocated to each ray worker node is equal
        # to resources allocated to the corresponding spark task.
        init_ray_cluster(num_worker_nodes=MAX_NUM_WORKER_NODES)

        # You can any ray application code here, the ray application will be executed
        # on the ray cluster setup above.
        # Note that you don't need to call `ray.init`.
        ...

        # Terminate ray cluster explicitly.
        # If you don't call it, when spark application is terminated, the ray cluster
        # will also be terminated.
        shutdown_ray_cluster()

2) Submit the spark application above to spark standalone cluster.

.. code-block:: bash

    #!/bin/bash
    spark-submit \
      --master spark://{spark_master_IP}:{spark_master_port} \
      path/to/ray-on-spark-example1.py

Creating a long running ray cluster on spark cluster
----------------------------------------------------

This is a spark application example code that starts a long running Ray cluster on spark.
The created ray cluster can be accessed by remote python processes.

1) Create a python file that contains a spark application code,
Assuming the python file name is 'long-running-ray-cluster-on-spark.py'.

.. code-block:: python

    from pyspark.sql import SparkSession
    import time
    from ray.util.spark import init_ray_cluster, MAX_NUM_WORKER_NODES

    if __name__ == "__main__":
        spark = SparkSession \
            .builder \
            .appName("long running ray cluster on spark") \
            .config("spark.task.cpus", "4") \
            .getOrCreate()

        cluster_address = init_ray_cluster(num_worker_nodes=MAX_NUM_WORKER_NODES)
        print("Ray cluster is initiated, you can connect to this ray cluster "
              f"via address ray://{cluster_address}")

        # Sleep forever until the spark application being terminated,
        # at that time, the ray cluster will also be terminated.
        while True:
            time.sleep(10)

2) Submit the spark application above to spark standalone cluster.

.. code-block:: bash

    #!/bin/bash
    spark-submit \
      --master spark://{spark_master_IP}:{spark_master_port} \
      path/to/long-running-ray-cluster-on-spark.py
