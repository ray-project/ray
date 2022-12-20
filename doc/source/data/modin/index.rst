.. _modin-on-ray:

Using Pandas on Ray (Modin)
===========================

Modin_, previously Pandas on Ray, is a dataframe manipulation library that
allows users to speed up their pandas workloads by acting as a drop-in
replacement. Modin also provides support for other APIs (e.g. spreadsheet)
and libraries, like xgboost.

.. code-block:: python

   import modin.pandas as pd
   import ray

   ray.init()
   df = pd.read_parquet("s3://my-bucket/big.parquet")

You can use Modin on Ray with your laptop or cluster. In this document,
we show instructions for how to set up a Modin compatible Ray cluster
and connect Modin to Ray.

.. note:: In previous versions of Modin, you had to initialize Ray before importing Modin. As of Modin 0.9.0, This is no longer the case.

Using Modin with Ray's autoscaler
---------------------------------

In order to use Modin with :ref:`Ray's autoscaler <cluster-index>`, you need to ensure that the
correct dependencies are installed at startup. Modin's repository has an
example `yaml file and set of tutorial notebooks`_ to ensure that the Ray
cluster has the correct dependencies. Once the cluster is up, connect Modin
by simply importing.

.. code-block:: python

   import modin.pandas as pd
   import ray

   ray.init(address="auto")
   df = pd.read_parquet("s3://my-bucket/big.parquet")

As long as Ray is initialized before any dataframes are created, Modin
will be able to connect to and use the Ray cluster.

Modin with the Ray Client
-------------------------

When using Modin with the :ref:`Ray Client <ray-client-ref>`, it is important to ensure that the
cluster has all dependencies installed.

.. code-block:: python

   import modin.pandas as pd
   import ray
   import ray.util

   ray.init("ray://<head_node_host>:10001")
   df = pd.read_parquet("s3://my-bucket/big.parquet")

Modin will automatically use the Ray Client for computation when the file
is read.

How Modin uses Ray
------------------

Modin has a layered architecture, and the core abstraction for data manipulation
is the Modin Dataframe, which implements a novel algebra that enables Modin to
handle all of pandas (see Modin's documentation_ for more on the architecture).
Modin's internal dataframe object has a scheduling layer that is able to partition
and operate on data with Ray.

Dataframe operations
''''''''''''''''''''

The Modin Dataframe uses Ray tasks to perform data manipulations. Ray Tasks have
a number of benefits over the actor model for data manipulation:

- Multiple tasks may be manipulating the same objects simultaneously
- Objects in Ray's object store are immutable, making provenance and lineage easier
  to track
- As new workers come online the shuffling of data will happen as tasks are
  scheduled on the new node
- Identical partitions need not be replicated, especially beneficial for operations
  that selectively mutate the data (e.g. ``fillna``).
- Finer grained parallelism with finer grained placement control

Machine Learning
''''''''''''''''

Modin uses Ray Actors for the machine learning support it currently provides.
Modin's implementation of XGBoost is able to spin up one actor for each node
and aggregate all of the partitions on that node to the XGBoost Actor. Modin
is able to specify precisely the node IP for each actor on creation, giving
fine-grained control over placement - a must for distributed training
performance.

.. _Modin: https://github.com/modin-project/modin
.. _documentation: https://modin.readthedocs.io/en/latest/development/architecture.html
.. _yaml file and set of tutorial notebooks: https://github.com/modin-project/modin/tree/master/examples/tutorial/jupyter/execution/pandas_on_ray/cluster
