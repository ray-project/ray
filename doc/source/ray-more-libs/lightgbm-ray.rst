..
  This part of the docs is generated from the LightGBM-Ray readme using m2r
  To update:
  - run `m2r /path/to/lightgbm_ray/README.md`
  - copy the contents of README.rst here
  - Get rid of the badges in the top
  - Get rid of the references section at the bottom
  - Be sure not to delete the API reference section in the bottom of this file.
  - add `.. _lightgbm-ray-tuning:` before the "Hyperparameter Tuning" section
  - Adjust some link targets (e.g. for "Ray Tune") to anonymous references
    by adding a second underscore (use `target <link>`__)
  - Search for `\ **` and delete this from the links (bold links are not supported)

.. _lightgbm-ray:

Distributed LightGBM on Ray
===========================

LightGBM-Ray is a distributed backend for
`LightGBM <https://lightgbm.readthedocs.io/>`_\ , built
on top of
`distributed computing framework Ray <https://ray.io>`_.

LightGBM-Ray


* enables `multi-node <#usage>`_ and `multi-GPU <#multi-gpu-training>`_ training
* integrates seamlessly with distributed `hyperparameter optimization <#hyperparameter-tuning>`_ library `Ray Tune <http://tune.io>`__
* comes with `fault tolerance handling <#fault-tolerance>`_ mechanisms, and
* supports `distributed dataframes and distributed data loading <#distributed-data-loading>`_

All releases are tested on large clusters and workloads.

This package is based on :ref:`XGBoost-Ray <xgboost-ray>`. As of now, XGBoost-Ray is a dependency for LightGBM-Ray.

Installation
------------

You can install the latest LightGBM-Ray release from PIP:

.. code-block:: bash

   pip install lightgbm_ray

If you'd like to install the latest master, use this command instead:

.. code-block:: bash

   pip install git+https://github.com/ray-project/lightgbm_ray.git#lightgbm_ray

Usage
-----

LightGBM-Ray provides a drop-in replacement for LightGBM's ``train``
function. To pass data, a ``RayDMatrix`` object is required, common
with XGBoost-Ray. You can also use a scikit-learn
interface - see next section.

Just as in original ``lgbm.train()`` function, the 
`training parameters <https://lightgbm.readthedocs.io/en/latest/Parameters.html>`_
are passed as the ``params`` dictionary.

Ray-specific distributed training parameters are configured with a
``lightgbm_ray.RayParams`` object. For instance, you can set
the ``num_actors`` property to specify how many distributed actors
you would like to use.

Here is a simplified example (which requires ``sklearn``\ ):

**Training:**

.. code-block:: python

   from lightgbm_ray import RayDMatrix, RayParams, train
   from sklearn.datasets import load_breast_cancer

   train_x, train_y = load_breast_cancer(return_X_y=True)
   train_set = RayDMatrix(train_x, train_y)

   evals_result = {}
   bst = train(
       {
           "objective": "binary",
           "metric": ["binary_logloss", "binary_error"],
       },
       train_set,
       evals_result=evals_result,
       valid_sets=[train_set],
       valid_names=["train"],
       verbose_eval=False,
       ray_params=RayParams(num_actors=2, cpus_per_actor=2))

   bst.booster_.save_model("model.lgbm")
   print("Final training error: {:.4f}".format(
       evals_result["train"]["binary_error"][-1]))

**Prediction:**

.. code-block:: python

   from lightgbm_ray import RayDMatrix, RayParams, predict
   from sklearn.datasets import load_breast_cancer
   import lightgbm as lgbm

   data, labels = load_breast_cancer(return_X_y=True)

   dpred = RayDMatrix(data, labels)

   bst = lgbm.Booster(model_file="model.lgbm")
   pred_ray = predict(bst, dpred, ray_params=RayParams(num_actors=2))

   print(pred_ray)

scikit-learn API
^^^^^^^^^^^^^^^^

LightGBM-Ray also features a scikit-learn API fully mirroring pure
LightGBM scikit-learn API, providing a completely drop-in
replacement. The following estimators are available:


* ``RayLGBMClassifier``
* ``RayLGBMRegressor``

Example usage of ``RayLGBMClassifier``\ :

.. code-block:: python

   from lightgbm_ray import RayLGBMClassifier, RayParams
   from sklearn.datasets import load_breast_cancer
   from sklearn.model_selection import train_test_split

   seed = 42

   X, y = load_breast_cancer(return_X_y=True)
   X_train, X_test, y_train, y_test = train_test_split(
       X, y, train_size=0.25, random_state=42)

   clf = RayLGBMClassifier(
       n_jobs=2,  # In LightGBM-Ray, n_jobs sets the number of actors
       random_state=seed)

   # scikit-learn API will automatically convert the data
   # to RayDMatrix format as needed.
   # You can also pass X as a RayDMatrix, in which case
   # y will be ignored.

   clf.fit(X_train, y_train)

   pred_ray = clf.predict(X_test)
   print(pred_ray)

   pred_proba_ray = clf.predict_proba(X_test)
   print(pred_proba_ray)

   # It is also possible to pass a RayParams object
   # to fit/predict/predict_proba methods - will override
   # n_jobs set during initialization

   clf.fit(X_train, y_train, ray_params=RayParams(num_actors=2))

   pred_ray = clf.predict(X_test, ray_params=RayParams(num_actors=2))
   print(pred_ray)

Things to keep in mind:


* ``n_jobs`` parameter controls the number of actors spawned.
  You can pass a ``RayParams`` object to the
  ``fit``\ /\ ``predict``\ /\ ``predict_proba`` methods as the ``ray_params`` argument
  for greater control over resource allocation. Doing
  so will override the value of ``n_jobs`` with the value of
  ``ray_params.num_actors`` attribute. For more information, refer
  to the `Resources <#resources>`_ section below.
* By default ``n_jobs`` is set to ``1``\ , which means the training
  will **not** be distributed. Make sure to either set ``n_jobs``
  to a higher value or pass a ``RayParams`` object as outlined above
  in order to take advantage of LightGBM-Ray's functionality.
* After calling ``fit``\ , additional evaluation results (e.g. training time,
  number of rows, callback results) will be available under
  ``additional_results_`` attribute.
* ``eval_`` arguments are supported, but early stopping is not.
* LightGBM-Ray's scikit-learn API is based on LightGBM 3.2.1.
  While we try to support older LightGBM versions, please note that
  this library is only fully tested and supported for LightGBM >= 3.2.1.

For more information on the scikit-learn API, refer to the `LightGBM documentation <https://lightgbm.readthedocs.io/en/latest/Python-API.html#scikit-learn-api>`_.

Data loading
------------

Data is passed to LightGBM-Ray via a ``RayDMatrix`` object.

The ``RayDMatrix`` lazy loads data and stores it sharded in the
Ray object store. The Ray LightGBM actors then access these
shards to run their training on.

A ``RayDMatrix`` support various data and file types, like
Pandas DataFrames, Numpy Arrays, CSV files and Parquet files.

Example loading multiple parquet files:

.. code-block:: python

   import glob
   from lightgbm_ray import RayDMatrix, RayFileType

   # We can also pass a list of files
   path = list(sorted(glob.glob("/data/nyc-taxi/*/*/*.parquet")))

   # This argument will be passed to `pd.read_parquet()`
   columns = [
       "passenger_count",
       "trip_distance", "pickup_longitude", "pickup_latitude",
       "dropoff_longitude", "dropoff_latitude",
       "fare_amount", "extra", "mta_tax", "tip_amount",
       "tolls_amount", "total_amount"
   ]

   dtrain = RayDMatrix(
       path,
       label="passenger_count",  # Will select this column as the label
       columns=columns,
       # ignore=["total_amount"],  # Optional list of columns to ignore
       filetype=RayFileType.PARQUET)

.. _lightgbm-ray-tuning:

Hyperparameter Tuning
---------------------

LightGBM-Ray integrates with :ref:`Ray Tune <tune-main>` to provide distributed hyperparameter tuning for your
distributed LightGBM models. You can run multiple LightGBM-Ray training runs in parallel, each with a different
hyperparameter configuration, and each training run parallelized by itself. All you have to do is move your training
code to a function, and pass the function to ``tune.run``. Internally, ``train`` will detect if ``tune`` is being used and will
automatically report results to tune.

Example using LightGBM-Ray with Ray Tune:

.. code-block:: python

   from lightgbm_ray import RayDMatrix, RayParams, train
   from sklearn.datasets import load_breast_cancer

   num_actors = 2
   num_cpus_per_actor = 2

   ray_params = RayParams(
       num_actors=num_actors, cpus_per_actor=num_cpus_per_actor)

   def train_model(config):
       train_x, train_y = load_breast_cancer(return_X_y=True)
       train_set = RayDMatrix(train_x, train_y)

       evals_result = {}
       bst = train(
           params=config,
           dtrain=train_set,
           evals_result=evals_result,
           valid_sets=[train_set],
           valid_names=["train"],
           verbose_eval=False,
           ray_params=ray_params)
       bst.booster_.save_model("model.lgbm")

   from ray import tune

   # Specify the hyperparameter search space.
   config = {
       "objective": "binary",
       "metric": ["binary_logloss", "binary_error"],
       "eta": tune.loguniform(1e-4, 1e-1),
       "subsample": tune.uniform(0.5, 1.0),
       "max_depth": tune.randint(1, 9)
   }

   # Make sure to use the `get_tune_resources` method to set the `resources_per_trial`
   analysis = tune.run(
       train_model,
       config=config,
       metric="train-binary_error",
       mode="min",
       num_samples=4,
       resources_per_trial=ray_params.get_tune_resources())
   print("Best hyperparameters", analysis.best_config)

Also see examples/simple_tune.py for another example.

Fault tolerance
---------------

LightGBM-Ray leverages the stateful Ray actor model to
enable fault tolerant training. Currently, only non-elastic
training is supported.

Non-elastic training (warm restart)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When an actor or node dies, LightGBM-Ray will retain the
state of the remaining actors. In non-elastic training,
the failed actors will be replaced as soon as resources
are available again. Only these actors will reload their
parts of the data. Training will resume once all actors
are ready for training again.

You can configure this mode in the ``RayParams``\ :

.. code-block:: python

   from lightgbm_ray import RayParams

   ray_params = RayParams(
       max_actor_restarts=2,    # How often are actors allowed to fail, Default = 0
   )

Resources
---------

By default, LightGBM-Ray tries to determine the number of CPUs
available and distributes them evenly across actors.

It is important to note that distributed LightGBM needs at least
two CPUs per actor to function efficiently (without blocking).
Therefore, by default, at least two CPUs will be assigned to each actor,
and an exception will be raised if an actor has less than two CPUs.
It is possible to override this check by setting the
``allow_less_than_two_cpus`` argument to ``True``\ , though it is not
recommended, as it will negatively impact training performance.

In the case of very large clusters or clusters with many different
machine sizes, it makes sense to limit the number of CPUs per actor
by setting the ``cpus_per_actor`` argument. Consider always
setting this explicitly.

The number of LightGBM actors always has to be set manually with
the ``num_actors`` argument.

Multi GPU training
^^^^^^^^^^^^^^^^^^

LightGBM-Ray enables multi GPU training. The LightGBM core backend
will automatically handle communication.
All you have to do is to start one actor per GPU and set LightGBM's
``device_type`` to a GPU-compatible option, eg. ``gpu`` (see LightGBM
documentation for more details.)

For instance, if you have 2 machines with 4 GPUs each, you will want
to start 8 remote actors, and set ``gpus_per_actor=1``. There is usually
no benefit in allocating less (e.g. 0.5) or more than one GPU per actor.

You should divide the CPUs evenly across actors per machine, so if your
machines have 16 CPUs in addition to the 4 GPUs, each actor should have
4 CPUs to use.

.. code-block:: python

   from lightgbm_ray import RayParams

   ray_params = RayParams(
       num_actors=8,
       gpus_per_actor=1,
       cpus_per_actor=4,   # Divide evenly across actors per machine
   )

How many remote actors should I use?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This depends on your workload and your cluster setup.
Generally there is no inherent benefit of running more than
one remote actor per node for CPU-only training. This is because
LightGBM core can already leverage multiple CPUs via threading.

However, there are some cases when you should consider starting
more than one actor per node:


* For `multi GPU training <#multi-gpu-training>`_\ , each GPU should have a separate
  remote actor. Thus, if your machine has 24 CPUs and 4 GPUs,
  you will want to start 4 remote actors with 6 CPUs and 1 GPU
  each
* In a **heterogeneous cluster**\ , you might want to find the
  `greatest common divisor <https://en.wikipedia.org/wiki/Greatest_common_divisor>`_
  for the number of CPUs.
  E.g. for a cluster with three nodes of 4, 8, and 12 CPUs, respectively,
  you should set the number of actors to 6 and the CPUs per
  actor to 4.

Distributed data loading
------------------------

LightGBM-Ray can leverage both centralized and distributed data loading.

In **centralized data loading**\ , the data is partitioned by the head node
and stored in the object store. Each remote actor then retrieves their
partitions by querying the Ray object store. Centralized loading is used
when you pass centralized in-memory dataframes, such as Pandas dataframes
or Numpy arrays, or when you pass a single source file, such as a single CSV
or Parquet file.

.. code-block:: python

   from lightgbm_ray import RayDMatrix

   # This will use centralized data loading, as only one source file is specified
   # `label_col` is a column in the CSV, used as the target label
   ray_params = RayDMatrix("./source_file.csv", label="label_col")

In **distributed data loading**\ , each remote actor loads their data directly from
the source (e.g. local hard disk, NFS, HDFS, S3),
without a central bottleneck. The data is still stored in the
object store, but locally to each actor. This mode is used automatically
when loading data from multiple CSV or Parquet files. Please note that
we do not check or enforce partition sizes in this case - it is your job
to make sure the data is evenly distributed across the source files.

.. code-block:: python

   from lightgbm_ray import RayDMatrix

   # This will use distributed data loading, as four source files are specified
   # Please note that you cannot schedule more than four actors in this case.
   # `label_col` is a column in the Parquet files, used as the target label
   ray_params = RayDMatrix([
       "hdfs:///tmp/part1.parquet",
       "hdfs:///tmp/part2.parquet",
       "hdfs:///tmp/part3.parquet",
       "hdfs:///tmp/part4.parquet",
   ], label="label_col")

Lastly, LightGBM-Ray supports **distributed dataframe** representations, such
as :ref:`Ray Datasets <datasets>`,
`Modin <https://modin.readthedocs.io/en/latest/>`_ and
`Dask dataframes <https://docs.dask.org/en/latest/dataframe.html>`_
(used with :ref:`Dask on Ray <dask-on-ray>`).
Here, LightGBM-Ray will check on which nodes the distributed partitions
are currently located, and will assign partitions to actors in order to
minimize cross-node data transfer. Please note that we also assume here
that partition sizes are uniform.

.. code-block:: python

   from lightgbm_ray import RayDMatrix

   # This will try to allocate the existing Modin partitions
   # to co-located Ray actors. If this is not possible, data will
   # be transferred across nodes
   ray_params = RayDMatrix(existing_modin_df)


Data sources
^^^^^^^^^^^^

The following data sources can be used with a ``RayDMatrix`` object.

.. list-table::
   :header-rows: 1

   * - Type
     - Centralized loading
     - Distributed loading
   * - Numpy array
     - Yes
     - No
   * - Pandas dataframe
     - Yes
     - No
   * - Single CSV
     - Yes
     - No
   * - Multi CSV
     - Yes
     - Yes
   * - Single Parquet
     - Yes
     - No
   * - Multi Parquet
     - Yes
     - Yes
   * - :ref:`Ray Dataset <datasets>`
     - Yes
     - Yes
   * - `Petastorm <https://github.com/uber/petastorm>`_
     - Yes
     - Yes
   * - `Dask dataframe <https://docs.dask.org/en/latest/dataframe.html>`_
     - Yes
     - Yes
   * - `Modin dataframe <https://modin.readthedocs.io/en/latest/>`_
     - Yes
     - Yes


Memory usage
------------

Details coming soon.

**Best practices**

In order to reduce peak memory usage, consider the following
suggestions:


* Store data as ``float32`` or less. More precision is often
  not needed, and keeping data in a smaller format will
  help reduce peak memory usage for initial data loading.
* Pass the ``dtype`` when loading data from CSV. Otherwise,
  floating point values will be loaded as ``np.float64``
  per default, increasing peak memory usage by 33%.

Placement Strategies
--------------------

LightGBM-Ray leverages Ray's Placement Group API (https://docs.ray.io/en/master/placement-group.html)
to implement placement strategies for better fault tolerance.

By default, a SPREAD strategy is used for training, which attempts to spread all of the training workers
across the nodes in a cluster on a best-effort basis. This improves fault tolerance since it minimizes the
number of worker failures when a node goes down, but comes at a cost of increased inter-node communication
To disable this strategy, set the ``RXGB_USE_SPREAD_STRATEGY`` environment variable to 0. If disabled, no
particular placement strategy will be used.


When LightGBM-Ray is used with Ray Tune for hyperparameter tuning, a PACK strategy is used. This strategy
attempts to place all workers for each trial on the same node on a best-effort basis. This means that if a node
goes down, it will be less likely to impact multiple trials.

When placement strategies are used, LightGBM-Ray will wait for 100 seconds for the required resources
to become available, and will fail if the required resources cannot be reserved and the cluster cannot autoscale
to increase the number of resources. You can change the ``RXGB_PLACEMENT_GROUP_TIMEOUT_S`` environment variable to modify
how long this timeout should be.

More examples
-------------

For complete end to end examples, please have a look at
the `examples folder <https://github.com/ray-project/lightgbm_ray/tree/master/examples/>`_\ :

* `Simple sklearn breastcancer dataset example <https://github.com/ray-project/lightgbm_ray/blob/main/lightgbm_ray/examples/simple.py>`_ (requires ``sklearn``\ )
* `HIGGS classification example <https://github.com/ray-project/lightgbm_ray/blob/main/lightgbm_ray/examples/higgs.py>`_
  (\ `download dataset (2.6 GB) <https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz>`_\ )
* `HIGGS classification example with Parquet <https://github.com/ray-project/lightgbm_ray/blob/main/lightgbm_ray/examples/higgs_parquet.py>`_ (uses the same dataset)
* `Test data classification <https://github.com/ray-project/lightgbm_ray/blob/main/lightgbm_ray/examples/train_on_test_data.py>`_ (uses a self-generated dataset)

API reference
-------------

.. autoclass:: lightgbm_ray.RayParams
    :members:

.. note::
  The ``xgboost_ray.RayDMatrix`` class is shared with :ref:`XGBoost-Ray <xgboost-ray>`.

.. autoclass:: xgboost_ray.RayDMatrix
    :members:
    :noindex:

.. autofunction:: lightgbm_ray.train

.. autofunction:: lightgbm_ray.predict

scikit-learn API
^^^^^^^^^^^^^^^^

.. autoclass:: lightgbm_ray.RayLGBMClassifier
    :members:

.. autoclass:: lightgbm_ray.RayLGBMRegressor
    :members: