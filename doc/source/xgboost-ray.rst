.. _xgboost-ray:

XGBoost on Ray
==============

This library adds a new backend for XGBoost utilizing Ray.

Please note that this is an early version and both the API and
the behavior can change without prior notice.

Installation
------------

You can install XGBoost on Ray (``xgboost_ray``) like this:

.. code-block:: bash

    git clone https://github.com/ray-project/xgboost_ray.git
    cd xgboost_ray
    pip install -e .


Usage
-----

After installation, you can import XGBoost on Ray via two ways:

.. code-block:: bash

    import xgboost_ray
    # or
    import ray.util.xgboost


``xgboost_ray`` provides a drop-in replacement for XGBoost's ``train``
function. To pass data, instead of using ``xgb.DMatrix`` you will
have to use ``ray.util.xgboost.RayDMatrix``.

Here is a simplified example:


.. literalinclude:: /../../python/ray/util/xgboost/simple_example.py
   :language: python
   :start-after:  __xgboost_begin__
   :end-before:  __xgboost_end__



Data loading
------------

Data is passed to ``xgboost_ray`` via a ``RayDMatrix`` object.

The ``RayDMatrix`` lazy loads data and stores it sharded in the
Ray object store. The Ray XGBoost actors then access these
shards to run their training on.

A ``RayDMatrix`` support various data and file types, like
Pandas DataFrames, Numpy Arrays, CSV files and Parquet files.

Example loading multiple parquet files:

.. code-block:: python

    import glob
    from ray.util.xgboost import RayDMatrix, RayFileType

    # We can also pass a list of files
    path = list(sorted(glob.glob("/data/nyc-taxi/*/*/*.parquet")))

    # This argument will be passed to pd.read_parquet()
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
        filetype=RayFileType.PARQUET)


Hyperparameter Tuning
---------------------
``xgboost_ray`` integrates with Ray Tune (:ref:`tune-main`) to provide distributed hyperparameter tuning for your
distributed XGBoost models. You can run multiple ``xgboost_ray`` training runs in parallel, each with a different
hyperparameter configuration, with each individual training run parallelized.

First, move your training code into a function. This function should take in a ``config`` argument which specifies the
hyperparameters for the xgboost model.

.. literalinclude:: /../../python/ray/util/xgboost/simple_tune.py
   :language: python
   :start-after:  __train_begin__
   :end-before:  __train_end__

Then, you import tune and use tune's search primitives to define a hyperparameter search space.

.. literalinclude:: /../../python/ray/util/xgboost/simple_tune.py
   :language: python
   :start-after:  __tune_begin__
   :end-before:  __tune_end__

Finally, you call ``tune.run`` passing in the training function and the ``config``. Internally, tune will resolve the
hyperparameter search space and invoke the training function multiple times, each with different hyperparameters.

.. literalinclude:: /../../python/ray/util/xgboost/simple_tune.py
   :language: python
   :start-after:  __tune_run_begin__
   :end-before:  __tune_run_end__

Make sure you set the ``extra_cpu`` field appropriately so tune is aware of the total number of resources each trial
requires.


Resources
---------

By default, ``xgboost_ray`` tries to determine the number of CPUs
available and distributes them evenly across actors.

In the case of very large clusters or clusters with many different
machine sizes, it makes sense to limit the number of CPUs per actor
by setting the ``cpus_per_actor`` argument. Consider always
setting this explicitly.

The number of XGBoost actors always has to be set manually with
the ``num_actors`` argument.

More examples
-------------

Fore complete end to end examples, please have a look at
the `examples folder <https://github.com/ray-project/xgboost_ray/tree/master/examples/>`__:

* `Simple sklearn breastcancer dataset example <https://github.com/ray-project/xgboost_ray/tree/master/examples/simple.py>`__ (requires `sklearn`)
* `Simple sklearn breastcancer dataset example with Ray Tune <ttps://github.com/ray-project/xgboost_ray/tree/master/examples/simple_tune.py>`__ (requires `sklearn`)
* `HIGGS classification example <https://github.com/ray-project/xgboost_ray/tree/master/examples/higgs.py>`__
  * `[download dataset (2.6 GB)] <https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz>`__
* `HIGGS classification example with Parquet <https://github.com/ray-project/xgboost_ray/tree/master/examples/higgs_parquet.py>`__ (uses the same dataset)
* `Test data classification <https://github.com/ray-project/xgboost_ray/tree/master/examples/train_on_test_data.py>`__ (uses a self-generated dataset)
