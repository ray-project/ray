.. _xgboost-ray:

XGBoost on Ray
==============

This library adds a new backend for XGBoost utilizing Ray.

Please note that this is an early version and both the API and
the behavior can change without prior notice.

Installation
------------

You can install ``xgboost_ray`` like this:

.. code-block:: bash

    git clone https://github.com/ray-project/xgboost_ray.git
    cd xgboost_ray
    pip install -e .


Usage
-----

``xgboost_ray`` provides a drop-in replacement for XGBoost's ``train``
function. To pass data, instead of using ``xgb.DMatrix`` you will
have to use ``xgboost_ray.RayDMatrix``.

Here is a simplified example:

.. code-block:: python

    from xgboost_ray import RayDMatrix, train

    train_x, train_y = None, None  # Load data here
    train_set = RayDMatrix(train_x, train_y)

    evals_result = {}
    bst = train(
        {
            "objective": "binary:logistic",
            "eval_metric": ["logloss", "error"],
        },
        train_set,
        evals_result=evals_result,
        evals=[(train_set, "train")],
        verbose_eval=False,
        num_actors=2,
        cpus_per_actor=1)

    bst.save_model("model.xgb")
    print("Final training error: {:.4f}".format(
        evals_result["train"]["error"][-1]))


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
    from xgboost_ray import RayDMatrix, RayFileType

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
* `HIGGS classification example <https://github.com/ray-project/xgboost_ray/tree/master/examples/higgs.py>`__
`[download dataset (2.6 GB)] <https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz>`__
* `HIGGS classification example with Parquet <https://github.com/ray-project/xgboost_ray/tree/master/examples/higgs_parquet.py>`__ (uses the same dataset)
* `Test data classification <https://github.com/ray-project/xgboost_ray/tree/master/examples/train_on_test_data.py>`__ (uses a self-generated dataset)

Package Reference
-----------------

Training/Validation
~~~~~~~~~~~~~~~~~~~

.. autofunction:: xgboost_ray.train

.. autofunction:: xgboost_ray.predict

RayDMatrix
~~~~~~~~~~

.. autoclass:: xgboost_ray.RayDMatrix

.. autoclass:: xgboost_ray.RayFileType

.. autoclass:: xgboost_ray.Data

.. autoclass:: xgboost_ray.RayShardingMode

.. autofunction:: xgboost_ray.combine_data
