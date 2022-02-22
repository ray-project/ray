% This part of the docs is generated from the XGBoost-Ray readme using m2r
% To update:
% - run `m2r /path/to/xgboost_ray/README.md`
% - copy the contents of README.rst here
% - Get rid of the badges in the top
% - Get rid of the references section at the bottom
% - Be sure not to delete the API reference section in the bottom of this file.
% - add `.. _xgboost-ray-tuning:` before the "Hyperparameter Tuning" section
% - Adjust some link targets (e.g. for "Ray Tune") to anonymous references
%   by adding a second underscore (use `target <link>`__)
% - Search for `\ **` and delete this from the links (bold links are not supported)

(xgboost-ray)=

# Distributed XGBoost on Ray

XGBoost-Ray is a distributed backend for
[XGBoost](https://xgboost.readthedocs.io/en/latest/), built
on top of
[distributed computing framework Ray](https://ray.io).

XGBoost-Ray

- enables [multi-node](#usage) and [multi-GPU](#multi-gpu-training) training
- integrates seamlessly with distributed [hyperparameter optimization](#hyperparameter-tuning) library [Ray Tune](http://tune.io)
- comes with advanced [fault tolerance handling](#fault-tolerance) mechanisms, and
- supports [distributed dataframes and distributed data loading](#distributed-data-loading)

All releases are tested on large clusters and workloads.

## Installation

You can install the latest XGBoost-Ray release from PIP:

```bash
pip install "xgboost_ray"
```

If you'd like to install the latest master, use this command instead:

```bash
pip install "git+https://github.com/ray-project/xgboost_ray.git#egg=xgboost_ray"
```

## Usage

XGBoost-Ray provides a drop-in replacement for XGBoost's `train`
function. To pass data, instead of using `xgb.DMatrix` you will
have to use `xgboost_ray.RayDMatrix`.

Distributed training parameters are configured with a
`xgboost_ray.RayParams` object. For instance, you can set
the `num_actors` property to specify how many distributed actors
you would like to use.

Here is a simplified example (which requires `sklearn`):

**Training:**

```python
from xgboost_ray import RayDMatrix, RayParams, train
from sklearn.datasets import load_breast_cancer

train_x, train_y = load_breast_cancer(return_X_y=True)
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
    ray_params=RayParams(
        num_actors=2,  # Number of remote actors
        cpus_per_actor=1))

bst.save_model("model.xgb")
print("Final training error: {:.4f}".format(
    evals_result["train"]["error"][-1]))
```

**Prediction:**

```python
from xgboost_ray import RayDMatrix, RayParams, predict
from sklearn.datasets import load_breast_cancer
import xgboost as xgb

data, labels = load_breast_cancer(return_X_y=True)

dpred = RayDMatrix(data, labels)

bst = xgb.Booster(model_file="model.xgb")
pred_ray = predict(bst, dpred, ray_params=RayParams(num_actors=2))

print(pred_ray)
```

### scikit-learn API

XGBoost-Ray also features a scikit-learn API fully mirroring pure
XGBoost scikit-learn API, providing a completely drop-in
replacement. The following estimators are available:

- `RayXGBClassifier`
- `RayXGRegressor`
- `RayXGBRFClassifier`
- `RayXGBRFRegressor`
- `RayXGBRanker`

Example usage of `RayXGBClassifier`:

```python
from xgboost_ray import RayXGBClassifier, RayParams
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

seed = 42

X, y = load_breast_cancer(return_X_y=True)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, train_size=0.25, random_state=42
)

clf = RayXGBClassifier(
    n_jobs=4,  # In XGBoost-Ray, n_jobs sets the number of actors
    random_state=seed
)

# scikit-learn API will automatically conver the data
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
```

Things to keep in mind:

- `n_jobs` parameter controls the number of actors spawned.
  You can pass a `RayParams` object to the
  `fit`/`predict`/`predict_proba` methods as the `ray_params` argument
  for greater control over resource allocation. Doing
  so will override the value of `n_jobs` with the value of
  `ray_params.num_actors` attribute. For more information, refer
  to the [Resources](#resources) section below.
- By default `n_jobs` is set to `1`, which means the training
  will **not** be distributed. Make sure to either set `n_jobs`
  to a higher value or pass a `RayParams` object as outlined above
  in order to take advantage of XGBoost-Ray's functionality.
- After calling `fit`, additional evaluation results (e.g. training time,
  number of rows, callback results) will be available under
  `additional_results_` attribute.
- XGBoost-Ray's scikit-learn API is based on XGBoost 1.4.
  While we try to support older XGBoost versions, please note that
  this library is only fully tested and supported for XGBoost >= 1.4.

For more information on the scikit-learn API, refer to the [XGBoost documentation](https://xgboost.readthedocs.io/en/latest/python/python_api.html#module-xgboost.sklearn).

## Data loading

Data is passed to XGBoost-Ray via a `RayDMatrix` object.

The `RayDMatrix` lazy loads data and stores it sharded in the
Ray object store. The Ray XGBoost actors then access these
shards to run their training on.

A `RayDMatrix` support various data and file types, like
Pandas DataFrames, Numpy Arrays, CSV files and Parquet files.

Example loading multiple parquet files:

```python
import glob
from xgboost_ray import RayDMatrix, RayFileType

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
    filetype=RayFileType.PARQUET)
```

(xgboost-ray-tuning)=

## Hyperparameter Tuning

XGBoost-Ray integrates with [Ray Tune](https://tune.io) to provide distributed hyperparameter tuning for your
distributed XGBoost models. You can run multiple XGBoost-Ray training runs in parallel, each with a different
hyperparameter configuration, and each training run parallelized by itself. All you have to do is move your training
code to a function, and pass the function to `tune.run`. Internally, `train` will detect if `tune` is being used and will
automatically report results to tune.

Example using XGBoost-Ray with Ray Tune:

```python
from xgboost_ray import RayDMatrix, RayParams, train
from sklearn.datasets import load_breast_cancer

num_actors = 4
num_cpus_per_actor = 1

ray_params = RayParams(
    num_actors=num_actors,
    cpus_per_actor=num_cpus_per_actor)

def train_model(config):
    train_x, train_y = load_breast_cancer(return_X_y=True)
    train_set = RayDMatrix(train_x, train_y)

    evals_result = {}
    bst = train(
        params=config,
        dtrain=train_set,
        evals_result=evals_result,
        evals=[(train_set, "train")],
        verbose_eval=False,
        ray_params=ray_params)
    bst.save_model("model.xgb")

from ray import tune

# Specify the hyperparameter search space.
config = {
    "tree_method": "approx",
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error"],
    "eta": tune.loguniform(1e-4, 1e-1),
    "subsample": tune.uniform(0.5, 1.0),
    "max_depth": tune.randint(1, 9)
}

# Make sure to use the `get_tune_resources` method to set the `resources_per_trial`
analysis = tune.run(
    train_model,
    config=config,
    metric="train-error",
    mode="min",
    num_samples=4,
    resources_per_trial=ray_params.get_tune_resources())
print("Best hyperparameters", analysis.best_config)
```

Also see examples/simple_tune.py for another example.

## Fault tolerance

XGBoost-Ray leverages the stateful Ray actor model to
enable fault tolerant training. There are currently
two modes implemented.

### Non-elastic training (warm restart)

When an actor or node dies, XGBoost-Ray will retain the
state of the remaining actors. In non-elastic training,
the failed actors will be replaced as soon as resources
are available again. Only these actors will reload their
parts of the data. Training will resume once all actors
are ready for training again.

You can set this mode in the `RayParams`:

```python
from xgboost_ray import RayParams

ray_params = RayParams(
    elastic_training=False,  # Use non-elastic training
    max_actor_restarts=2,    # How often are actors allowed to fail
)
```

### Elastic training

In elastic training, XGBoost-Ray will continue training
with fewer actors (and on fewer data) when a node or actor
dies. The missing actors are staged in the background,
and are reintegrated into training once they are back and
loaded their data.

This mode will train on fewer data for a period of time,
which can impact accuracy. In practice, we found these
effects to be minor, especially for large shuffled datasets.
The immediate benefit is that training time is reduced
significantly to almost the same level as if no actors died.
Thus, especially when data loading takes a large part of
the total training time, this setting can dramatically speed
up training times for large distributed jobs.

You can configure this mode in the `RayParams`:

```python
from xgboost_ray import RayParams

ray_params = RayParams(
    elastic_training=True,  # Use elastic training
    max_failed_actors=3,    # Only allow at most 3 actors to die at the same time
    max_actor_restarts=2,   # How often are actors allowed to fail
)
```

## Resources

By default, XGBoost-Ray tries to determine the number of CPUs
available and distributes them evenly across actors.

In the case of very large clusters or clusters with many different
machine sizes, it makes sense to limit the number of CPUs per actor
by setting the `cpus_per_actor` argument. Consider always
setting this explicitly.

The number of XGBoost actors always has to be set manually with
the `num_actors` argument.

### Multi GPU training

XGBoost-Ray enables multi GPU training. The XGBoost core backend
will automatically leverage NCCL2 for cross-device communication.
All you have to do is to start one actor per GPU.

For instance, if you have 2 machines with 4 GPUs each, you will want
to start 8 remote actors, and set `gpus_per_actor=1`. There is usually
no benefit in allocating less (e.g. 0.5) or more than one GPU per actor.

You should divide the CPUs evenly across actors per machine, so if your
machines have 16 CPUs in addition to the 4 GPUs, each actor should have
4 CPUs to use.

```python
from xgboost_ray import RayParams

ray_params = RayParams(
    num_actors=8,
    gpus_per_actor=1,
    cpus_per_actor=4,   # Divide evenly across actors per machine
)
```

### How many remote actors should I use?

This depends on your workload and your cluster setup.
Generally there is no inherent benefit of running more than
one remote actor per node for CPU-only training. This is because
XGBoost core can already leverage multiple CPUs via threading.

However, there are some cases when you should consider starting
more than one actor per node:

- For [multi GPU training](#multi-gpu-training), each GPU should have a separate
  remote actor. Thus, if your machine has 24 CPUs and 4 GPUs,
  you will want to start 4 remote actors with 6 CPUs and 1 GPU
  each
- In a **heterogeneous cluster**, you might want to find the
  [greatest common divisor](https://en.wikipedia.org/wiki/Greatest_common_divisor)
  for the number of CPUs.
  E.g. for a cluster with three nodes of 4, 8, and 12 CPUs, respectively,
  you should set the number of actors to 6 and the CPUs per
  actor to 4.

## Distributed data loading

XGBoost-Ray can leverage both centralized and distributed data loading.

In **centralized data loading**, the data is partitioned by the head node
and stored in the object store. Each remote actor then retrieves their
partitions by querying the Ray object store. Centralized loading is used
when you pass centralized in-memory dataframes, such as Pandas dataframes
or Numpy arrays, or when you pass a single source file, such as a single CSV
or Parquet file.

```python
from xgboost_ray import RayDMatrix

# This will use centralized data loading, as only one source file is specified
# `label_col` is a column in the CSV, used as the target label
ray_params = RayDMatrix("./source_file.csv", label="label_col")
```

In **distributed data loading**, each remote actor loads their data directly from
the source (e.g. local hard disk, NFS, HDFS, S3),
without a central bottleneck. The data is still stored in the
object store, but locally to each actor. This mode is used automatically
when loading data from multiple CSV or Parquet files. Please note that
we do not check or enforce partition sizes in this case - it is your job
to make sure the data is evenly distributed across the source files.

```python
from xgboost_ray import RayDMatrix

# This will use distributed data loading, as four source files are specified
# Please note that you cannot schedule more than four actors in this case.
# `label_col` is a column in the Parquet files, used as the target label
ray_params = RayDMatrix([
    "hdfs:///tmp/part1.parquet",
    "hdfs:///tmp/part2.parquet",
    "hdfs:///tmp/part3.parquet",
    "hdfs:///tmp/part4.parquet",
], label="label_col")
```

Lastly, XGBoost-Ray supports **distributed dataframe** representations, such
as [Modin](https://modin.readthedocs.io/en/latest/) and
[Dask dataframes](https://docs.dask.org/en/latest/dataframe.html)
(used with [Dask on Ray](https://docs.ray.io/en/master/data/dask-on-ray.html)).
Here, XGBoost-Ray will check on which nodes the distributed partitions
are currently located, and will assign partitions to actors in order to
minimize cross-node data transfer. Please note that we also assume here
that partition sizes are uniform.

```python
from xgboost_ray import RayDMatrix

# This will try to allocate the existing Modin partitions
# to co-located Ray actors. If this is not possible, data will
# be transferred across nodes
ray_params = RayDMatrix(existing_modin_df)
```

### Data sources

```{eval-rst}
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
   * - `Petastorm <https://github.com/uber/petastorm>`__
     - Yes
     - Yes
   * - `Dask dataframe <https://docs.dask.org/en/latest/dataframe.html>`__
     - Yes
     - Yes
   * - `Modin dataframe <https://modin.readthedocs.io/en/latest/>`__
     - Yes
     - Yes

```

## Memory usage

XGBoost uses a compute-optimized datastructure, the `DMatrix`,
to hold training data. When converting a dataset to a `DMatrix`,
XGBoost creates intermediate copies and ends up
holding a complete copy of the full data. The data will be converted
into the local dataformat (on a 64 bit system these are 64 bit floats.)
Depending on the system and original dataset dtype, this matrix can
thus occupy more memory than the original dataset.

The **peak memory usage** for CPU-based training is at least
**3x** the dataset size (assuming dtype `float32` on a 64bit system)
plus about **400,000 KiB** for other resources,
like operating system requirements and storing of intermediate
results.

**Example**

- Machine type: AWS m5.xlarge (4 vCPUs, 16 GiB RAM)
- Usable RAM: ~15,350,000 KiB
- Dataset: 1,250,000 rows with 1024 features, dtype float32.
  Total size: 5,000,000 KiB
- XGBoost DMatrix size: ~10,000,000 KiB

This dataset will fit exactly on this node for training.

Note that the DMatrix size might be lower on a 32 bit system.

**GPUs**

Generally, the same memory requirements exist for GPU-based
training. Additionally, the GPU must have enough memory
to hold the dataset.

In the example above, the GPU must have at least
10,000,000 KiB (about 9.6 GiB) memory. However,
empirically we found that using a `DeviceQuantileDMatrix`
seems to show more peak GPU memory usage, possibly
for intermediate storage when loading data (about 10%).

**Best practices**

In order to reduce peak memory usage, consider the following
suggestions:

- Store data as `float32` or less. More precision is often
  not needed, and keeping data in a smaller format will
  help reduce peak memory usage for initial data loading.
- Pass the `dtype` when loading data from CSV. Otherwise,
  floating point values will be loaded as `np.float64`
  per default, increasing peak memory usage by 33%.

## Placement Strategies

XGBoost-Ray leverages Ray's Placement Group API (<https://docs.ray.io/en/master/placement-group.html>)
to implement placement strategies for better fault tolerance.

By default, a SPREAD strategy is used for training, which attempts to spread all of the training workers
across the nodes in a cluster on a best-effort basis. This improves fault tolerance since it minimizes the
number of worker failures when a node goes down, but comes at a cost of increased inter-node communication
To disable this strategy, set the `USE_SPREAD_STRATEGY` environment variable to 0. If disabled, no
particular placement strategy will be used.

Note that this strategy is used only when `elastic_training` is not used. If `elastic_training` is set to `True`,
no placement strategy is used.

When XGBoost-Ray is used with Ray Tune for hyperparameter tuning, a PACK strategy is used. This strategy
attempts to place all workers for each trial on the same node on a best-effort basis. This means that if a node
goes down, it will be less likely to impact multiple trials.

When placement strategies are used, XGBoost-Ray will wait for 100 seconds for the required resources
to become available, and will fail if the required resources cannot be reserved and the cluster cannot autoscale
to increase the number of resources. You can change the `PLACEMENT_GROUP_TIMEOUT_S` environment variable to modify
how long this timeout should be.

## More examples

For complete end to end examples, please have a look at
the [examples folder](https://github.com/ray-project/xgboost_ray/tree/master/examples/):

- [Simple sklearn breastcancer dataset example](https://github.com/ray-project/xgboost_ray/blob/master/xgboost_ray/examples/simple.py) (requires `sklearn`)
- [HIGGS classification example](https://github.com/ray-project/xgboost_ray/blob/master/xgboost_ray/examples/higgs.py)
  ([download dataset (2.6 GB)](https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz))
- [HIGGS classification example with Parquet](https://github.com/ray-project/xgboost_ray/blob/master/xgboost_ray/examples/higgs_parquet.py) (uses the same dataset)
- [Test data classification](https://github.com/ray-project/xgboost_ray/blob/master/xgboost_ray/examples/train_on_test_data.py) (uses a self-generated dataset)

## API reference

```{eval-rst}
.. autoclass:: xgboost_ray.RayParams
    :members:
```

```{eval-rst}
.. autoclass:: xgboost_ray.RayDMatrix
    :members:
```

```{eval-rst}
.. autofunction:: xgboost_ray.train
```

```{eval-rst}
.. autofunction:: xgboost_ray.predict
```

### scikit-learn API

```{eval-rst}
.. autoclass:: xgboost_ray.RayXGBClassifier
    :members:
```

```{eval-rst}
.. autoclass:: xgboost_ray.RayXGBRegressor
    :members:
```

```{eval-rst}
.. autoclass:: xgboost_ray.RayXGBRFClassifier
    :members:
```

```{eval-rst}
.. autoclass:: xgboost_ray.RayXGBRFRegressor
    :members:
```
