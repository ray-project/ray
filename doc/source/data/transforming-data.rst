.. _transforming_data:

=================
Transforming Data
=================

Transformations let you process and modify your dataset. You can compose transformations
to express a chain of computations.

.. note::
    Transformations are lazy by default. They aren't executed until you trigger consumption of the data by :ref:`iterating over the Dataset <iterating-over-data>`, :ref:`saving the Dataset <saving-data>`, or :ref:`inspecting properties of the Dataset <inspecting-data>`.

This guide shows you how to:

* :ref:`Transform rows <transforming_rows>`
* :ref:`Transform batches <transforming_batches>`
* :ref:`Order rows <ordering_of_rows>`
* :ref:`Perform stateful transformations <stateful_transforms>`
* :ref:`Perform Aggregations <aggregations>`
* :ref:`Transform groups <transforming_groupby>`

.. _transforming_rows:

Transforming rows
=================

.. tip::

    If your transformation is vectorized, call :meth:`~ray.data.Dataset.map_batches` for
    better performance. To learn more, see :ref:`Transforming batches <transforming_batches>`.

Transforming rows with map
~~~~~~~~~~~~~~~~~~~~~~~~~~

If your transformation returns exactly one row for each input row, call
:meth:`~ray.data.Dataset.map`.

.. testcode::

    import os
    from typing import Any, Dict
    import ray

    def parse_filename(row: Dict[str, Any]) -> Dict[str, Any]:
        row["filename"] = os.path.basename(row["path"])
        return row

    ds = (
        ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple", include_paths=True)
        .map(parse_filename)
    )

The user defined function passed to :meth:`~ray.data.Dataset.map` should be of type
`Callable[[Dict[str, Any]], Dict[str, Any]]`. In other words, your function should
input and output a dictionary with keys of strings and values of any type. For example:

.. testcode::

    from typing import Any, Dict

    def fn(row: Dict[str, Any]) -> Dict[str, Any]:
        # access row data
        value = row["col1"]

        # add data to row
        row["col2"] = ...

        # return row
        return row

Transforming rows with flat map
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your transformation returns multiple rows for each input row, call
:meth:`~ray.data.Dataset.flat_map`.

.. testcode::

    from typing import Any, Dict, List
    import ray

    def duplicate_row(row: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [row] * 2

    print(
        ray.data.range(3)
        .flat_map(duplicate_row)
        .take_all()
    )

.. testoutput::

    [{'id': 0}, {'id': 0}, {'id': 1}, {'id': 1}, {'id': 2}, {'id': 2}]

The user defined function passed to :meth:`~ray.data.Dataset.flat_map` should be of type
`Callable[[Dict[str, Any]], List[Dict[str, Any]]]`. In other words your function should
input a dictionary with keys of strings and values of any type and output a list of
dictionaries that have the same type as the input, for example:

.. testcode::

    from typing import Any, Dict, List

    def fn(row: Dict[str, Any]) -> List[Dict[str, Any]]:
        # access row data
        value = row["col1"]

        # add data to row
        row["col2"] = ...

        # construct output list
        output = [row, row]

        # return list of output rows
        return output

.. _transforming_batches:

Transforming batches
====================

If your transformation can be vectorized using NumPy, PyArrow or Pandas operations, transforming
batches is considerably more performant than transforming individual rows.

.. testcode::

    from typing import Dict
    import numpy as np
    import ray

    def increase_brightness(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["image"] = np.clip(batch["image"] + 4, 0, 255)
        return batch

    ds = (
        ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
        .map_batches(increase_brightness)
    )

.. _configure_batch_format:

Configuring batch format
~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data represents batches as dicts of NumPy ndarrays, pandas DataFrames or Arrow Tables. By
default, Ray Data represents batches as dicts of NumPy ndarrays. To configure the batch type,
specify ``batch_format`` in :meth:`~ray.data.Dataset.map_batches`. You can return either
format from your function, but ``batch_format`` should match the input of your function.

When applying transformations to batches of rows, Ray Data could represent these batches as either NumPy's ``ndarrays``,
Pandas ``DataFrame`` or PyArrow ``Table``.

When using
    * ``batch_format=numpy``, the input to the function will be a dictionary where keys correspond to column names and values to column values represented as ``ndarrays``.
    * ``batch_format=pyarrow``, the input to the function will be a Pyarrow ``Table``.
    * ``batch_format=pandas``, the input to the function will be a Pandas ``DataFrame``.

.. tab-set::

    .. tab-item:: NumPy

        .. testcode::

            from typing import Dict
            import numpy as np
            import ray

            def increase_brightness(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                batch["image"] = np.clip(batch["image"] + 4, 0, 255)
                return batch

            ds = (
                ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
                .map_batches(increase_brightness, batch_format="numpy")
            )

    .. tab-item:: pandas

        .. testcode::

            import pandas as pd
            import ray

            def drop_nas(batch: pd.DataFrame) -> pd.DataFrame:
                return batch.dropna()

            ds = (
                ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
                .map_batches(drop_nas, batch_format="pandas")
            )
    .. tab-item:: pyarrow

        .. testcode::

            import pyarrow as pa
            import pyarrow.compute as pc
            import ray

            def drop_nas(batch: pa.Table) -> pa.Table:
                return pc.drop_null(batch)

            ds = (
                ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
                .map_batches(drop_nas, batch_format="pyarrow")
            )

The user defined function can also be a Python generator that yields batches, so the function can also
be of type ``Callable[DataBatch, Iterator[[DataBatch]]``, where ``DataBatch = Union[pd.DataFrame, Dict[str, np.ndarray]]``.
In this case, your function would look like:

.. testcode::

    from typing import Dict, Iterator
    import numpy as np

    def fn(batch: Dict[str, np.ndarray]) -> Iterator[Dict[str, np.ndarray]]:
        # yield the same batch multiple times
        for _ in range(10):
            yield batch
            
Choosing the right batch format
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When choosing appropriate batch format for your ``map_batches`` primary consideration is a trade-off of convenience vs performance:

1. Batches are a sliding window into the underlying block: the UDF is invoked with a subset of rows of the underlying block that make up the current batch of specified ``batch_size``. Specifying ``batch_size=None`` makes batch include all rows of the block in a single batch.

2. Depending on the batch format, such view can either be a *zero-copy* (when batch format matches
block type of either ``pandas`` or ``pyarrow``) or copying one (when the batch format differs from the block type).

For example, if the underlying block type is Arrow, specifying ``batch_format="numpy"`` or ``batch_format="pandas"``  might invoke a copy on the underlying data when converting it from the underlying block type.

Ray Data also strives to minimize the amount of data conversions: for example, if your ``map_batches`` operation returns Pandas batches, then these batches are combined into blocks *without* conversion and propagated further as Pandas blocks.

Most Ray Data datasources produce Arrow blocks, so using batch format ``pyarrow`` can avoid unnecessary data conversions.

If you'd like to use a more ergonomic API for transformations but avoid performance overheads, you can consider using ``polars`` inside your ``map_batches`` operation with ``batch_format="pyarrow"`` as follows:

.. testcode::

    import pyarrow as pa

    def udf(table: pa.Table):
        import polars as pl
        df = polars.from_pyarrow(table)
        df.summary()
        return df.to_arrow()

    ds.map_batches(udf, batch_format="pyarrow")



Configuring batch size
~~~~~~~~~~~~~~~~~~~~~~

Increasing ``batch_size`` improves the performance of vectorized transformations as well
as performance of model inference. However, if your batch size is too large, your
program might run into out-of-memory (OOM) errors.

If you encounter an OOM errors, try decreasing your ``batch_size``.

.. _ordering_of_rows:

Ordering of rows
================

When transforming data, the order of :ref:`blocks <data_key_concepts>` isn't preserved by default.

If the order of blocks needs to be preserved/deterministic,
you can use :meth:`~ray.data.Dataset.sort` method, or set :attr:`ray.data.ExecutionOptions.preserve_order` to `True`.
Note that setting this flag may negatively impact performance on larger cluster setups where stragglers are more likely.

.. testcode::

   import ray
   
   ctx = ray.data.DataContext().get_current()
   
   # By default, this is set to False.
   ctx.execution_options.preserve_order = True

.. _stateful_transforms:

Stateful Transforms
===================

If your transform requires expensive setup such as downloading
model weights, use a callable Python class instead of a function to make the transform stateful. When a Python class
is used, the ``__init__`` method is called to perform setup exactly once on each worker.
In contrast, functions are stateless, so any setup must be performed for each data item.

Internally, Ray Data uses tasks to execute functions, and uses actors to execute classes.
To learn more about tasks and actors, read the
:ref:`Ray Core Key Concepts <core-key-concepts>`.

To transform data with a Python class, complete these steps:

1. Implement a class. Perform setup in ``__init__`` and transform data in ``__call__``.

2. Call :meth:`~ray.data.Dataset.map_batches`, :meth:`~ray.data.Dataset.map`, or
   :meth:`~ray.data.Dataset.flat_map`. Pass a compute strategy with the ``compute``
   argument to control how many workers Ray uses. Each worker transforms a partition
   of data in parallel. Use ``ray.data.TaskPoolStrategy(size=n)`` to cap the number of
   concurrent tasks, or ``ray.data.ActorPoolStrategy(...)`` to run callable classes on
   a fixed or autoscaling actor pool.

.. tab-set::

    .. tab-item:: CPU

        .. testcode::

            from typing import Dict
            import numpy as np
            import torch
            import ray

            class TorchPredictor:

                def __init__(self):
                    self.model = torch.nn.Identity()
                    self.model.eval()

                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                    inputs = torch.as_tensor(batch["data"], dtype=torch.float32)
                    with torch.inference_mode():
                        batch["output"] = self.model(inputs).detach().numpy()
                    return batch

            ds = (
                ray.data.from_numpy(np.ones((32, 100)))
                .map_batches(
                    TorchPredictor,
                    compute=ray.data.ActorPoolStrategy(size=2),
                )
            )

        .. testcode::
            :hide:

            ds.materialize()

    .. tab-item:: GPU

        .. testcode::

            from typing import Dict
            import numpy as np
            import torch
            import ray

            class TorchPredictor:

                def __init__(self):
                    self.model = torch.nn.Identity().cuda()
                    self.model.eval()

                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                    inputs = torch.as_tensor(batch["data"], dtype=torch.float32).cuda()
                    with torch.inference_mode():
                        batch["output"] = self.model(inputs).detach().cpu().numpy()
                    return batch

            ds = (
                ray.data.from_numpy(np.ones((32, 100)))
                .map_batches(
                    TorchPredictor,
                    # Two workers with one GPU each
                    compute=ray.data.ActorPoolStrategy(size=2),
                    # Batch size is required if you're using GPUs.
                    batch_size=4,
                    num_gpus=1
                )
            )

        .. testcode::
            :hide:

            ds.materialize()

Avoiding out-of-memory errors
=============================

If your user defined function uses lots of memory, you might encounter out-of-memory 
errors. To avoid these errors, configure the ``memory`` parameter. It tells Ray how much 
memory your function uses, and prevents Ray from scheduling too many tasks on a node.

.. testcode::
    :hide:

    import ray
    
    ds = ray.data.range(1)

.. testcode::

    def uses_lots_of_memory(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        ...

    # Tell Ray that the function uses 1 GiB of memory
    ds.map_batches(uses_lots_of_memory, memory=1 * 1024 * 1024)


.. _transforming_groupby:

Group-by and transforming groups
================================

To transform groups, call :meth:`~ray.data.Dataset.groupby` to group rows based on provided ``key`` column values.
Then, call :meth:`~ray.data.grouped_data.GroupedData.map_groups` to execute a transformation on each group.

.. tab-set::

    .. tab-item:: NumPy

        .. testcode::

            from typing import Dict
            import numpy as np
            import ray

            items = [
                {"image": np.zeros((32, 32, 3)), "label": label}
                for _ in range(10) for label in range(100)
            ]

            def normalize_images(group: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                group["image"] = (group["image"] - group["image"].mean()) / group["image"].std()
                return group

            ds = (
                ray.data.from_items(items)
                .groupby("label")
                .map_groups(normalize_images)
            )

    .. tab-item:: pandas

        .. testcode::

            import pandas as pd
            import ray

            def normalize_features(group: pd.DataFrame) -> pd.DataFrame:
                target = group.drop("target")
                group = (group - group.min()) / group.std()
                group["target"] = target
                return group

            ds = (
                ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
                .groupby("target")
                .map_groups(normalize_features)
            )
