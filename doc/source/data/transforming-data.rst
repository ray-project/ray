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
* :ref:`Stateful Transforms <stateful_transforms>`
* :ref:`Groupby and transform groups <transforming_groupby>`
* :ref:`Shuffle rows <shuffling_rows>`
* :ref:`Repartition data <repartitioning_data>`

.. _transforming_rows:

Transforming rows
=================

To transform rows, call :meth:`~ray.data.Dataset.map` or
:meth:`~ray.data.Dataset.flat_map`.

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

.. tip::

    If your transformation is vectorized, call :meth:`~ray.data.Dataset.map_batches` for
    better performance. To learn more, see `Transforming batches <#transforming-batches>`_.

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

.. _transforming_batches:

Transforming batches
====================

If your transformation is vectorized like most NumPy or pandas operations, transforming
batches is more performant than transforming rows.

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

Ray Data represents batches as dicts of NumPy ndarrays or pandas DataFrames. By
default, Ray Data represents batches as dicts of NumPy ndarrays.

To configure the batch type, specify ``batch_format`` in
:meth:`~ray.data.Dataset.map_batches`. You can return either format from your function.

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

Configuring batch size
~~~~~~~~~~~~~~~~~~~~~~

Increasing ``batch_size`` improves the performance of vectorized transformations like
NumPy functions and model inference. However, if your batch size is too large, your
program might run out of memory. If you encounter an out-of-memory error, decrease your
``batch_size``.

.. note::

    The default batch size depends on your resource type. If you're using CPUs,
    the default batch size is 4096. If you're using GPUs, you must specify an explicit
    batch size.

.. _stateful_transforms:

Stateful Transforms
==============================

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
   :meth:`~ray.data.Dataset.flat_map`. Pass the number of concurrent workers to use with the ``concurrency`` argument. Each worker transforms a partition of data in parallel.
   Fixing the number of concurrent workers gives the most predictable performance, but you can also pass a tuple of ``(min, max)`` to allow Ray Data to automatically
   scale the number of concurrent workers.

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
                .map_batches(TorchPredictor, concurrency=2)
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
                    concurrency=2,
                    # Batch size is required if you're using GPUs.
                    batch_size=4,
                    num_gpus=1
                )
            )

        .. testcode::
            :hide:

            ds.materialize()

.. _transforming_groupby:

Groupby and transforming groups
===============================

To transform groups, call :meth:`~ray.data.Dataset.groupby` to group rows. Then, call
:meth:`~ray.data.grouped_data.GroupedData.map_groups` to transform the groups.

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

.. _shuffling_rows:

Shuffling rows
==============

To randomly shuffle all rows, call :meth:`~ray.data.Dataset.random_shuffle`.

.. testcode::

    import ray

    ds = (
        ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
        .random_shuffle()
    )

.. tip::

    :meth:`~ray.data.Dataset.random_shuffle` is slow. For better performance, try
    :ref:`Iterating over batches with shuffling <iterating-over-batches-with-shuffling>`.

.. _repartitioning_data:

Repartitioning data
===================

A :class:`~ray.data.dataset.Dataset` operates on a sequence of distributed data
:term:`blocks <block>`. If you want to achieve more fine-grained parallelization,
increase the number of blocks by setting a higher ``parallelism`` at read time.

To change the number of blocks for an existing Dataset, call
:meth:`Dataset.repartition() <ray.data.Dataset.repartition>`.

.. testcode::

    import ray

    ds = ray.data.range(10000, parallelism=1000)

    # Repartition the data into 100 blocks. Since shuffle=False, Ray Data will minimize
    # data movement during this operation by merging adjacent blocks.
    ds = ds.repartition(100, shuffle=False).materialize()

    # Repartition the data into 200 blocks, and force a full data shuffle.
    # This operation will be more expensive
    ds = ds.repartition(200, shuffle=True).materialize()
