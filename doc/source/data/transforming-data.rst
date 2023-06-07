.. _transforming-data:

=================
Transforming data
=================

Transformations let you process and modify your dataset. You can compose transformations
to express a chain of computations.

This guide shows you how to:

* `Transform rows <#transforming-rows>`_
* `Transform batches <#transforming-batches>`_
* `Transform groups <#transforming-groups>`_
* `Shuffle rows <#shuffling-rows>`_

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
        ray.data.read_images("example://image-datasets/simple", include_paths=True)
        .map(parse_filename)
    )

.. tip::

    If your transformation is vectorized, call :meth:`~ray.data.Dataset.map_batches` for
    better performance. To learn more, see `Transforming batches with actors <transforming-batches-with-actors>`_.

Transforming rows with flat map
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your transformation returns multiple rows for each input row, call
:meth:`~ray.data.Dataset.flat_map`.

.. testcode::

    from typing import Any, Dict
    import ray

    def duplicate_row(row: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [row] * 2

    print(
        ray.data.range(3)
        .flat_map(duplicate_row)
        .take_all()
    )

.. testoutput::

    [{"item": 0}, {"item": 0}, {"item": 1}, {"item": 1}, {"item": 2}, {"item": 2}]

Transforming batches
====================

If your transformation is vectorized like most NumPy or pandas operations, transforming
batches is more performant than transforming rows.

Choosing between tasks and actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data transforms batches with either tasks or actors. Actors perform setup exactly
once. In contrast, tasks require setup every batch. So, if your transformation involves
expensive setup like downloading model weights, use actors. Otherwise, use tasks.

To learn more about tasks and actors, read the
:ref:`Ray Core Key Concepts <core-key-concepts>`.

Transforming batches with tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To transform batches with tasks, call :meth:`~ray.data.Dataset.map_batches`. Ray Data
uses tasks by default.

.. testcode::

    from typing import Dict
    import numpy as np
    import ray

    def increase_brightness(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["image"] = np.clip(batch["image"] + 4, 0, 255)
        return batch

    ds = (
        ray.data.read_images("example://image-datasets/simple")
        .map_batches(increase_brightness)
    )

Transforming batches with actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To transform batches with actors, complete these steps:

1. Implement a class. Perform setup in ``__init__`` and transform data in ``__call__``.

2. Create an :class:`~ray.data.ActorPoolStrategy` and configure the number of concurrent
   workers. Each worker transforms a partition of data.

3. Call :meth:`~ray.data.Dataset.map_batches` and pass your ``ActorPoolStrategy`` to ``compute``.

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

                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]
                    inputs = torch.as_tensor(batch["data"], dtype=torch.float32)
                    with torch.inference_mode():
                        batch["output"] = self.model(tensor).detach().numpy())
                    return batch

            ds = (
                ray.data.from_numpy(np.ones((32, 100))
                .map_batches(TorchPredictor, compute=ray.data.ActorPoolStrategy(size=2))
            )

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

                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]
                    inputs = torch.as_tensor(batch["data"], dtype=torch.float32).cuda()
                    with torch.inference_mode():
                        batch["output"] = self.model(tensor).detach().cpu().numpy())
                    return batch

            ds = (
                ray.data.from_numpy(np.ones((32, 100))
                .map_batches(
                    TorchPredictor,
                    # Two workers with one GPU each
                    compute=ray.data.ActorPoolStrategy(size=2),
                    num_gpus=1
                )
            )

Configuring batch type
~~~~~~~~~~~~~~~~~~~~~~

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
                ray.data.read_images("example://image-datasets/simple")
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

Transforming groups
===================

To transform groups, call :meth:`~ray.data.Dataset.groupby` to group rows. Then, call
:meth:`~ray.data.grouped_data.GroupedData.map_groups` to transform the groups.

.. tab-set::

    .. tab-item:: NumPy

        .. testcode::

            from typing import Dict
            import numpy as np
            import ray

            items = [
                {"image": np.zeros((32, 32, 3)), "label": i}
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

Shuffling rows
==============

To randomly shuffle all rows, call :meth:`~ray.data.Dataset.random_shuffle`.

.. testcode::

    import ray

    ds = (
        ray.data.read_images("example://image-datasets/simple")
        .random_shuffle()
    )

.. tip::

    :meth:`~ray.data.Dataset.random_shuffle` is slow. For better performance, try
    `Iterating over batches with shuffling <iterating-over-data#iterating-over-batches-with-shuffling>`_.
