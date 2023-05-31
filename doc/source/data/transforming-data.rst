.. _transforming-data:

=================
Transforming data
=================

Transforming rows
=================

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

Transforming batches
====================

Transforming batches with tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
                    compute=ray.data.ActorPoolStrategy(size=2),
                    num_gpus=1
                )
            )

Configuring batch type
~~~~~~~~~~~~~~~~~~~~~~

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

.. testcode::

    import ray

    ds = (
        ray.data.read_images("example://image-datasets/simple")
        .random_shuffle()
    )

.. tip::

    :meth:`~ray.data.Dataset.random_shuffle` is slow. For better performance, try
    :ref:`iterating-over-batches-with-shuffling`.

Creating rows
=============

.. testcode::

    from typing import Any, Dict
    import ray

    def duplicate_row(row: Dict[str, Any]) -> List[[str, Any]]:
        return [{"item": row["item"]}] * 2

    print(
        ray.data.range(3)
        .flat_map(duplicate_row)
        .take_all()
    )

.. testoutput::

    [{"item": 0}, {"item": 0}, {"item": 1}, {"item": 1}, {"item": 2}, {"item": 2}]
