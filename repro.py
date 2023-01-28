import numpy as np

import ray
from ray.air.util.tensor_extensions.pandas import _create_possibly_ragged_ndarray

ds = ray.data.from_items([{"x": i} for i in range(1, 4)])


def fn(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    batch["x"] = np.array(
        [np.zeros((x, 1), dtype=float) for x in batch["x"]], dtype=object
    )
    return batch


ds.map_batches(fn, batch_format="numpy", batch_size=2)
