import numpy as np

import ray


def compute_intesive_function(batch):
    # Simulate compute-intensive work with matrix operations
    data = batch["id"] if isinstance(batch, dict) else batch

    # Create random matrices and perform multiplications
    matrix_a = np.random.rand(100, 100)
    matrix_b = np.random.rand(100, 100)

    for _ in range(50):
        result = np.dot(matrix_a, matrix_b)
        matrix_a = result / np.max(result)  # Normalize to prevent overflow

    # Return the original data squared
    return {"id": data * data} if isinstance(batch, dict) else data * data


ray.data.DataContext.get_current().enable_rich_progress_bars = True
ray.data.DataContext.get_current().use_ray_tqdm = False
ds = ray.data.range(1000000)
ds = ds.map_batches(compute_intesive_function, batch_size=25600)
ds = ds.zip(ds)
ds = ds.map_batches(compute_intesive_function, batch_size=25600)
ds.materialize()
