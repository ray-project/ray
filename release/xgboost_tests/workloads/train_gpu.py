"""Training on a GPU cluster.

This will train a small dataset on a distributed GPU cluster.

Test owner: krfricke

Acceptance criteria: Should run through and report final results.

Notes: The test will report output such as this:
```
[05:14:49] WARNING: ../src/gbm/gbtree.cc:350: Loading from a raw memory buffer
on CPU only machine.  Changing tree_method to hist.
[05:14:49] WARNING: ../src/learner.cc:222: No visible GPU is found, setting
`gpu_id` to -1
```

This is _not_ an error. This is due to the checkpoints being loaded on the
XGBoost driver, and since the driver lives on the head node (which has no
GPU), XGBoost warns that it can't use the GPU. Training still happened using
the GPUs.
"""
import ray
from xgboost_ray import RayParams

from _train import train_ray

if __name__ == "__main__":
    ray.init(address="auto")

    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=4,
        cpus_per_actor=4,
        gpus_per_actor=1)

    train_ray(
        path="/data/classification.parquet",
        num_workers=4,
        num_boost_rounds=100,
        num_files=25,
        regression=False,
        use_gpu=True,
        ray_params=ray_params,
        xgboost_params=None,
    )
