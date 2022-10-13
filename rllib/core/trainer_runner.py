import numpy as np

import ray

from ray.train._internal.backend_executor import BackendExecutor
from ray.train.torch import TorchConfig
from ray.train.tensorflow import TensorflowConfig
from ray.air.config import ScalingConfig


class TrainerRunner:
    def __init__(self, trainer_class, trainer_config, framework="torch"):
        self._trainer_config = trainer_config
        resources = self._compute_necessary_resources()
        scaling_config = ScalingConfig(
            num_workers=resources["num_workers"],
            use_gpu=resources["use_gpu"],
        )
        # the only part of this class that is framework agnostic:
        if framework == "torch":
            backend_config = TorchConfig()
        elif framework == "tf":
            backend_config = TensorflowConfig()
        else:
            raise ValueError("framework must be either torch or tf")

        self.backend_executor = BackendExecutor(
            backend_config=backend_config,
            num_workers=scaling_config.num_workers,
            num_cpus_per_worker=scaling_config.num_cpus_per_worker,
            num_gpus_per_worker=scaling_config.num_gpus_per_worker,
            max_retries=0,
        )

        _scaling_config = {"world_size": resources["num_workers"]}
        trainer_config["_scaling_config"] = _scaling_config
        self.backend_executor.start(
            train_cls=trainer_class, train_cls_args=(trainer_config,)
        )
        self.workers = [w.actor for w in self.backend_executor.worker_group.workers]

        ray.get([w._init_model.remote() for w in self.workers])

    def _compute_necessary_resources(self):
        num_gpus = self._trainer_config.get("num_gpus", 0)
        num_workers = self._trainer_config.get("num_training_workers", 0)
        if num_workers and num_gpus:
            assert num_workers == num_gpus, (
                "If num_training_workers and "
                "num_gpus are specified it must be equal to num_gpus"
            )

        elif num_gpus and not num_workers:
            num_workers = num_gpus

        elif not num_gpus and not num_workers:
            num_workers = 1

        return {"num_workers": num_workers, "use_gpu": bool(num_gpus)}

    def update(self, batch):
        global_size = len(self.workers)
        batch_size = np.ceil(len(batch) / global_size)
        refs = []
        for i, worker in enumerate(self.workers):
            start = batch_size * i
            end = min(start + batch_size, len(batch))
            new_batch = batch[int(start) : int(end)]
            refs.append(worker.update.remote(new_batch))
        return ray.get(refs)
