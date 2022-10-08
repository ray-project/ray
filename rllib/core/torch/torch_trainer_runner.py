import numpy as np

import ray
from ray.train import Trainer as train_trainer


class TorchTrainerRunner:

    def __init__(self, trainer_class, trainer_config):
        self._trainer_config = trainer_config
        resources = self._compute_necessary_resources()
        _train_trainer = train_trainer(
            "torch",
            logdir="/tmp/rllib_tmp",
            num_workers=resources["num_workers"],
            resources_per_worker={
                "CPU": 1,
                "GPU": bool(resources["use_gpu"]),
            },
            use_gpu=bool(resources["use_gpu"]),
        )

        _scaling_config = {"world_size": resources["num_workers"]}
        trainer_config["_scaling_config"] = _scaling_config
        self._training_workers = _train_trainer.to_worker_group(
            train_cls=trainer_class, config=trainer_config
        )
        self.num_training_workers = len([w for w in self._training_workers])
        ray.get([w._init_model.remote()for w in self._training_workers])

    def _compute_necessary_resources(self):
        num_gpus = self._trainer_config.get("num_gpus", 0)
        num_workers = self._trainer_config.get("num_training_workers", 0)
        if num_workers and num_gpus:
            assert num_workers == num_gpus, ("If num_training_workers and "
                "num_gpus are specified it must be equal to num_gpus")

        if num_gpus and not num_workers:
            num_workers = num_gpus

        return {"num_workers": num_workers, "use_gpu": num_gpus}

    def update(self, batch):
        global_size = self.num_training_workers
        batch_size = np.ceil(len(batch) / global_size)
        for i, worker in enumerate(self._training_workers):
            start = batch_size * i
            end = min(start + batch_size, len(batch))
            new_batch = batch[int(start) : int(end)]
            worker.update.remote(new_batch)
