import numpy as np

import ray

from ray.train._internal.backend_executor import BackendExecutor
from ray.train.torch import TorchConfig
from ray.train.tensorflow import TensorflowConfig
from ray.air.config import ScalingConfig


class TrainerRunner:
    """This gets instantiated in Algorithm and then is used with calls to .update() to actually perform 1 gradient step

    Public API:
        .update()
        .get_state() -> returns the state of the model on worker 0 which should be in sync with the other workers
        .set_state() -> sets the state of the model on all workers
        .apply(fn, *args, **kwargs) -> apply a function to all workers while having access to the attributes
            >>> trainer_runner.apply(lambda rl_trainer_worker: rl_trainer_worker.get_weights())
    """

    def __init__(self, trainer_class, trainer_config, framework="torch"):
        """TODO: frameowrk seems redundant if you know that the trainer_class is a subclass of TorchTrainer or TFTrainer

        # TODO create a trainer_config clas that has all the necessary attributes
        """
        self._trainer_config = trainer_config

        # TODO: remove the _compute_necessary_resources and just use trainer_config["use_gpu"] and trainer_config["num_workers"]
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
            max_retries=0,  # TODO: make this configurable in trainer_config with default 0
        )

        # TODO: let's not pass this into the config which will cause information leakage into the SARLTrainer about other workers.
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

    def update(self, batch=None, **kwargs):
        """TODO: acount for **kwargs

        Example in DQN:
            >>> trainer_runner.update(batch) # updates the gradient
            >>> trainer_runner.update(update_target=True) # should soft-update the target network

        """
        if batch is None:
            for worker in self.workers:
                refs.append(worker.update.remote(**kwargs))
        else:

            global_size = len(self.workers)
            batch_size = np.ceil(len(batch) / global_size)
            refs = []
            for i, worker in enumerate(self.workers):
                start = batch_size * i
                end = min(start + batch_size, len(batch))
                new_batch = batch[int(start) : int(end)]
                refs.append(worker.update.remote(new_batch))

        return ray.get(refs)
