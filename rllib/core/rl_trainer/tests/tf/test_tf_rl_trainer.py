import gym
import numpy as np

import ray

from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.offline import IOContext
from ray.rllib.offline.dataset_reader import (
    DatasetReader,
    get_dataset_and_shards,
)

from ray.rllib.core.rl_trainer.tf.tf_rl_trainer import TfRLTrainer
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_optimizer import BCTFOptimizer
from ray.air.config import ScalingConfig
from ray.train._internal.backend_executor import BackendExecutor
from ray.train.torch import TorchConfig
from ray.train.tensorflow import TensorflowConfig


class TrainerRunner:
    """Coordinator of RLTrainer workers.
    Public API:
        .update()
        .get_state() -> returns the state of the model on worker 0 which should
                        be in sync with the other workers
        .set_state() -> sets the state of the model on all workers
        .apply(fn, *args, **kwargs) -> apply a function to all workers while
                                       having access to the attributes
        >>> trainer_runner.apply(lambda w: w.get_weights())
    """

    def __init__(self, trainer_class, trainer_config, compute_config, framework="tf"):
        """ """
        self._trainer_config = trainer_config
        self._compute_config = compute_config

        # TODO: remove the _compute_necessary_resources and just use
        # trainer_config["use_gpu"] and trainer_config["num_workers"]
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
            max_retries=0,  # TODO: make this configurable in trainer_config
            # with default 0
        )

        # TODO: let's not pass this into the config which will cause
        # information leakage into the SARLTrainer about other workers.
        scaling_config = {"world_size": resources["num_workers"]}
        trainer_config["scaling_config"] = scaling_config
        trainer_config["distributed"] = True
        self.backend_executor.start(
            train_cls=trainer_class, train_cls_kwargs=trainer_config
        )
        self.workers = [w.actor for w in self.backend_executor.worker_group.workers]

        ray.get([w.make_distributed.remote() for w in self.workers])

    def _compute_necessary_resources(self):
        num_gpus = self._compute_config.get("num_gpus", 0)
        num_workers = self._compute_config.get("num_training_workers", 0)
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
        """TODO: account for **kwargs
        Example in DQN:
            >>> trainer_runner.update(batch) # updates the gradient
            >>> trainer_runner.update(update_target=True) # should soft-update
                the target network
        """
        refs = []
        if batch is None:
            for worker in self.workers:
                refs.append(worker.update.remote(**kwargs))
        else:
            global_size = len(self.workers)
            batch_size = np.ceil(len(batch) / global_size)
            for i, worker in enumerate(self.workers):
                start = batch_size * i
                end = min(start + batch_size, len(batch))
                new_batch = batch[int(start) : int(end)]
                refs.append(worker.update.remote(new_batch))

        return ray.get(refs)


if __name__ == "__main__":
    ray.init()
    env = gym.make("CartPole-v0")
    trainer_class = TfRLTrainer
    trainer_cfg = dict(
        module_class=DiscreteBCTFModule,
        module_config={
            "observation_space": env.observation_space,
            "action_space": env.action_space,
            "model_config": {"hidden_dim": 32}
        },
        optimizer_class=BCTFOptimizer,
        optimizer_config={}
    )
    runner = TrainerRunner(trainer_class, trainer_cfg, compute_config=dict(num_gpus=0))
    env = gym.make("CartPole-v1")

    # path = "s3://air-example-data/rllib/cartpole/large.json"
    path = "tests/data/cartpole/large.json"
    input_config = {"format": "json", "paths": path}
    dataset, _ = get_dataset_and_shards(
        AlgorithmConfig().offline_data(input_="dataset", input_config=input_config)
    )
    batch_size = 500
    ioctx = IOContext(
        config=(
            AlgorithmConfig()
            .training(train_batch_size=batch_size)
            .offline_data(actions_in_input_normalized=True)
        ),
        worker_index=0,
    )
    reader = DatasetReader(dataset, ioctx)
    num_epochs = 100
    total_timesteps_of_training = 1000000
    inter_steps = total_timesteps_of_training // (num_epochs * batch_size)
    for _ in range(num_epochs):
        for _ in range(inter_steps):
            batch = reader.next()
            runner.update(batch)

    # test 1: check distributed training ok
    # construct TrainerRunner()
    # construct dataset
    # call TrainerRunner.update with batch from dataset
    # check if gradients are same across workers
    # check if weights are same across workers

    # test 2: check add remove module / optimizer ok
    # check if memory usage all right after adding / removing module
