from typing import Type, Union, Optional, Callable

import numpy as np

from ray.rllib import SampleBatch
from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.pg.default_config import DEFAULT_CONFIG
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.agents.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED,
    WORKER_UPDATE_TIMER,
)
from ray.rllib.utils.typing import (
    TrainerConfigDict,
    PartialTrainerConfigDict,
    EnvType,
    ResultDict,
)
import ray.train as train
from ray.train import Trainer as train_trainer
from ray.tune.logger import Logger


class PGTrainer(Trainer):
    """Policy Gradient (PG) Trainer.

    Defines the distributed Trainer class for policy gradients.
    See `pg_[tf|torch]_policy.py` for the definition of the policy losses for
    TensorFlow and PyTorch.

    Detailed documentation:
    https://docs.ray.io/en/master/rllib-algorithms.html#pg

    Only overrides the default config- and policy selectors
    (`get_default_policy` and `get_default_config`). Utilizes
    the default `execution_plan()` of `Trainer`.
    """

    def __init__(
        self,
        config: Optional[PartialTrainerConfigDict] = None,
        env: Optional[Union[str, EnvType]] = None,
        logger_creator: Optional[Callable[[], Logger]] = None,
        remote_checkpoint_dir: Optional[str] = None,
        sync_function_tpl: Optional[str] = None,
    ):
        super(PGTrainer, self).__init__(
            config, env, logger_creator, remote_checkpoint_dir, sync_function_tpl
        )
        self._train_trainer = None
        if config["framework"] == "torch":
            num_gpus = self.config.get("num_gpus", 0)
            total_num_cpus_driver = self.config.get("num_cpus_for_driver", 1) or 1
            self._train_trainer = train_trainer(
                "torch",
                logdir="/tmp/rllib_tmp",
                num_workers=(num_gpus or 1),
            )
            self._train_trainer.start()

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def get_default_policy_class(self, config) -> Type[Policy]:
        return PGTorchPolicy if config.get("framework") == "torch" else PGTFPolicy

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        if self._train_trainer:
            return self._rllib_on_train_training_iteration_fn()
        else:
            return super(PGTrainer, self).training_iteration()

    def _rllib_on_train_training_iteration_fn(self) -> ResultDict:
        # Some shortcuts.
        batch_size = self.config["train_batch_size"]

        # Collects SampleBatches in parallel and synchronously
        # from the Trainer's RolloutWorkers until we hit the
        # configured `train_batch_size`.
        sample_batches = []
        num_env_steps = 0
        num_agent_steps = 0
        while (not self._by_agent_steps and num_env_steps < batch_size) or (
            self._by_agent_steps and num_agent_steps < batch_size
        ):
            new_sample_batches = synchronous_parallel_sample(self.workers)
            sample_batches.extend(new_sample_batches)
            num_env_steps += sum(len(s) for s in new_sample_batches)
            num_agent_steps += sum(
                len(s) if isinstance(s, SampleBatch) else s.agent_steps()
                for s in new_sample_batches
            )
        self._counters[NUM_ENV_STEPS_SAMPLED] += num_env_steps
        self._counters[NUM_AGENT_STEPS_SAMPLED] += num_agent_steps

        # Combine all batches at once
        train_batch = SampleBatch.concat_samples(sample_batches)
        train_results = {}

        if self.workers.remote_workers():
            with self._timers[WORKER_UPDATE_TIMER]:
                self.workers.sync_weights()

        return train_results

    # @staticmethod
    # def _do_one_step(config):
    #     rank = train.world_rank()
    #     sample_buffer = config["sample_buffer"]
    #     policy_class: Type[Policy] = config["policy"]
    #     policy_weights: Type[np.ndarray] = config["policy_weights"]
    #     training_iterations = config["training_iterations"]
    #     model = policy_class()
    #     for i in range(training_iterations):
    #         samples = sample_buffer.remote.get(rank)
    #         # preprocess samples (can we stick this in some function if it isn't
    #         # alreday in a preprocess function?)
    #         model.learn_on_batch(samples)


# class PGTrain:
#     def __init__(self, config):
#         self.worker =
#
#     def train_one_step(self, samples):
#         infos = self.worker.learn_on_batch(samples)
#
