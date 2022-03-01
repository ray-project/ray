import copy
from typing import Type, Union, Optional, Callable

import numpy as np

import ray
from ray.rllib import SampleBatch, RolloutWorker
from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.pg.default_config import DEFAULT_CONFIG
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.agents.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.debug import update_global_seed_if_necessary
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED,
    WORKER_UPDATE_TIMER,
)
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.rllib.utils.typing import (
    TrainerConfigDict,
    PartialTrainerConfigDict,
    EnvType,
    ResultDict,
)
import ray.train as train
from ray.train import Trainer as train_trainer

import rayportal


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

    def setup(self, config: PartialTrainerConfigDict):
        super(PGTrainer, self).setup(config)
        self._train_trainer = None
        if self.config["framework"] == "torch":
            config_copy = copy.deepcopy(self.config)
            num_gpus = config_copy.get("num_gpus", 0)
            self._num_train_workers = config_copy["num_training_workers"]
            num_cpus_per_training_worker = config_copy["num_cpus_per_training_worker"]
            num_gpus_per_training_worker = config_copy["num_gpus_per_training_worker"]
            self._train_trainer = train_trainer(
                "torch",
                logdir="/tmp/rllib_tmp",
                num_workers=self._num_train_workers,
            )
            config_copy["default_policy_cls"] = self.get_default_policy_class(
                self.config
            )
            config_copy["spaces_inferred_from_sampling_workers"] = self.workers.spaces

            self._training_workers = self._train_trainer.to_worker_group(
                train_cls=PGTrainWorkerTorch, config=config_copy
            )
            for idx, w in enumerate(self._training_workers):
                w.start.remote(0, self._num_train_workers)

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
        train_batch_ref = ray.put(train_batch)
        train_batch_shard_idx = []
        start = 0
        batch_size = len(train_batch) // self._num_train_workers
        for i in range(self._num_train_workers):
            train_batch_shard_idx.append(
                (start, min(start + batch_size, len(train_batch)))
            )
            start = start + batch_size

        training_results = [
            w.train_one_step.remote(train_batch_ref, idx)
            for idx, w in zip(train_batch_shard_idx, self._training_workers)
        ]
        training_results = ray.get(training_results)
        learner_info_builder = LearnerInfoBuilder(num_devices=self._training_workers)
        for result in training_results:
            learner_info_builder.add_learn_on_batch_results_multi_agent(result)

        # this handles getting the weights from the training workers (weights are
        # automatically synchronized via torch ddp)
        weights_of_trained_policy = ray.get(
            self._training_workers[0].get_weights.remote()
        )
        # set the weights from the first training worker to the local worker. The
        # weights of the first training worker are the same as the weights of all of
        # the other training workers since the gradients on all workers are synced
        # via torch ddp.
        self.workers.local_worker().set_weights(weights_of_trained_policy)
        # broadcast the weights of the local worker to the sampling workers
        if self.workers.remote_workers():
            with self._timers[WORKER_UPDATE_TIMER]:
                self.workers.sync_weights()

        return learner_info_builder.finalize()


class PGTrainWorkerTorch:
    def __init__(self, config):
        self.config = copy.deepcopy(config)

    def start(self, rank, world_size):
        if self.config["seed"]:
            update_global_seed_if_necessary("torch", self.config["seed"] + rank)
        self.config["training_worker_rank"] = rank
        self.config["number_of_training_workers"] = world_size
        if not self.config.get("observation_space") or not self.config.get(
            "action_space"
        ):
            spaces = self.config["spaces_inferred_from_sampling_workers"]
        else:
            spaces = None

        self._local_worker = Trainer.make_worker(
            cls=RolloutWorker,
            env_creator=lambda _: None,
            policy_cls=self.config["default_policy_cls"],
            worker_index=0,
            num_workers=self.config["number_of_training_workers"],
            config=self.config,
            spaces=spaces,
            validate_env=None,
        )
        for pid in self._local_worker.policies_to_train:
            self._local_worker.policy_map[pid].model = train.torch.prepare_model(
                self._local_worker.policy_map["default_policy"].model
            )

    def train_one_step(self, train_batch, idx):
        # if self.config["training_worker_rank"] == 0:
        #     import rayportal; rayportal.set_trace()
        train_batch = train_batch[idx[0]: idx[1]]

        infos = self._local_worker.learn_on_batch(train_batch)
        return infos

    def get_weights(self):
        return self._local_worker.get_weights()
