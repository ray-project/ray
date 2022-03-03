import contextlib
import copy
from typing import Type, Union, List, Dict

import numpy as np
import torch
from torch.nn.parallel import DistributedDataParallel

import ray
from ray.rllib import SampleBatch, RolloutWorker
from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.pg.default_config import DEFAULT_CONFIG
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.agents.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import MultiAgentBatch
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
    ResultDict,
    TensorType,
)
from ray.train import Trainer as train_trainer

from ray.rllib.utils.test_utils import check as check_weights


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
            self._num_train_workers = config_copy["num_training_workers"]
            num_cpus_per_training_worker = config_copy["num_cpus_per_training_worker"]
            num_gpus_per_training_worker = config_copy["num_gpus_per_training_worker"]

            self._train_trainer = train_trainer(
                "torch",
                logdir="/tmp/rllib_tmp",
                num_workers=self._num_train_workers,
                resources_per_worker={
                    "CPU": num_cpus_per_training_worker,
                    "GPU": num_gpus_per_training_worker,
                },
            )
            config_copy["default_policy_cls"] = self.get_default_policy_class(
                self.config
            )
            config_copy["spaces_inferred_from_sampling_workers"] = self.workers.spaces

            self._training_workers = self._train_trainer.to_worker_group(
                train_cls=PGTrainWorkerTorch, config=config_copy
            )
            self._training_workers_started = False

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
        if not self._training_workers_started:
            for idx, w in enumerate(self._training_workers):
                w.start.remote(0, self._num_train_workers)
            self._training_workers_started = True
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
        train_batch_shard_idx = self.get_shard_idxs(train_batch)
        training_results = [
            w.train_one_step.remote(train_batch_ref, idx)
            for idx, w in zip(train_batch_shard_idx, self._training_workers)
        ]
        training_results = ray.get(training_results)
        learner_info_builder = LearnerInfoBuilder(num_devices=self._training_workers)
        for result in training_results:
            learner_info_builder.add_learn_on_batch_results_multi_agent(result)

        self._check_all_weights_same()
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

    def _check_all_weights_same(self):
        weights_all_trained_policy = ray.get(
            [w.get_weights.remote() for w in self._training_workers]
        )
        for i in range(len(weights_all_trained_policy)):
            for j in range(i, len(weights_all_trained_policy)):
                check_weights(
                    weights_all_trained_policy[i],
                    weights_all_trained_policy[j],
                    atol=5e-2,
                )

    def get_shard_idxs(self, train_batch: SampleBatch):
        def get_shard_idxs_helper(_train_batch: SampleBatch):
            train_batch_shard_idx = []
            start = 0
            batch_size = len(_train_batch) // self._num_train_workers
            for i in range(self._num_train_workers):
                train_batch_shard_idx.append(
                    (start, min(start + batch_size, len(_train_batch)))
                )
                start = start + batch_size
            return train_batch_shard_idx

        if isinstance(train_batch, SampleBatch):
            return get_shard_idxs_helper(train_batch)
        elif isinstance(train_batch, MultiAgentBatch):
            idxs = {}
            for name, batch in train_batch.policy_batches.items():
                idxs[name] = get_shard_idxs_helper(batch)
            ret = [{} for _ in range(self._num_train_workers)]
            for name, idx in idxs.items():
                for i, v in enumerate(idx):
                    ret[i][name] = v
            return ret


class PGTrainWorkerTorch:
    def __init__(self, config):
        self.config = copy.deepcopy(config)

    def start(self, rank, world_size):
        if self.config["seed"]:
            update_global_seed_if_necessary("torch", self.config["seed"])
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
        if not (world_size > 1):
            return
        for pid in self._local_worker.policies_to_train:
            model = self._local_worker.policy_map[pid].model
            if isinstance(model, DDPTorchModelV2Wrapper) or not isinstance(
                model, TorchModelV2
            ):
                continue
            if torch.cuda.is_available():
                model = DDPTorchModelV2Wrapper(
                    model, device_ids=[rank], output_device=rank
                )
            else:
                model = DDPTorchModelV2Wrapper(model)

            self._local_worker.policy_map[pid].model = model

    def train_one_step(self, train_batch, idx):
        if isinstance(train_batch, SampleBatch):
            train_batch = train_batch[idx[0] : idx[1]]
        elif isinstance(train_batch, MultiAgentBatch):
            policy_batch = {}
            for policy, _idx in idx.items():
                policy_batch[policy] = train_batch.policy_batches[policy][
                    _idx[0] : _idx[1]
                ]
            train_batch = MultiAgentBatch(policy_batch, train_batch.count)

        infos = self._local_worker.learn_on_batch(train_batch)
        return infos

    def get_weights(self):
        return self._local_worker.get_weights()


class DDPTorchModelV2Wrapper(TorchModelV2):
    def __init__(
        self,
        model,
        *args,
        **kwargs,
    ):
        self.ddp_model = DistributedDataParallel(model, *args, **kwargs)

    @override(TorchModelV2)
    def get_initial_state(self) -> List[np.ndarray]:
        return self.ddp_model.module.get_initial_state()

    @override(TorchModelV2)
    def value_function(self) -> TensorType:
        return self.ddp_model.module.value_function()

    @override(TorchModelV2)
    def custom_loss(
        self, policy_loss: TensorType, loss_inputs: Dict[str, TensorType]
    ) -> Union[List[TensorType], TensorType]:
        return self.ddp_model.module.custom_loss(policy_loss, loss_inputs)

    @override(TorchModelV2)
    def metrics(self) -> Dict[str, TensorType]:
        return self.ddp_model.module.metrics()

    @override(TorchModelV2)
    def import_from_h5(self, h5_file: str) -> None:
        pass

    @override(TorchModelV2)
    def last_output(self) -> TensorType:
        return self.ddp_model.module.last_output()

    @override(TorchModelV2)
    def context(self) -> contextlib.AbstractContextManager:
        return self.ddp_model.module.context()

    @override(TorchModelV2)
    def variables(
        self, as_dict: bool = False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        return self.ddp_model.module.variables(as_dict=as_dict)

    @override(TorchModelV2)
    def trainable_variables(
        self, as_dict: bool = False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        return self.ddp_model.module.trainable_variables(as_dict=as_dict)

    @override(TorchModelV2)
    def is_time_major(self) -> bool:
        return self.ddp_model.module.is_time_major()

    def train(self):
        self.ddp_model.module.train()

    def state_dict(self):
        return self.ddp_model.module.state_dict()
