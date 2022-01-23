"""
Deep Q-Networks (DQN, Rainbow, Parametric DQN)
==============================================

This file defines the distributed Trainer class for the Deep Q-Networks
algorithm. See `dqn_[tf|torch]_policy.py` for the definition of the policies.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#deep-q-networks-dqn-rainbow-parametric-dqn
"""  # noqa: E501

import logging
from typing import List, Optional, Type

from ray.rllib.agents.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.agents.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.agents.dqn.simple_q import SimpleQTrainer, \
    DEFAULT_CONFIG as SIMPLEQ_DEFAULT_CONFIG
from ray.rllib.agents.trainer import Trainer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import TrainOneStep, UpdateTargetNetwork, \
    MultiGPUTrainOneStep
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.iter import LocalIterator

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = Trainer.merge_trainer_configs(
    SIMPLEQ_DEFAULT_CONFIG,
    {
        # === Model ===
        # Number of atoms for representing the distribution of return. When
        # this is greater than 1, distributional Q-learning is used.
        # the discrete supports are bounded by v_min and v_max
        "num_atoms": 1,
        "v_min": -10.0,
        "v_max": 10.0,
        # Whether to use noisy network
        "noisy": False,
        # control the initial value of noisy nets
        "sigma0": 0.5,
        # Whether to use dueling dqn
        "dueling": True,
        # Dense-layer setup for each the advantage branch and the value branch
        # in a dueling architecture.
        "hiddens": [256],
        # Whether to use double dqn
        "double_q": True,
        # N-step Q learning
        "n_step": 1,

        # === Prioritized replay buffer ===
        # If True prioritized replay buffer will be used.
        "prioritized_replay": True,
        # Alpha parameter for prioritized replay buffer.
        "prioritized_replay_alpha": 0.6,
        # Beta parameter for sampling from prioritized replay buffer.
        "prioritized_replay_beta": 0.4,
        # Final value of beta (by default, we use constant beta=0.4).
        "final_prioritized_replay_beta": 0.4,
        # Time steps over which the beta parameter is annealed.
        "prioritized_replay_beta_annealing_timesteps": 20000,
        # Epsilon to add to the TD errors when updating priorities.
        "prioritized_replay_eps": 1e-6,

        # Callback to run before learning on a multi-agent batch of
        # experiences.
        "before_learn_on_batch": None,

        # The intensity with which to update the model (vs collecting samples
        # from the env). If None, uses the "natural" value of:
        # `train_batch_size` / (`rollout_fragment_length` x `num_workers` x
        # `num_envs_per_worker`).
        # If provided, will make sure that the ratio between ts inserted into
        # and sampled from the buffer matches the given value.
        # Example:
        #   training_intensity=1000.0
        #   train_batch_size=250 rollout_fragment_length=1
        #   num_workers=1 (or 0) num_envs_per_worker=1
        #   -> natural value = 250 / 1 = 250.0
        #   -> will make sure that replay+train op will be executed 4x as
        #      often as rollout+insert op (4 * 250 = 1000).
        # See: rllib/agents/dqn/dqn.py::calculate_rr_weights for further
        # details.
        "training_intensity": None,

        # === Parallelism ===
        # Whether to compute priorities on workers.
        "worker_side_prioritization": False,
    },
    _allow_unknown_configs=True,
)
# __sphinx_doc_end__
# yapf: enable


def calculate_rr_weights(config: TrainerConfigDict) -> List[float]:
    """Calculate the round robin weights for the rollout and train steps"""
    if not config["training_intensity"]:
        return [1, 1]

    # Calculate the "native ratio" as:
    # [train-batch-size] / [size of env-rolled-out sampled data]
    # This is to set freshly rollout-collected data in relation to
    # the data we pull from the replay buffer (which also contains old
    # samples).
    native_ratio = config["train_batch_size"] / \
        (config["rollout_fragment_length"] *
         config["num_envs_per_worker"] * config["num_workers"])

    # Training intensity is specified in terms of
    # (steps_replayed / steps_sampled), so adjust for the native ratio.
    weights = [1, config["training_intensity"] / native_ratio]
    return weights


class DQNTrainer(SimpleQTrainer):
    @classmethod
    @override(SimpleQTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(SimpleQTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        # Update effective batch size to include n-step
        adjusted_rollout_len = max(config["rollout_fragment_length"],
                                   config["n_step"])
        config["rollout_fragment_length"] = adjusted_rollout_len

    @override(SimpleQTrainer)
    def get_default_policy_class(
            self, config: TrainerConfigDict) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return DQNTorchPolicy
        else:
            return DQNTFPolicy

    @staticmethod
    @override(SimpleQTrainer)
    def execution_plan(workers: WorkerSet, config: TrainerConfigDict,
                       **kwargs) -> LocalIterator[dict]:
        assert "local_replay_buffer" in kwargs, (
            "DQN's execution plan requires a local replay buffer.")

        # Assign to Trainer, so we can store the MultiAgentReplayBuffer's
        # data when we save checkpoints.
        local_replay_buffer = kwargs["local_replay_buffer"]

        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        # We execute the following steps concurrently:
        # (1) Generate rollouts and store them in our local replay buffer.
        # Calling next() on store_op drives this.
        store_op = rollouts.for_each(
            StoreToReplayBuffer(local_buffer=local_replay_buffer))

        def update_prio(item):
            samples, info_dict = item
            if config.get("prioritized_replay"):
                prio_dict = {}
                for policy_id, info in info_dict.items():
                    # TODO(sven): This is currently structured differently for
                    #  torch/tf. Clean up these results/info dicts across
                    #  policies (note: fixing this in torch_policy.py will
                    #  break e.g. DDPPO!).
                    td_error = info.get(
                        "td_error", info[LEARNER_STATS_KEY].get("td_error"))
                    samples.policy_batches[policy_id].set_get_interceptor(None)
                    batch_indices = samples.policy_batches[policy_id].get(
                        "batch_indexes")
                    # In case the buffer stores sequences, TD-error could
                    # already be calculated per sequence chunk.
                    if len(batch_indices) != len(td_error):
                        T = local_replay_buffer.replay_sequence_length
                        assert len(batch_indices) > len(
                            td_error) and len(batch_indices) % T == 0
                        batch_indices = batch_indices.reshape([-1, T])[:, 0]
                        assert len(batch_indices) == len(td_error)
                    prio_dict[policy_id] = (batch_indices, td_error)
                local_replay_buffer.update_priorities(prio_dict)
            return info_dict

        # (2) Read and train on experiences from the replay buffer. Every batch
        # returned from the LocalReplay() iterator is passed to TrainOneStep to
        # take a SGD step, and then we decide whether to update the target
        # network.
        post_fn = config.get("before_learn_on_batch") or (lambda b, *a: b)

        if config["simple_optimizer"]:
            train_step_op = TrainOneStep(workers)
        else:
            train_step_op = MultiGPUTrainOneStep(
                workers=workers,
                sgd_minibatch_size=config["train_batch_size"],
                num_sgd_iter=1,
                num_gpus=config["num_gpus"],
                _fake_gpus=config["_fake_gpus"])

        replay_op = Replay(local_buffer=local_replay_buffer) \
            .for_each(lambda x: post_fn(x, workers, config)) \
            .for_each(train_step_op) \
            .for_each(update_prio) \
            .for_each(UpdateTargetNetwork(
                workers, config["target_network_update_freq"]))

        # Alternate deterministically between (1) and (2).
        # Only return the output of (2) since training metrics are not
        # available until (2) runs.
        train_op = Concurrently(
            [store_op, replay_op],
            mode="round_robin",
            output_indexes=[1],
            round_robin_weights=calculate_rr_weights(config))

        return StandardMetricsReporting(train_op, workers, config)


@Deprecated(
    new="Sub-class directly from `DQNTrainer` and override its methods",
    error=False)
class GenericOffPolicyTrainer(DQNTrainer):
    pass
