import logging
from typing import Type

from ray.rllib.agents import with_common_config
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.agents.trainer import Trainer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.replay_ops import (
    SimpleReplayBuffer,
    Replay,
    StoreToReplayBuffer,
    WaitUntilTimestepsElapsed,
)
from ray.rllib.execution.rollout_ops import (
    ParallelRollouts,
    ConcatBatches,
    synchronous_parallel_sample,
)
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
    TrainOneStep,
)
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import restore_original_dimensions
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SYNCH_WORKER_WEIGHTS_TIMER,
)
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config
from ray.rllib.utils.typing import ResultDict, TrainerConfigDict
from ray.util.iter import LocalIterator

from ray.rllib.algorithms.alpha_zero.alpha_zero_policy import AlphaZeroPolicy
from ray.rllib.algorithms.alpha_zero.mcts import MCTS
from ray.rllib.algorithms.alpha_zero.ranked_rewards import get_r2_env_wrapper

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class AlphaZeroDefaultCallbacks(DefaultCallbacks):
    """AlphaZero callbacks.

    If you use custom callbacks, you must extend this class and call super()
    for on_episode_start.
    """

    def on_episode_start(self, worker, base_env, policies, episode, **kwargs):
        # save env state when an episode starts
        env = base_env.get_sub_environments()[0]
        state = env.get_state()
        episode.user_data["initial_state"] = state


# fmt: off
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # Size of batches collected from each worker
    "rollout_fragment_length": 200,
    # Number of timesteps collected for each SGD round
    "train_batch_size": 4000,
    # Total SGD batch size across all devices for SGD
    "sgd_minibatch_size": 128,
    # Whether to shuffle sequences in the batch when training (recommended)
    "shuffle_sequences": True,
    # Number of SGD iterations in each outer loop
    "num_sgd_iter": 30,
    # In case a buffer optimizer is used
    "learning_starts": 1000,
    # Size of the replay buffer in batches (not timesteps!).
    "buffer_size": DEPRECATED_VALUE,
    "replay_buffer_config": {
        "_enable_replay_buffer_api": True,
        "type": "SimpleReplayBuffer",
        # Size of the replay buffer in batches (not timesteps!).
        "capacity": 1000,
        # When to start returning samples (in batches, not timesteps!).
        "learning_starts": 500,
    },
    # Stepsize of SGD
    "lr": 5e-5,
    # Learning rate schedule
    "lr_schedule": None,
    # Share layers for value function. If you set this to True, it"s important
    # to tune vf_loss_coeff.
    "vf_share_layers": False,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "complete_episodes",
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",

    # === MCTS ===
    "mcts_config": {
        "puct_coefficient": 1.0,
        "num_simulations": 30,
        "temperature": 1.5,
        "dirichlet_epsilon": 0.25,
        "dirichlet_noise": 0.03,
        "argmax_tree_policy": False,
        "add_dirichlet_noise": True,
    },

    # === Ranked Rewards ===
    # implement the ranked reward (r2) algorithm
    # from: https://arxiv.org/pdf/1807.01672.pdf
    "ranked_rewards": {
        "enable": True,
        "percentile": 75,
        "buffer_max_length": 1000,
        # add rewards obtained from random policy to
        # "warm start" the buffer
        "initialize_buffer": True,
        "num_init_rewards": 100,
    },

    # === Evaluation ===
    # Extra configuration that disables exploration.
    "evaluation_config": {
        "mcts_config": {
            "argmax_tree_policy": True,
            "add_dirichlet_noise": False,
        },
    },

    # === Callbacks ===
    "callbacks": AlphaZeroDefaultCallbacks,

    "framework": "torch",  # Only PyTorch supported so far.
})
# __sphinx_doc_end__
# fmt: on


def alpha_zero_loss(policy, model, dist_class, train_batch):
    # get inputs unflattened inputs
    input_dict = restore_original_dimensions(
        train_batch["obs"], policy.observation_space, "torch"
    )
    # forward pass in model
    model_out = model.forward(input_dict, None, [1])
    logits, _ = model_out
    values = model.value_function()
    logits, values = torch.squeeze(logits), torch.squeeze(values)
    priors = nn.Softmax(dim=-1)(logits)
    # compute actor and critic losses
    policy_loss = torch.mean(
        -torch.sum(train_batch["mcts_policies"] * torch.log(priors), dim=-1)
    )
    value_loss = torch.mean(torch.pow(values - train_batch["value_label"], 2))
    # compute total loss
    total_loss = (policy_loss + value_loss) / 2
    return total_loss, policy_loss, value_loss


class AlphaZeroPolicyWrapperClass(AlphaZeroPolicy):
    def __init__(self, obs_space, action_space, config):
        model = ModelCatalog.get_model_v2(
            obs_space, action_space, action_space.n, config["model"], "torch"
        )
        env_creator = Trainer._get_env_creator_from_env_id(None, config["env"])
        if config["ranked_rewards"]["enable"]:
            # if r2 is enabled, tne env is wrapped to include a rewards buffer
            # used to normalize rewards
            env_cls = get_r2_env_wrapper(env_creator, config["ranked_rewards"])

            # the wrapped env is used only in the mcts, not in the
            # rollout workers
            def _env_creator():
                return env_cls(config["env_config"])

        else:

            def _env_creator():
                return env_creator(config["env_config"])

        def mcts_creator():
            return MCTS(model, config["mcts_config"])

        super().__init__(
            obs_space,
            action_space,
            config,
            model,
            alpha_zero_loss,
            TorchCategorical,
            mcts_creator,
            _env_creator,
        )


class AlphaZeroTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    def validate_config(self, config: TrainerConfigDict) -> None:
        """Checks and updates the config based on settings."""
        # Call super's validation method.
        super().validate_config(config)
        validate_buffer_config(config)

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        return AlphaZeroPolicyWrapperClass

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        """TODO:

        Returns:
            The results dict from executing the training iteration.
        """

        # Sample n MultiAgentBatches from n workers.
        new_sample_batches = synchronous_parallel_sample(
            worker_set=self.workers, concat=False
        )

        for batch in new_sample_batches:
            # Update sampling step counters.
            self._counters[NUM_ENV_STEPS_SAMPLED] += batch.env_steps()
            self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
            # Store new samples in the replay buffer
            # Use deprecated add_batch() to support old replay buffers for now
            if self.local_replay_buffer is not None:
                self.local_replay_buffer.add(batch)

        if self.local_replay_buffer is not None:
            train_batch = self.local_replay_buffer.sample(
                self.config["train_batch_size"]
            )
        else:
            train_batch = SampleBatch.concat_samples(new_sample_batches)

        # Learn on the training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        train_results = {}
        if train_batch is not None:
            if self.config.get("simple_optimizer") is True:
                train_results = train_one_step(self, train_batch)
            else:
                train_results = multi_gpu_train_one_step(self, train_batch)

        # TODO: Move training steps counter update outside of `train_one_step()` method.
        # # Update train step counters.
        # self._counters[NUM_ENV_STEPS_TRAINED] += train_batch.env_steps()
        # self._counters[NUM_AGENT_STEPS_TRAINED] += train_batch.agent_steps()

        # Update weights and global_vars - after learning on the local worker - on all
        # remote workers.
        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            self.workers.sync_weights(global_vars=global_vars)

        # Return all collected metrics for the iteration.
        return train_results

    @staticmethod
    @override(Trainer)
    def execution_plan(
        workers: WorkerSet, config: TrainerConfigDict, **kwargs
    ) -> LocalIterator[dict]:
        assert (
            len(kwargs) == 0
        ), "Alpha zero execution_plan does NOT take any additional parameters"

        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        if config["simple_optimizer"]:
            train_op = rollouts.combine(
                ConcatBatches(
                    min_batch_size=config["train_batch_size"],
                    count_steps_by=config["multiagent"]["count_steps_by"],
                )
            ).for_each(TrainOneStep(workers, num_sgd_iter=config["num_sgd_iter"]))
        else:
            replay_buffer = SimpleReplayBuffer(config["buffer_size"])

            store_op = rollouts.for_each(
                StoreToReplayBuffer(local_buffer=replay_buffer)
            )

            replay_op = (
                Replay(local_buffer=replay_buffer)
                .filter(WaitUntilTimestepsElapsed(config["learning_starts"]))
                .combine(
                    ConcatBatches(
                        min_batch_size=config["train_batch_size"],
                        count_steps_by=config["multiagent"]["count_steps_by"],
                    )
                )
                .for_each(TrainOneStep(workers, num_sgd_iter=config["num_sgd_iter"]))
            )

            train_op = Concurrently(
                [store_op, replay_op], mode="round_robin", output_indexes=[1]
            )

        return StandardMetricsReporting(train_op, workers, config)
