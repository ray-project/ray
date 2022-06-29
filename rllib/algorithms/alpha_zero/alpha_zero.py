import logging
from typing import List, Optional, Type, Union

from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
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
from ray.rllib.utils.annotations import Deprecated, override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SYNCH_WORKER_WEIGHTS_TIMER,
)
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config
from ray.rllib.utils.typing import ResultDict, AlgorithmConfigDict
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


class AlphaZeroConfig(AlgorithmConfig):
    """Defines a configuration class from which an AlphaZero Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.alpha_zero import AlphaZeroConfig
        >>> config = AlphaZeroConfig().training(sgd_minibatch_size=256)\
        ...             .resources(num_gpus=0)\
        ...             .rollouts(num_workers=4)
        >>> print(config.to_dict())
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()

    Example:
        >>> from ray.rllib.algorithms.alpha_zero import AlphaZeroConfig
        >>> from ray import tune
        >>> config = AlphaZeroConfig()
        >>> # Print out some default values.
        >>> print(config.shuffle_sequences)
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "AlphaZero",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self, algo_class=None):
        """Initializes a PPOConfig instance."""
        super().__init__(algo_class=algo_class or AlphaZero)

        # fmt: off
        # __sphinx_doc_begin__
        # AlphaZero specific config settings:
        self.sgd_minibatch_size = 128
        self.shuffle_sequences = True
        self.num_sgd_iter = 30
        self.learning_starts = 1000
        self.replay_buffer_config = {
            "type": "ReplayBuffer",
            # Size of the replay buffer in batches (not timesteps!).
            "capacity": 1000,
            # When to start returning samples (in batches, not timesteps!).
            "learning_starts": 500,
            # Choosing `fragments` here makes it so that the buffer stores entire
            # batches, instead of sequences, episodes or timesteps.
            "storage_unit": "fragments",
        }
        self.lr_schedule = None
        self.vf_share_layers = False
        self.mcts_config = {
            "puct_coefficient": 1.0,
            "num_simulations": 30,
            "temperature": 1.5,
            "dirichlet_epsilon": 0.25,
            "dirichlet_noise": 0.03,
            "argmax_tree_policy": False,
            "add_dirichlet_noise": True,
        }
        self.ranked_rewards = {
            "enable": True,
            "percentile": 75,
            "buffer_max_length": 1000,
            # add rewards obtained from random policy to
            # "warm start" the buffer
            "initialize_buffer": True,
            "num_init_rewards": 100,
        }

        # Override some of AlgorithmConfig's default values with AlphaZero-specific
        # values.
        self.framework_str = "torch"
        self.callbacks_class = AlphaZeroDefaultCallbacks
        self.lr = 5e-5
        self.rollout_fragment_length = 200
        self.train_batch_size = 4000
        self.batch_mode = "complete_episodes"
        # Extra configuration that disables exploration.
        self.evaluation_config = {
            "mcts_config": {
                "argmax_tree_policy": True,
                "add_dirichlet_noise": False,
            },
        }
        # __sphinx_doc_end__
        # fmt: on

        self.buffer_size = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        sgd_minibatch_size: Optional[int] = None,
        shuffle_sequences: Optional[bool] = None,
        num_sgd_iter: Optional[int] = None,
        replay_buffer_config: Optional[dict] = None,
        lr_schedule: Optional[List[List[Union[int, float]]]] = None,
        vf_share_layers: Optional[bool] = None,
        mcts_config: Optional[dict] = None,
        ranked_rewards: Optional[dict] = None,
        **kwargs,
    ) -> "AlphaZeroConfig":
        """Sets the training related configuration.

        Args:
            sgd_minibatch_size: Total SGD batch size across all devices for SGD.
            shuffle_sequences: Whether to shuffle sequences in the batch when training
                (recommended).
            num_sgd_iter: Number of SGD iterations in each outer loop.
            replay_buffer_config: Replay buffer config.
                Examples:
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentReplayBuffer",
                "learning_starts": 1000,
                "capacity": 50000,
                "replay_sequence_length": 1,
                }
                - OR -
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentPrioritizedReplayBuffer",
                "capacity": 50000,
                "prioritized_replay_alpha": 0.6,
                "prioritized_replay_beta": 0.4,
                "prioritized_replay_eps": 1e-6,
                "replay_sequence_length": 1,
                }
                - Where -
                prioritized_replay_alpha: Alpha parameter controls the degree of
                prioritization in the buffer. In other words, when a buffer sample has
                a higher temporal-difference error, with how much more probability
                should it drawn to use to update the parametrized Q-network. 0.0
                corresponds to uniform probability. Setting much above 1.0 may quickly
                result as the sampling distribution could become heavily “pointy” with
                low entropy.
                prioritized_replay_beta: Beta parameter controls the degree of
                importance sampling which suppresses the influence of gradient updates
                from samples that have higher probability of being sampled via alpha
                parameter and the temporal-difference error.
                prioritized_replay_eps: Epsilon parameter sets the baseline probability
                for sampling so that when the temporal-difference error of a sample is
                zero, there is still a chance of drawing the sample.
            lr_schedule: Learning rate schedule. In the format of
                [[timestep, lr-value], [timestep, lr-value], ...]
                Intermediary timesteps will be assigned to interpolated learning rate
                values. A schedule should normally start from timestep 0.
            vf_share_layers: Share layers for value function. If you set this to True,
                it's important to tune vf_loss_coeff.
            mcts_config: MCTS specific settings.
            ranked_rewards: Settings for the ranked reward (r2) algorithm
                from: https://arxiv.org/pdf/1807.01672.pdf

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if sgd_minibatch_size is not None:
            self.sgd_minibatch_size = sgd_minibatch_size
        if shuffle_sequences is not None:
            self.shuffle_sequences = shuffle_sequences
        if num_sgd_iter is not None:
            self.num_sgd_iter = num_sgd_iter
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config
        if lr_schedule is not None:
            self.lr_schedule = lr_schedule
        if vf_share_layers is not None:
            self.vf_share_layers = vf_share_layers
        if mcts_config is not None:
            self.mcts_config = mcts_config
        if ranked_rewards is not None:
            self.ranked_rewards = ranked_rewards

        return self


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
        _, env_creator = Algorithm._get_env_id_and_creator(config["env"], config)
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


class AlphaZero(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return AlphaZeroConfig().to_dict()

    def validate_config(self, config: AlgorithmConfigDict) -> None:
        """Checks and updates the config based on settings."""
        # Call super's validation method.
        super().validate_config(config)
        validate_buffer_config(config)

    @override(Algorithm)
    def get_default_policy_class(self, config: AlgorithmConfigDict) -> Type[Policy]:
        return AlphaZeroPolicyWrapperClass

    @override(Algorithm)
    def training_step(self) -> ResultDict:
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
    @override(Algorithm)
    def execution_plan(
        workers: WorkerSet, config: AlgorithmConfigDict, **kwargs
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


# Deprecated: Use ray.rllib.algorithms.alpha_zero.AlphaZeroConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(AlphaZeroConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.alpha_zero.alpha_zero.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.alpha_zero.alpha_zero.AlphaZeroConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
