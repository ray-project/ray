import logging
from typing import List, Optional, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    train_one_step,
)
from ray.rllib.utils.typing import ResultDict
from ray.rllib.policy.sample_batch import concat_samples
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import restore_original_dimensions
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config
from ray.rllib.utils.replay_buffers import PrioritizedReplayBuffer
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SYNCH_WORKER_WEIGHTS_TIMER,
)

from ray.rllib.algorithms.leela_chess_zero.leela_chess_zero_model import (
    LeelaChessZeroModel,
)
from ray.rllib.algorithms.leela_chess_zero.leela_chess_zero_policy import (
    LeelaChessZeroPolicy,
)
from ray.rllib.algorithms.leela_chess_zero.mcts import MCTS

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class LeelaChessZeroDefaultCallbacks(DefaultCallbacks):
    """LeelaChessZero callbacks.
    If you use custom callbacks, you must extend this class and call super()
    for on_episode_start.
    """

    def __init__(self):
        super().__init__()

    @override(DefaultCallbacks)
    def on_episode_start(self, worker, base_env, policies, episode, **kwargs):
        # save env state when an episode starts
        env = base_env.get_sub_environments()[0]
        state = env.get_state()
        episode.user_data["initial_state"] = state
        episode.user_data["current_state"] = [state]

    @override(DefaultCallbacks)
    def on_episode_step(self, worker, base_env, policies, episode, **kwargs) -> None:
        env = base_env.get_sub_environments()[0]
        state = env.get_state()
        episode.user_data["current_state"].append(state)


class LeelaChessZeroConfig(AlgorithmConfig):
    """Defines a configuration class from which a LeelaChessZero Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.leela_chess_zero import LeelaChessZeroConfig
        >>> config = LeelaChessZeroConfig()   # doctest: +SKIP
        >>> config = config.training(sgd_minibatch_size=256)   # doctest: +SKIP
        >>> config = config..resources(num_gpus=0)   # doctest: +SKIP
        >>> config = config..rollouts(num_rollout_workers=4)   # doctest: +SKIP
        >>> print(config.to_dict()) # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")  # doctest: +SKIP
        >>> algo.train() # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.leela_chess_zero import LeelaChessZeroConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = LeelaChessZeroConfig()
        >>> # Print out some default values.
        >>> print(config.shuffle_sequences) # doctest: +SKIP
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]))  # doctest: +SKIP
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")   # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner( # doctest: +SKIP
        ...     "LeelaChessZero",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a LeelaChessZeroConfig instance."""
        super().__init__(algo_class=algo_class or LeelaChessZero)

        # fmt: off
        # __sphinx_doc_begin__
        # LeelaChessZero specific config settings:
        self.sgd_minibatch_size = 256
        self.shuffle_sequences = True
        self.num_sgd_iter = 30
        self.replay_buffer_config = {
            "_enable_replay_buffer_api": True,
            "type": "MultiAgentReplayBuffer",
            "underlying_replay_buffer_config": {
                "type": PrioritizedReplayBuffer,
                "capacity": 10000, "storage_unit": "episodes",
                "prioritized_replay_alpha": 0.6, "prioritized_replay_beta": 0.4,
                "prioritized_replay_eps": 1e-6,
            },
        }
        # Number of timesteps to collect from rollout workers before we start
        # sampling from replay buffers for learning. Whether we count this in agent
        # steps  or environment steps depends on config["multiagent"]["count_steps_by"].
        self.num_steps_sampled_before_learning_starts = 1000
        self.lr_schedule = None
        self.vf_share_layers = False
        self.mcts_config = {
            "puct_coefficient": 2**0.5,
            "num_simulations": 25,
            "temperature": 1.5,
            "dirichlet_epsilon": 0.25,
            "dirichlet_noise": 0.03,
            "argmax_tree_policy": True,
            "add_dirichlet_noise": True,
            "epsilon": 0.05,
            "turn_based_flip": True,
            "argmax_child_value": True,
        }
        self.model = {"custom_model" : LeelaChessZeroModel}

        # Override some of AlgorithmConfig's default values with AlphaZero-specific
        # values.
        self.framework_str = "torch"
        self.callbacks_class = LeelaChessZeroDefaultCallbacks
        self.lr = 1e-3
        self.num_rollout_workers = 8
        self.rollout_fragment_length = 200
        self.train_batch_size = 2048
        self.batch_mode = "complete_episodes"
        # Extra configuration for eval that disables exploration.
        self.evaluation(evaluation_config={
            "mcts_config": {
                "argmax_tree_policy": True,
                "add_dirichlet_noise": False,
            },
        })
        # __sphinx_doc_end__
        # fmt: on

        self.buffer_size = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def callbacks(
        self, *, callbacks_class: Optional[DefaultCallbacks] = NotProvided, **kwargs
    ) -> "LeelaChessZeroConfig":
        super().callbacks(callbacks_class, **kwargs)

        if callbacks_class is not NotProvided:
            self.callbacks_class = callbacks_class
        return self

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        sgd_minibatch_size: Optional[int] = NotProvided,
        shuffle_sequences: Optional[bool] = NotProvided,
        num_sgd_iter: Optional[int] = NotProvided,
        replay_buffer_config: Optional[dict] = NotProvided,
        lr: Optional[float] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        vf_share_layers: Optional[bool] = NotProvided,
        mcts_config: Optional[dict] = NotProvided,
        num_steps_sampled_before_learning_starts: Optional[int] = NotProvided,
        model: Optional[dict] = NotProvided,
        **kwargs,
    ) -> "LeelaChessZeroConfig":
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
            num_steps_sampled_before_learning_starts: Number of timesteps to collect
                from rollout workers before we start sampling from replay buffers for
                learning. Whether we count this in agent steps  or environment steps
                depends on config["multiagent"]["count_steps_by"].

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if sgd_minibatch_size is not NotProvided:
            self.sgd_minibatch_size = sgd_minibatch_size
        if shuffle_sequences is not NotProvided:
            self.shuffle_sequences = shuffle_sequences
        if num_sgd_iter is not NotProvided:
            self.num_sgd_iter = num_sgd_iter
        if replay_buffer_config is not NotProvided:
            self.replay_buffer_config = replay_buffer_config
        if lr is not NotProvided:
            self.lr = lr
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if vf_share_layers is not NotProvided:
            self.vf_share_layers = vf_share_layers
        if mcts_config is not NotProvided:
            # only assign provided keys
            for k, v in mcts_config.items():
                self.mcts_config[k] = v
        if num_steps_sampled_before_learning_starts is not NotProvided:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )
        if model is not NotProvided:
            self.model = model

        return self

    @override(AlgorithmConfig)
    def update_from_dict(self, config_dict) -> "LeelaChessZeroConfig":
        config_dict = config_dict.copy()

        if "ranked_rewards" in config_dict:
            value = config_dict.pop("ranked_rewards")
            self.training(ranked_rewards=value)

        return super().update_from_dict(config_dict)

    @override(AlgorithmConfig)
    def validate(self) -> None:
        """Checks and updates the config based on settings."""
        # Call super's validation method.
        super().validate()
        validate_buffer_config(self)


def leela_chess_zero_loss(policy, model, dist_class, train_batch):
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


class LeelaChessZeroPolicyWrapperClass(LeelaChessZeroPolicy):
    def __init__(self, obs_space, action_space, config):
        model = ModelCatalog.get_model_v2(
            obs_space, action_space, action_space.n, config["model"], "torch"
        )
        _, env_creator = Algorithm._get_env_id_and_creator(config["env"], config)

        def _env_creator():
            return env_creator(config["env_config"])

        def mcts_creator():
            mcts_params = config["mcts_config"]
            return MCTS(model, mcts_params)

        super().__init__(
            obs_space,
            action_space,
            config,
            model,
            leela_chess_zero_loss,
            TorchCategorical,
            mcts_creator,
            _env_creator,
        )


class LeelaChessZero(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return LeelaChessZeroConfig()

    @override(Algorithm)
    def get_default_policy_class(self, *args, **kwargs) -> Optional[Type[Policy]]:
        return LeelaChessZeroPolicyWrapperClass

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
            if self.local_replay_buffer is not None:
                self.local_replay_buffer.add(batch)

        if self.local_replay_buffer is not None:
            # Update target network every `target_network_update_freq` sample steps.
            cur_ts = self._counters[
                NUM_AGENT_STEPS_SAMPLED
                if self.config.count_steps_by == "agent_steps"
                else NUM_ENV_STEPS_SAMPLED
            ]

            if cur_ts > self.config.num_steps_sampled_before_learning_starts:
                train_batch = self.local_replay_buffer.sample(
                    self.config.train_batch_size
                )
            else:
                train_batch = None
        else:
            train_batch = concat_samples(new_sample_batches)

        # Learn on the training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        train_results = {}
        if train_batch is not None:
            train_results = train_one_step(self, train_batch)

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


DEFAULT_CONFIG = LeelaChessZeroConfig().to_dict()
