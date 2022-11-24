import logging
from typing import List, Optional, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.callbacks import DefaultCallbacks

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import restore_original_dimensions
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config
from ray.rllib.utils.deprecation import DEPRECATED_VALUE

from ray.rllib.algorithms.leela_zero.leela_zero_policy import LeelaZeroPolicy
from ray.rllib.algorithms.leela_zero.mcts import MCTS

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class LeelaZeroDefaultCallbacks(DefaultCallbacks):
    """LeelaZero callbacks.
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

    @override(DefaultCallbacks)
    def on_episode_end(self, worker, base_env, policies, episode, **kwargs):
        env = base_env.get_sub_environments()[0]
        if env.env.board.outcome():
            winner = env.env.board.outcome().winner
        else:
            winner = "Draw"
        logging.info(
            "Game Over:"
            + str(winner)
            + "Reward:"
            + str(policies)
            + str(episode.agent_rewards)
        )


class LeelaZeroConfig(AlgorithmConfig):
    """Defines a configuration class from which a LeelaZero Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.leela_zero import LeelaZeroConfig
        >>> config = LeelaZeroConfig()   # doctest: +SKIP
        >>> config = config.training(sgd_minibatch_size=256)   # doctest: +SKIP
        >>> config = config..resources(num_gpus=0)   # doctest: +SKIP
        >>> config = config..rollouts(num_rollout_workers=4)   # doctest: +SKIP
        >>> print(config.to_dict()) # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")  # doctest: +SKIP
        >>> algo.train() # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.leela_zero import LeelaZeroConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = LeelaZeroConfig()
        >>> # Print out some default values.
        >>> print(config.shuffle_sequences) # doctest: +SKIP
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]))  # doctest: +SKIP
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")   # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner( # doctest: +SKIP
        ...     "LeelaZero",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a LeelaZeroConfig instance."""
        super().__init__(algo_class=algo_class or LeelaZero)

        # fmt: off
        # __sphinx_doc_begin__
        # LeelaZero specific config settings:
        self.sgd_minibatch_size = 256
        self.shuffle_sequences = True
        self.num_sgd_iter = 1
        self.replay_buffer_config = {
            "type": "ReplayBuffer",
            # Size of the replay buffer in batches (not timesteps!).
            "capacity": 1000,
            # Choosing `fragments` here makes it so that the buffer stores entire
            # batches, instead of sequences, episodes or timesteps.
            "storage_unit": "fragments",
        }
        # Number of timesteps to collect from rollout workers before we start
        # sampling from replay buffers for learning. Whether we count this in agent
        # steps  or environment steps depends on config["multiagent"]["count_steps_by"].
        self.num_steps_sampled_before_learning_starts = 1
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
            "epsilon": 0.05,
            "turn_based_flip": True,
            "argmax_child_value": True,
        },
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
        self.callbacks_class = LeelaZeroDefaultCallbacks
        self.lr = 5e-5
        self.num_rollout_workers = 2
        self.rollout_fragment_length = 200
        self.train_batch_size = 256
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
        self, *, callbacks_class: Optional[DefaultCallbacks] = NotProvided
    ) -> "LeelaZeroConfig":
        super().callbacks(**kwargs)

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
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        vf_share_layers: Optional[bool] = NotProvided,
        mcts_config: Optional[dict] = NotProvided,
        ranked_rewards: Optional[dict] = NotProvided,
        num_steps_sampled_before_learning_starts: Optional[int] = NotProvided,
        **kwargs,
    ) -> "LeelaZeroConfig":
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
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if vf_share_layers is not NotProvided:
            self.vf_share_layers = vf_share_layers
        if mcts_config is not NotProvided:
            self.mcts_config = mcts_config
        if ranked_rewards is not NotProvided:
            self.ranked_rewards.update(ranked_rewards)
        if num_steps_sampled_before_learning_starts is not NotProvided:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )

        return self

    @override(AlgorithmConfig)
    def update_from_dict(self, config_dict) -> "LeelaZeroConfig":
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


def leela_zero_loss(policy, model, dist_class, train_batch):
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


class LeelaZeroPolicyWrapperClass(LeelaZeroPolicy):
    def __init__(self, obs_space, action_space, config):
        model = ModelCatalog.get_model_v2(
            obs_space, action_space, action_space.n, config["model"], "torch"
        )
        _, env_creator = Algorithm._get_env_id_and_creator(config["env"], config)
        # if config["ranked_rewards"]["enable"]:
        #     # if r2 is enabled, tne env is wrapped to include a rewards buffer
        #     # used to normalize rewards
        #     env_cls = get_r2_env_wrapper(env_creator, config["ranked_rewards"])

        #     # the wrapped env is used only in the mcts, not in the
        #     # rollout workers
        #     def _env_creator():
        #         return env_cls(config["env_config"])

        # else:

        def _env_creator():
            return env_creator(config["env_config"])

        def mcts_creator():
            mcts_params = config["mcts_config"]
            return MCTS(model, *mcts_params)

        super().__init__(
            obs_space,
            action_space,
            config,
            model,
            leela_zero_loss,
            TorchCategorical,
            mcts_creator,
            _env_creator,
        )


class LeelaZero(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return LeelaZeroConfig()

    @override(Algorithm)
    def get_default_policy_class(self, config: AlgorithmConfig) -> Type[Policy]:
        return LeelaZeroPolicyWrapperClass

    # @staticmethod
    # @override(Algorithm)
    # def execution_plan(
    #     workers: WorkerSet, config: AlgorithmConfig, **kwargs
    # ) -> LocalIterator[dict]:
    #     assert (
    #         len(kwargs) == 0
    #     ), "Alpha zero execution_plan does NOT take any additional parameters"

    #     rollouts = ParallelRollouts(workers, mode="bulk_sync")

    #     if config["simple_optimizer"]:
    #         train_op = rollouts.combine(
    #             ConcatBatches(
    #                 min_batch_size=config["train_batch_size"],
    #                 count_steps_by=config["multiagent"]["count_steps_by"],
    #             )
    #         ).for_each(TrainOneStep(workers, num_sgd_iter=config["num_sgd_iter"]))
    #     else:
    #         replay_buffer = SimpleReplayBuffer(config["buffer_size"])

    #         store_op = rollouts.for_each(
    #             StoreToReplayBuffer(local_buffer=replay_buffer)
    #         )

    #         replay_op = (
    #             Replay(local_buffer=replay_buffer)
    #             .filter(WaitUntilTimestepsElapsed(config["learning_starts"]))
    #             .combine(
    #                 ConcatBatches(
    #                     min_batch_size=config["train_batch_size"],
    #                     count_steps_by=config["multiagent"]["count_steps_by"],
    #                 )
    #             )
    #             .for_each(TrainOneStep(workers, num_sgd_iter=config["num_sgd_iter"]))
    #         )

    #         train_op = Concurrently(
    #             [store_op, replay_op], mode="round_robin", output_indexes=[1]
    #         )

    #     return StandardMetricsReporting(train_op, workers, config)
