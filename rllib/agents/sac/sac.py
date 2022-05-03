import logging
from typing import Optional, Type

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.dqn.dqn import DQNTrainer
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.agents.sac.sac_tf_policy import SACTFPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    Deprecated,
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.typing import TrainerConfigDict

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()

logger = logging.getLogger(__name__)

OPTIMIZER_SHARED_CONFIGS = [
    "buffer_size",
    "prioritized_replay",
    "prioritized_replay_alpha",
    "prioritized_replay_beta",
    "prioritized_replay_eps",
    "rollout_fragment_length",
    "train_batch_size",
    "learning_starts",
]


class SACConfig(TrainerConfig):
    """Defines a SACTrainer configuration class from which a SACTrainer can be built.

    Example:
        >>> config = SACConfig() 
        >>> config.training(prioritized_replay=True)\
        >>>       .resources(num_gpus=1)\
        >>>       .rollouts(num_rollout_workers=3)\
        >>>       .environment("CartPole-v1")
        >>> trainer = DQNTrainer(config=config)
        >>> while True:
        >>>     trainer.train()

    Example:
        >>> config = SACConfig()
        >>> config.training(train_batch_size=tune.grid_search([64,128,256]))
        >>> config.environment(env="CartPole-v1")
        >>> tune.run(
        >>>     "SAC",
        >>>     stop={"episode_reward_mean":200},
        >>>     config=config.to_dict()
        >>> )

    Example:
        >>> config = SACConfig()
        >>> print(config.exploration_config)
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "initial_epsilon": 1.5,
        >>>         "final_epsilon": 0.01,
        >>>         "epsilone_timesteps": 5000,
        >>>     }
        >>> )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\
        >>>       .exploration(exploration_config=explore_config)

    Example:
        >>> config = SACConfig()
        >>> print(config.exploration_config)
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "type": "softq",
        >>>         "temperature": [1.0],
        >>>     }
        >>> )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\
        >>>       .exploration(exploration_config=explore_config)
    """

    def __init__(self):
        """Initializes a SACConfig instance."""
        super().__init__(self)

        # fmt: off
        # __sphinx_doc_begin__
        #
        self.trainer_class = SACTrainer
        self.twin_q = True
        self.Q_model = {
            "fcnet_hiddens": [256, 256],
            "fcnet_activation": "relu",
            "post_fcnet_hiddens": [],
            "post_fcnet_activation": None,
            "custom_model": None,  # Use this to define custom Q-model(s).
            "custom_model_config": {},
        }
        self.policy_model = {
            "fcnet_hiddens": [256, 256],
            "fcnet_activation": "relu",
            "post_fcnet_hiddens": [],
            "post_fcnet_activation": None,
            "custom_model": None,  # Use this to define a custom policy model.
            "custom_model_config": {},
        }
        self.clip_actions = False
        self.tau = 5e-3
        self.initial_alpha = 1.0
        self.target_entropy = "auto"
        self.n_step = 1
        self.timesteps_per_iteration = 100
        self.replay_buffer_config = {
            "_enable_replay_buffer_api": False,
            "type": "MultiAgentReplayBuffer",
            "capacity": int(1e6),
        }
        self.store_buffer_in_checkpoints = False
        self.prioritized_replay = False
        self.prioritized_replay_alpha = 0.6
        self.prioritized_replay_beta = 0.4
        self.prioritized_replay_eps = 1e-6
        self.compress_observations = False
        self.training_intensity = None
        self.optimization = {
            "actor_learning_rate": 3e-4,
            "critic_learning_rate": 3e-4,
            "entropy_learning_rate": 3e-4,
        }
        self.grad_clip = None
        self.learning_starts = 1500
        self.rollout_fragment_length = 1
        self.train_batch_size = 256
        self.target_network_update_freq = 0
        self.num_gpus = 0
        self.num_workers = 0
        self.num_gpus_per_worker = 0
        self.num_cpus_per_worker = 1
        self.worker_side_prioritization = False
        self.min_time_s_per_reporting = 1
        self._deterministic_loss = False
        self._use_beta_distribution = False
        self.use_state_preprocessor = DEPRECATED_VALUE
        # fmt: on
        # __sphinx_doc_end__
        #

    @override(TrainerConfig)
    def training(
        self,
        *,
        twin_q: Optional[bool] = None,
        Q_model: Optional[dict] = None,
        policy_model: Optional[dict] = None,
        clip_actions: Optional[bool] = None,
        tau: Optional[float] = None,
        initial_alpha: Optional[float] = None,
        target_entropy: Optional[str] = None,
        n_step: Optional[int] = None,
        timesteps_per_iteration: Optional[int] = None,
        replay_buffer_config: Optional[dict] = None,
        store_buffer_in_checkpoints: Optional[bool] = None,
        prioritized_replay: Optional[bool] = None,
        prioritized_replay_alpha: Optional[float] = None,
        prioritized_replay_beta: Optional[float] = None,
        prioritized_replay_eps: Optional[float] = None,
        compress_observations: Optional[bool] = None,
        training_intensity: Optional[float] = None,
        optimization: Optional[dict] = None,
        grad_clip: Optional[int] = None,
        learning_starts: Optional[int] = None,
        rollout_fragment_length: Optional[int] = None,
        train_batch_size: Optional[int] = None,
        target_network_update_freq: Optional[int] = None,
        num_gpus: Optional[int] = None,
        num_workers: Optional[int] = None,
        num_gpus_per_worker: Optional[int] = None,
        num_cpus_per_worker: Optional[int] = None,
        worker_side_prioritization: Optional[bool] = None,
        min_time_s_per_reporting: Optional[int] = None,
        _deterministic_loss: Optional[bool] = None,
        _use_beta_distribution: Optional[bool] = None,
        **kwargs,
    ) -> "SACConfig":
        """Sets the training related configuration.

        Args:
            twin_q: Use two Q-networks (instead of one) for action-value estimation. Note: Each Q-network will have its own target network.
            Q_model: Model options for the Q network(s). These will override MODEL_DEFAULTS.
            The `Q_model` dict is treated just as the top-level `model` dict in setting up the Q-network(s) (2 if twin_q=True).
            That means, you can do for different observation spaces:
                obs=Box(1D) -> Tuple(Box(1D) + Action) -> concat -> post_fcnet
                obs=Box(3D) -> Tuple(Box(3D) + Action) -> vision-net -> concat w/ action -> post_fcnet
                obs=Tuple(Box(1D), Box(3D)) -> Tuple(Box(1D), Box(3D), Action) -> vision-net -> concat w/ Box(1D) and action -> post_fcnet
            You can also have SAC use your custom_model as Q-model(s), by simply specifying the `custom_model` (just like you would do for the top-level `model`)
            policy_model: Model options for the policy function (see `Q_model` above for details).
            The difference to `Q_model` above is that no action concat'ing is performed before the post_fcnet stack.
            clip_actions: Actions are already normalized, no need to clip them further.
            tau: Update the target by \tau * policy + (1-\tau) * target_policy.
            initial_alpha: Initial value to use for the entropy weight alpha.
            target_entropy: Target entropy lower bound. If "auto", will be set to -|A| (e.g. -2.0 for Discrete(2), -3.0 for Box(shape=(3,))).
            This is the inverse of reward scale, and will be optimized automatically.
            n_step: N-step target updates. If >1, sars' tuples in trajectories will be postprocessed to become sa[discounted sum of R][s t+n] tuples.
            timesteps_per_iteration: Number of env steps to optimize for before returning.
            replay_buffer_config: Replay buffer config
            store_buffer_in_checkpoints: Set this to True, if you want the contents of your buffer(s) to be stored in any saved checkpoints as well.
            Warnings will be created if:
                - This is True AND restoring from a checkpoint that contains no buffer data.
                - This is False AND restoring from a checkpoint that does contain buffer data.
            prioritized_replay: If True prioritized replay buffer will be used.
            prioritized_replay_alpha: Alpha parameter controls the degree of prioritization in the buffer. In other words, when a buffer sample has a higher temporal-difference error, with how much more probability should it drawn to use to update the parametrized Q-network. 0.0 corresponds to uniform probability. Setting much above 1.0 may quickly result as the sampling distribution could become heavily “pointy” with low entropy.
            prioritized_replay_beta: Beta parameter controls the degree of importance sampling which suppresses the influence of gradient updates from samples that have higher probability of being sampled via alpha parameter and the temporal-difference error.
            prioritized_replay_eps: Epsilon parameter sets the baseline probability for sampling so that when the temporal-difference error of a sample is zero, there is still a chance of drawing the sample.
            compress_observations: Whether to LZ4 compress observations.
            training_intensity: The intensity with which to update the model (vs collecting samples from the env). If None, uses the "natural" value of:
                `train_batch_size` / (`rollout_fragment_length` x `num_workers` x `num_envs_per_worker`).
            If provided, will make sure that the ratio between ts inserted into and sampled from the buffer matches the given value.
            Example:
              training_intensity=1000.0
              train_batch_size=250 rollout_fragment_length=1
              num_workers=1 (or 0) num_envs_per_worker=1
              -> natural value = 250 / 1 = 250.0
              -> will make sure that replay+train op will be executed 4x as often as rollout+insert op (4 * 250 = 1000).
            See: rllib/agents/dqn/dqn.py::calculate_rr_weights for further details.
            optimization: Learning rate optimizations.
            grad_clip: If not None, clip gradients during optimization at this value.
            learning_starts: How many steps of the model to sample before learning starts.
            rollout_fragment_length: Update the replay buffer with this many samples at once. Note that this setting applies per-worker if num_workers > 1.
            train_batch_size: Size of a batched sampled from replay buffer for training.
            target_network_update_freq: Update the target network every `target_network_update_freq` steps.
            num_gpus: Whether to use a GPU for local optimization.
            num_workers: Number of workers for collecting samples with.
            This only makes sense to increase if your environment is particularly slow to sample, or if you"re using the Async or Ape-X optimizers.
            num_gpus_per_worker: Whether to allocate GPUs for workers (if > 0).
            num_cpus_per_worker: Whether to allocate CPUs for workers (if > 0).
            worker_side_prioritization: Whether to compute priorities on workers.
            min_time_s_per_reporting: Prevent reporting frequency from going lower than this time span.
            _deterministic_loss: Whether the loss should be calculated deterministically (w/o the stochastic action sampling step).
            True only useful for cont. actions and for debugging!
            _use_beta_distribution: Use a Beta-distribution instead of a SquashedGaussian for bounded, continuous action spaces (not recommended, for debugging only).

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if twin_q is not None:
            self.twin_q = twin_q
        if Q_model is not None:
            self.Q_model = Q_model
        if policy_model is not None:
            self.policy_model = policy_model
        if clip_actions is not None:
            self.clip_actions = clip_actions
        if tau is not None:
            self.tau = tau
        if initial_alpha is not None:
            self.initial_alpha = initial_alpha
        if target_entropy is not None:
            self.target_entropy = target_entropy
        if n_step is not None:
            self.n_step = n_step
        if timesteps_per_iteration is not None:
            self.timesteps_per_iteration = timesteps_per_iteration
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config
        if store_buffer_in_checkpoints is not None:
            self.store_buffer_in_checkpoints = store_buffer_in_checkpoints
        if prioritized_replay is not None:
            self.prioritized_replay = prioritized_replay
        if prioritized_replay_alpha is not None:
            self.prioritized_replay_alpha = prioritized_replay_alpha
        if prioritized_replay_beta is not None:
            self.prioritized_replay_beta = prioritized_replay_beta
        if prioritized_replay_eps is not None:
            self.prioritized_replay_eps = prioritized_replay_eps
        if compress_observations is not None:
            self.compress_observations = compress_observations
        if training_intensity is not None:
            self.training_intensity = training_intensity
        if optimization is not None:
            self.optimization = optimization
        if optimization is not None:
            self.optimization = optimization
        if grad_clip is not None:
            self.grad_clip = grad_clip
        if learning_starts is not None:
            self.learning_starts = learning_starts
        if rollout_fragment_length is not None:
            self.rollout_fragment_length = rollout_fragment_length
        if train_batch_size is not None:
            self.train_batch_size = train_batch_size
        if target_network_update_freq is not None:
            self.target_network_update_freq = target_network_update_freq
        if num_gpus is not None:
            self.num_gpus = num_gpus
        if num_workers is not None:
            self.num_workers = num_workers
        if num_gpus_per_worker is not None:
            self.num_gpus_per_worker = num_gpus_per_worker
        if num_cpus_per_worker is not None:
            self.num_cpus_per_worker = num_cpus_per_worker
        if worker_side_prioritization is not None:
            self.worker_side_prioritization = worker_side_priorization
        if min_time_s_per_reporting is not None:
            self.min_time_s_per_reporting = min_time_s_per_reporting
        if _deterministic_loss is not None:
            self._deterministic_loss = _determinisitic_loss
        if _use_beta_distribution is not None:
            self._use_beta_distribution = _use_beta_distribution


# Deprecated: Use ray.rllib.agents.sac.SACConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(
            with_common_config(
                {
                    "twin_q": True,
                    "use_state_preprocessor": DEPRECATED_VALUE,
                    "Q_model": {
                        "fcnet_hiddens": [256, 256],
                        "fcnet_activation": "relu",
                        "post_fcnet_hiddens": [],
                        "post_fcnet_activation": None,
                        "custom_model": None,  # To define custom Q-model(s).
                        "custom_model_config": {},
                    },
                    "policy_model": {
                        "fcnet_hiddens": [256, 256],
                        "fcnet_activation": "relu",
                        "post_fcnet_hiddens": [],
                        "post_fcnet_activation": None,
                        "custom_model": None,  # To define a custom policy model.
                        "custom_model_config": {},
                    },
                    "clip_actions": False,
                    "tau": 5e-3,
                    "initial_alpha": 1.0,
                    "target_entropy": "auto",
                    "n_step": 1,
                    "timesteps_per_iteration": 100,
                    "buffer_size": DEPRECATED_VALUE,
                    "replay_buffer_config": {
                        "_enable_replay_buffer_api": False,
                        "type": "MultiAgentReplayBuffer",
                        "capacity": int(1e6),
                    },
                    "store_buffer_in_checkpoints": False,
                    "prioritized_replay": False,
                    "prioritized_replay_alpha": 0.6,
                    "prioritized_replay_beta": 0.4,
                    "prioritized_replay_eps": 1e-6,
                    "compress_observations": False,
                    "training_intensity": None,
                    "optimization": {
                        "actor_learning_rate": 3e-4,
                        "critic_learning_rate": 3e-4,
                        "entropy_learning_rate": 3e-4,
                    },
                    "grad_clip": None,
                    "learning_starts": 1500,
                    "rollout_fragment_length": 1,
                    "train_batch_size": 256,
                    "target_network_update_freq": 0,
                    "num_gpus": 0,
                    "num_workers": 0,
                    "num_gpus_per_worker": 0,
                    "num_cpus_per_worker": 1,
                    "worker_side_prioritization": False,
                    "min_time_s_per_reporting": 1,
                    "_deterministic_loss": False,
                    "_use_beta_distribution": False,
                },
            )
        )

    @Deprecated(
        old="ray.rllib.agents.sac.sac.DEFAULT_CONFIG",
        new="ray.rllib.agents.sac.sac.SACConfig",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()


class SACTrainer(DQNTrainer):
    """Soft Actor Critic (SAC) Trainer class.

    This file defines the distributed Trainer class for the soft actor critic
    algorithm.
    See `sac_[tf|torch]_policy.py` for the definition of the policy loss.

    Detailed documentation:
    https://docs.ray.io/en/master/rllib-algorithms.html#sac
    """

    def __init__(self, *args, **kwargs):
        self._allow_unknown_subkeys += ["policy_model", "Q_model"]
        super().__init__(*args, **kwargs)

    @classmethod
    @override(DQNTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return SACConfig().to_dict()

    @override(DQNTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        """Validates the Trainer's config dict.

        Args:
            config (TrainerConfigDict): The Trainer's config to check.

        Raises:
            ValueError: In case something is wrong with the config.

        """
        # Call super's validation method.
        super().validate_config(config)

        if config["use_state_preprocessor"] != DEPRECATED_VALUE:
            deprecation_warning(old="config['use_state_preprocessor']", error=False)
            config["use_state_preprocessor"] = DEPRECATED_VALUE

        if config["grad_clip"] is not None and config["grad_clip"] <= 0.0:
            raise ValueError("`grad_clip` value must be > 0.0!")

        if config["framework"] in ["tf", "tf2", "tfe"] and tfp is None:
            logger.warning(
                "You need `tensorflow_probability` in order to run SAC! "
                "Install it via `pip install tensorflow_probability`. Your "
                f"tf.__version__={tf.__version__ if tf else None}."
                "Trying to import tfp results in the following error:"
            )
            try_import_tfp(error=True)

    @override(DQNTrainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.agents.sac.sac_torch_policy import SACTorchPolicy

            return SACTorchPolicy
        else:
            return SACTFPolicy
