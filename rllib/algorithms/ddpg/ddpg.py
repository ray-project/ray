import logging
from typing import List, Optional, Type

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.simple_q.simple_q import SimpleQ, SimpleQConfig
from ray.rllib.algorithms.ddpg.ddpg_tf_policy import DDPGTFPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import AlgorithmConfigDict
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.deprecation import Deprecated

logger = logging.getLogger(__name__)


class DDPGConfig(SimpleQConfig):
    """Defines a configuration class from which a DDPG Trainer can be built.

    Example:
        >>> from ray.rllib.algorithms.ddpg.ddpg import DDPGConfig
        >>> config = DDPGConfig().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run one training iteration.
        >>> trainer = config.build(env="Pendulum-v1")
        >>> trainer.train()

    Example:
        >>> from ray.rllib.algorithms.ddpg.ddpg import DDPGConfig
        >>> from ray import tune
        >>> config = DDPGConfig()
        >>> # Print out some default values.
        >>> print(config.lr) # doctest: +SKIP
        0.0004
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's env.
        >>> config.environment(env="Pendulum-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "DDPG",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self, algo_class=None):
        """Initializes a DDPGConfig instance."""
        super().__init__(algo_class=algo_class or DDPG)

        # fmt: off
        # __sphinx_doc_begin__
        # DDPG-specific settings.
        self.twin_q = False
        self.policy_delay = 1
        self.smooth_target_policy = False
        self.target_noise = 0.2
        self.target_noise_clip = 0.5

        self.use_state_preprocessor = False
        self.actor_hiddens = [400, 300]
        self.actor_hidden_activation = "relu"
        self.critic_hiddens = [400, 300]
        self.critic_hidden_activation = "relu"
        self.n_step = 1

        self.training_intensity = None
        self.critic_lr = 1e-3
        self.actor_lr = 1e-3
        self.tau = 0.002
        self.use_huber = False
        self.huber_threshold = 1.0
        self.l2_reg = 1e-6

        # Override some of SimpleQ's default values with DDPG-specific values.
        # .exploration()
        self.exploration_config = {
            # DDPG uses OrnsteinUhlenbeck (stateful) noise to be added to NN-output
            # actions (after a possible pure random phase of n timesteps).
            "type": "OrnsteinUhlenbeckNoise",
            # For how many timesteps should we return completely random actions,
            # before we start adding (scaled) noise?
            "random_timesteps": 1000,
            # The OU-base scaling factor to always apply to action-added noise.
            "ou_base_scale": 0.1,
            # The OU theta param.
            "ou_theta": 0.15,
            # The OU sigma param.
            "ou_sigma": 0.2,
            # The initial noise scaling factor.
            "initial_scale": 1.0,
            # The final noise scaling factor.
            "final_scale": 0.02,
            # Timesteps over which to anneal scale (from initial to final values).
            "scale_timesteps": 10000,
        }

        # Common DDPG buffer parameters.
        self.replay_buffer_config = {
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity": 50000,
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization, for example: MultiAgentPrioritizedReplayBuffer.
            "prioritized_replay": DEPRECATED_VALUE,
            # Alpha parameter for prioritized replay buffer.
            "prioritized_replay_alpha": 0.6,
            # Beta parameter for sampling from prioritized replay buffer.
            "prioritized_replay_beta": 0.4,
            # Epsilon to add to the TD errors when updating priorities.
            "prioritized_replay_eps": 1e-6,
            # How many steps of the model to sample before learning starts.
            "learning_starts": 1500,
            # Whether to compute priorities on workers.
            "worker_side_prioritization": False,
        }

        # .training()
        self.grad_clip = None
        self.train_batch_size = 256
        self.target_network_update_freq = 0

        # .rollouts()
        self.rollout_fragment_length = 1
        self.compress_observations = False

        # __sphinx_doc_end__
        # fmt: on

        # Deprecated.
        self.worker_side_prioritization = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        twin_q: Optional[bool] = None,
        policy_delay: Optional[int] = None,
        smooth_target_policy: Optional[bool] = None,
        target_noise: Optional[bool] = None,
        target_noise_clip: Optional[float] = None,
        use_state_preprocessor: Optional[bool] = None,
        actor_hiddens: Optional[List[int]] = None,
        actor_hidden_activation: Optional[str] = None,
        critic_hiddens: Optional[List[int]] = None,
        critic_hidden_activation: Optional[str] = None,
        n_step: Optional[int] = None,
        critic_lr: Optional[float] = None,
        actor_lr: Optional[float] = None,
        tau: Optional[float] = None,
        use_huber: Optional[bool] = None,
        huber_threshold: Optional[float] = None,
        l2_reg: Optional[float] = None,
        training_intensity: Optional[float] = None,
        **kwargs,
    ) -> "DDPGConfig":
        """Sets the training related configuration.

        === Twin Delayed DDPG (TD3) and Soft Actor-Critic (SAC) tricks ===
        TD3: https://spinningup.openai.com/en/latest/algorithms/td3.html
        In addition to settings below, you can use "exploration_noise_type" and
        "exploration_gauss_act_noise" to get IID Gaussian exploration noise
        instead of OrnsteinUhlenbeck exploration noise.

        Args:
            twin_q: Use twin Q-net.
            policy_delay: Delayed policy update.
            smooth_target_policy: Target policy smoothing (this also replaces
                OrnsteinUhlenbeck exploration noise with IID Gaussian exploration
                noise, for now).
            target_noise: Gaussian stddev of target action noise for smoothing.
            target_noise_clip: Target noise limit (bound).
            use_state_preprocessor: Apply a state preprocessor with spec given by the
                "model" config option
                (like other RL algorithms). This is mostly useful if you have a weird
                observation shape, like an image. Disabled by default.
            actor_hiddens: Postprocess the policy network model output with these
                hidden layers. If use_state_preprocessor is False, then these will
                be the *only* hidden layers in the network.
            actor_hidden_activation: Hidden layers activation of the postprocessing
                stage of the policy network
            critic_hiddens: Postprocess the critic network model output with these
                hidden layers; again, if use_state_preprocessor is True, then the
                state will be preprocessed by the model specified with the "model"
                config option first.
            critic_hidden_activation: Hidden layers activation of the postprocessing
                state of the critic.
            n_step: N-step Q learning
            critic_lr: Learning rate for the critic (Q-function) optimizer.
            actor_lr: Learning rate for the actor (policy) optimizer.
            tau: Update the target by \tau * policy + (1-\tau) * target_policy
            use_huber: Conventionally, no need to clip gradients if using a huber loss
            huber_threshold: Threshold of a huber loss
            l2_reg: Weights for L2 regularization
            training_intensity: The intensity with which to update the model
                (vs collecting samples from
                the env). If None, uses the "natural" value of:
                `train_batch_size` / (`rollout_fragment_length` x `num_workers` x
                `num_envs_per_worker`).
                If provided, will make sure that the ratio between ts inserted into and
                sampled from the buffer matches the given value.
                Example:
                    training_intensity=1000.0
                    train_batch_size=250 rollout_fragment_length=1
                    num_workers=1 (or 0) num_envs_per_worker=1
                    -> natural value = 250 / 1 = 250.0
                    -> will make sure that replay+train op will be executed 4x as
                    often as rollout+insert op (4 * 250 = 1000).
                See: rllib/algorithms/dqn/dqn.py::calculate_rr_weights for further
                details.

        Returns:
            This updated DDPGConfig object.
        """
        super().training(**kwargs)

        if twin_q is not None:
            self.twin_q = twin_q
        if policy_delay is not None:
            self.policy_delay = policy_delay
        if smooth_target_policy is not None:
            self.smooth_target_policy = smooth_target_policy
        if target_noise is not None:
            self.target_noise = target_noise
        if target_noise_clip is not None:
            self.target_noise_clip = target_noise_clip
        if use_state_preprocessor is not None:
            self.use_state_preprocessor = use_state_preprocessor
        if actor_hiddens is not None:
            self.actor_hiddens = actor_hiddens
        if actor_hidden_activation is not None:
            self.actor_hidden_activation = actor_hidden_activation
        if critic_hiddens is not None:
            self.critic_hiddens = critic_hiddens
        if critic_hidden_activation is not None:
            self.critic_hidden_activation = critic_hidden_activation
        if n_step is not None:
            self.n_step = n_step
        if critic_lr is not None:
            self.critic_lr = critic_lr
        if actor_lr is not None:
            self.actor_lr = actor_lr
        if tau is not None:
            self.tau = tau
        if use_huber is not None:
            self.use_huber = use_huber
        if huber_threshold is not None:
            self.huber_threshold = huber_threshold
        if l2_reg is not None:
            self.l2_reg = l2_reg
        if training_intensity is not None:
            self.training_intensity = training_intensity

        return self


class DDPG(SimpleQ):
    @classmethod
    @override(SimpleQ)
    # TODO make this return a AlgorithmConfig
    def get_default_config(cls) -> AlgorithmConfigDict:
        return DDPGConfig().to_dict()

    @override(SimpleQ)
    def get_default_policy_class(self, config: AlgorithmConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.ddpg.ddpg_torch_policy import DDPGTorchPolicy

            return DDPGTorchPolicy
        else:
            return DDPGTFPolicy

    @override(SimpleQ)
    def validate_config(self, config: AlgorithmConfigDict) -> None:

        # Call super's validation method.
        super().validate_config(config)

        if config["model"]["custom_model"]:
            logger.warning(
                "Setting use_state_preprocessor=True since a custom model "
                "was specified."
            )
            config["use_state_preprocessor"] = True

        if config["grad_clip"] is not None and config["grad_clip"] <= 0.0:
            raise ValueError("`grad_clip` value must be > 0.0!")

        if config["exploration_config"]["type"] == "ParameterNoise":
            if config["batch_mode"] != "complete_episodes":
                logger.warning(
                    "ParameterNoise Exploration requires `batch_mode` to be "
                    "'complete_episodes'. Setting "
                    "batch_mode=complete_episodes."
                )
                config["batch_mode"] = "complete_episodes"


# Deprecated: Use ray.rllib.algorithms.ddpg.DDPGConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(DDPGConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.ddpg.ddpg::DEFAULT_CONFIG",
        new="ray.rllib.algorithms.ddpg.ddpg.DDPGConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
