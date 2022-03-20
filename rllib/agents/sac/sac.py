import logging
from typing import Type

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.dqn.dqn import DQNTrainer
from ray.rllib.agents.sac.sac_tf_policy import SACTFPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, deprecation_warning
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

# fmt: off
# __sphinx_doc_begin__

# Adds the following updates to the (base) `Trainer` config in
# rllib/agents/trainer.py (`COMMON_CONFIG` dict).
DEFAULT_CONFIG = with_common_config({
    # === Model ===
    # Use two Q-networks (instead of one) for action-value estimation.
    # Note: Each Q-network will have its own target network.
    "twin_q": True,
    # Use a e.g. conv2D state preprocessing network before concatenating the
    # resulting (feature) vector with the action input for the input to
    # the Q-networks.
    "use_state_preprocessor": DEPRECATED_VALUE,
    # Model options for the Q network(s). These will override MODEL_DEFAULTS.
    # The `Q_model` dict is treated just as the top-level `model` dict in
    # setting up the Q-network(s) (2 if twin_q=True).
    # That means, you can do for different observation spaces:
    # obs=Box(1D) -> Tuple(Box(1D) + Action) -> concat -> post_fcnet
    # obs=Box(3D) -> Tuple(Box(3D) + Action) -> vision-net -> concat w/ action
    #   -> post_fcnet
    # obs=Tuple(Box(1D), Box(3D)) -> Tuple(Box(1D), Box(3D), Action)
    #   -> vision-net -> concat w/ Box(1D) and action -> post_fcnet
    # You can also have SAC use your custom_model as Q-model(s), by simply
    # specifying the `custom_model` sub-key in below dict (just like you would
    # do in the top-level `model` dict.
    "Q_model": {
        "fcnet_hiddens": [256, 256],
        "fcnet_activation": "relu",
        "post_fcnet_hiddens": [],
        "post_fcnet_activation": None,
        "custom_model": None,  # Use this to define custom Q-model(s).
        "custom_model_config": {},
    },
    # Model options for the policy function (see `Q_model` above for details).
    # The difference to `Q_model` above is that no action concat'ing is
    # performed before the post_fcnet stack.
    "policy_model": {
        "fcnet_hiddens": [256, 256],
        "fcnet_activation": "relu",
        "post_fcnet_hiddens": [],
        "post_fcnet_activation": None,
        "custom_model": None,  # Use this to define a custom policy model.
        "custom_model_config": {},
    },
    # Actions are already normalized, no need to clip them further.
    "clip_actions": False,

    # === Learning ===
    # Update the target by \tau * policy + (1-\tau) * target_policy.
    "tau": 5e-3,
    # Initial value to use for the entropy weight alpha.
    "initial_alpha": 1.0,
    # Target entropy lower bound. If "auto", will be set to -|A| (e.g. -2.0 for
    # Discrete(2), -3.0 for Box(shape=(3,))).
    # This is the inverse of reward scale, and will be optimized automatically.
    "target_entropy": "auto",
    # N-step target updates. If >1, sars' tuples in trajectories will be
    # postprocessed to become sa[discounted sum of R][s t+n] tuples.
    "n_step": 1,
    # Number of env steps to optimize for before returning.
    "timesteps_per_iteration": 100,

    # === Replay buffer ===
    # Size of the replay buffer (in time steps).
    "buffer_size": DEPRECATED_VALUE,
    "replay_buffer_config": {
        "type": "MultiAgentReplayBuffer",
        "capacity": int(1e6),
    },
    # Set this to True, if you want the contents of your buffer(s) to be
    # stored in any saved checkpoints as well.
    # Warnings will be created if:
    # - This is True AND restoring from a checkpoint that contains no buffer
    #   data.
    # - This is False AND restoring from a checkpoint that does contain
    #   buffer data.
    "store_buffer_in_checkpoints": False,
    # If True prioritized replay buffer will be used.
    "prioritized_replay": False,
    "prioritized_replay_alpha": 0.6,
    "prioritized_replay_beta": 0.4,
    "prioritized_replay_eps": 1e-6,
    # Whether to LZ4 compress observations
    "compress_observations": False,

    # The intensity with which to update the model (vs collecting samples from
    # the env). If None, uses the "natural" value of:
    # `train_batch_size` / (`rollout_fragment_length` x `num_workers` x
    # `num_envs_per_worker`).
    # If provided, will make sure that the ratio between ts inserted into and
    # sampled from the buffer matches the given value.
    # Example:
    #   training_intensity=1000.0
    #   train_batch_size=250 rollout_fragment_length=1
    #   num_workers=1 (or 0) num_envs_per_worker=1
    #   -> natural value = 250 / 1 = 250.0
    #   -> will make sure that replay+train op will be executed 4x as
    #      often as rollout+insert op (4 * 250 = 1000).
    # See: rllib/agents/dqn/dqn.py::calculate_rr_weights for further details.
    "training_intensity": None,

    # === Optimization ===
    "optimization": {
        "actor_learning_rate": 3e-4,
        "critic_learning_rate": 3e-4,
        "entropy_learning_rate": 3e-4,
    },
    # If not None, clip gradients during optimization at this value.
    "grad_clip": None,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1500,
    # Update the replay buffer with this many samples at once. Note that this
    # setting applies per-worker if num_workers > 1.
    "rollout_fragment_length": 1,
    # Size of a batched sampled from replay buffer for training.
    "train_batch_size": 256,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 0,

    # === Parallelism ===
    # Whether to use a GPU for local optimization.
    "num_gpus": 0,
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Whether to allocate GPUs for workers (if > 0).
    "num_gpus_per_worker": 0,
    # Whether to allocate CPUs for workers (if > 0).
    "num_cpus_per_worker": 1,
    # Whether to compute priorities on workers.
    "worker_side_prioritization": False,
    # Prevent reporting frequency from going lower than this time span.
    "min_time_s_per_reporting": 1,

    # Whether the loss should be calculated deterministically (w/o the
    # stochastic action sampling step). True only useful for cont. actions and
    # for debugging!
    "_deterministic_loss": False,
    # Use a Beta-distribution instead of a SquashedGaussian for bounded,
    # continuous action spaces (not recommended, for debugging only).
    "_use_beta_distribution": False,
})
# __sphinx_doc_end__
# fmt: on


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
        return DEFAULT_CONFIG

    @override(DQNTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
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
