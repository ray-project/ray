import logging

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.dqn.dqn_policy import DQNTFPolicy
from ray.rllib.agents.dqn.simple_q_policy import SimpleQPolicy
from ray.rllib.optimizers import SyncReplayOptimizer
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.deprecation import deprecation_warning, DEPRECATED_VALUE
from ray.rllib.utils.exploration import PerWorkerEpsilonGreedy

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
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
    # Whether to use double dqn
    "double_q": True,
    # Postprocess model outputs with these hidden layers to compute the
    # state and action values. See also the model config in catalog.py.
    "hiddens": [256],
    # N-step Q learning
    "n_step": 1,

    # === Exploration Settings (Experimental) ===
    "exploration_config": {
        # The Exploration class to use.
        "type": "EpsilonGreedy",
        # Config for the Exploration class' constructor:
        "initial_epsilon": 1.0,
        "final_epsilon": 0.02,
        "epsilon_timesteps": 10000,  # Timesteps over which to anneal epsilon.

        # For soft_q, use:
        # "exploration_config" = {
        #   "type": "SoftQ"
        #   "temperature": [float, e.g. 1.0]
        # }
    },
    # Switch to greedy actions in evaluation workers.
    "evaluation_config": {
        "explore": False,
    },

    # TODO(sven): Make Exploration class for parameter noise.
    # If True parameter space noise will be used for exploration
    # See https://blog.openai.com/better-exploration-with-parameter-noise/
    "parameter_noise": False,

    # Minimum env steps to optimize for per train call. This value does
    # not affect learning, only the length of iterations.
    "timesteps_per_iteration": 1000,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 500,
    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": 50000,
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
    # Whether to LZ4 compress observations
    "compress_observations": True,

    # === Optimization ===
    # Learning rate for adam optimizer
    "lr": 5e-4,
    # Learning rate schedule
    "lr_schedule": None,
    # Adam epsilon hyper parameter
    "adam_epsilon": 1e-8,
    # If not None, clip gradients during optimization at this value
    "grad_norm_clipping": 40,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1000,
    # Update the replay buffer with this many samples at once. Note that
    # this setting applies per-worker if num_workers > 1.
    "sample_batch_size": 4,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 32,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Whether to compute priorities on workers.
    "worker_side_prioritization": False,
    # Prevent iterations from going lower than this time span
    "min_iter_time_s": 1,

    # DEPRECATED VALUES (set to -1 to indicate they have not been overwritten
    # by user's config). If we don't set them here, we will get an error
    # from the config-key checker.
    "schedule_max_timesteps": DEPRECATED_VALUE,
    "exploration_final_eps": DEPRECATED_VALUE,
    "exploration_fraction": DEPRECATED_VALUE,
    "beta_annealing_fraction": DEPRECATED_VALUE,
    "per_worker_exploration": DEPRECATED_VALUE,
    "softmax_temp": DEPRECATED_VALUE,
    "soft_q": DEPRECATED_VALUE,
})
# __sphinx_doc_end__
# yapf: enable


def make_policy_optimizer(workers, config):
    """Create the single process DQN policy optimizer.

    Returns:
        SyncReplayOptimizer: Used for generic off-policy Trainers.
    """
    return SyncReplayOptimizer(
        workers,
        # TODO(sven): Move all PR-beta decays into Schedule components.
        learning_starts=config["learning_starts"],
        buffer_size=config["buffer_size"],
        prioritized_replay=config["prioritized_replay"],
        prioritized_replay_alpha=config["prioritized_replay_alpha"],
        prioritized_replay_beta=config["prioritized_replay_beta"],
        prioritized_replay_beta_annealing_timesteps=config[
            "prioritized_replay_beta_annealing_timesteps"],
        final_prioritized_replay_beta=config["final_prioritized_replay_beta"],
        prioritized_replay_eps=config["prioritized_replay_eps"],
        train_batch_size=config["train_batch_size"],
        **config["optimizer"])


def validate_config_and_setup_param_noise(config):
    """Checks and updates the config based on settings.

    Rewrites sample_batch_size to take into account n_step truncation.
    """
    # PyTorch check.
    if config["use_pytorch"]:
        raise ValueError("DQN does not support PyTorch yet! Use tf instead.")

    # TODO(sven): Remove at some point.
    #  Backward compatibility of epsilon-exploration config AND beta-annealing
    # fraction settings (both based on schedule_max_timesteps, which is
    # deprecated).
    schedule_max_timesteps = None
    if config.get("schedule_max_timesteps", DEPRECATED_VALUE) != \
            DEPRECATED_VALUE:
        deprecation_warning(
            "schedule_max_timesteps",
            "exploration_config.epsilon_timesteps AND "
            "prioritized_replay_beta_annealing_timesteps")
        schedule_max_timesteps = config["schedule_max_timesteps"]
    if config.get("exploration_final_eps", DEPRECATED_VALUE) != \
            DEPRECATED_VALUE:
        deprecation_warning("exploration_final_eps",
                            "exploration_config.final_epsilon")
        if isinstance(config["exploration_config"], dict):
            config["exploration_config"]["final_epsilon"] = \
                config.pop("exploration_final_eps")
    if config.get("exploration_fraction", DEPRECATED_VALUE) != \
            DEPRECATED_VALUE:
        assert schedule_max_timesteps is not None
        deprecation_warning("exploration_fraction",
                            "exploration_config.epsilon_timesteps")
        if isinstance(config["exploration_config"], dict):
            config["exploration_config"]["epsilon_timesteps"] = config.pop(
                "exploration_fraction") * schedule_max_timesteps
    if config.get("beta_annealing_fraction", DEPRECATED_VALUE) != \
            DEPRECATED_VALUE:
        assert schedule_max_timesteps is not None
        deprecation_warning(
            "beta_annealing_fraction (decimal)",
            "prioritized_replay_beta_annealing_timesteps (int)")
        config["prioritized_replay_beta_annealing_timesteps"] = config.pop(
            "beta_annealing_fraction") * schedule_max_timesteps
    if config.get("per_worker_exploration", DEPRECATED_VALUE) != \
            DEPRECATED_VALUE:
        deprecation_warning("per_worker_exploration",
                            "exploration_config.type=PerWorkerEpsilonGreedy")
        if isinstance(config["exploration_config"], dict):
            config["exploration_config"]["type"] = PerWorkerEpsilonGreedy
    if config.get("softmax_temp", DEPRECATED_VALUE) != DEPRECATED_VALUE:
        deprecation_warning(
            "soft_q", "exploration_config={"
            "type=StochasticSampling, temperature=[float]"
            "}")
        if config.get("softmax_temp", 1.0) < 0.00001:
            logger.warning("softmax temp very low: Clipped it to 0.00001.")
            config["softmax_temperature"] = 0.00001
    if config.get("soft_q", DEPRECATED_VALUE) != DEPRECATED_VALUE:
        deprecation_warning(
            "soft_q", "exploration_config={"
            "type=SoftQ, temperature=[float]"
            "}")
        config["exploration_config"] = {
            "type": "SoftQ",
            "temperature": config.get("softmax_temp", 1.0)
        }

    # Update effective batch size to include n-step
    adjusted_batch_size = max(config["sample_batch_size"],
                              config.get("n_step", 1))
    config["sample_batch_size"] = adjusted_batch_size

    # Setup parameter noise.
    if config.get("parameter_noise", False):
        if config["batch_mode"] != "complete_episodes":
            raise ValueError("Exploration with parameter space noise requires "
                             "batch_mode to be complete_episodes.")
        if config.get("noisy", False):
            raise ValueError("Exploration with parameter space noise and "
                             "noisy network cannot be used at the same time.")

        start_callback = config["callbacks"].get("on_episode_start")

        def on_episode_start(info):
            # as a callback function to sample and pose parameter space
            # noise on the parameters of network
            policies = info["policy"]
            for pi in policies.values():
                pi.add_parameter_noise()
            if start_callback is not None:
                start_callback(info)

        config["callbacks"]["on_episode_start"] = on_episode_start

        end_callback = config["callbacks"].get("on_episode_end")

        def on_episode_end(info):
            # as a callback function to monitor the distance
            # between noisy policy and original policy
            policies = info["policy"]
            episode = info["episode"]
            model = policies[DEFAULT_POLICY_ID].model
            if hasattr(model, "pi_distance"):
                episode.custom_metrics["policy_distance"] = model.pi_distance
            if end_callback is not None:
                end_callback(info)

        config["callbacks"]["on_episode_end"] = on_episode_end


def get_initial_state(config):
    return {
        "last_target_update_ts": 0,
        "num_target_updates": 0,
    }


# TODO(sven): Move this to generic Trainer/Policy. Every Algo should do this.
def update_worker_exploration(trainer):
    """Sets epsilon exploration values in all policies to updated values.

    According to current time-step.

    Args:
        trainer (Trainer): The Trainer object for the DQN.
    """
    # Store some data for metrics after learning.
    global_timestep = trainer.optimizer.num_steps_sampled
    trainer.train_start_timestep = global_timestep

    # Get all current exploration-infos (from Policies, which cache this info).
    trainer.exploration_infos = trainer.workers.foreach_trainable_policy(
        lambda p, _: p.get_exploration_info())


def after_train_result(trainer, result):
    """Add some DQN specific metrics to results."""
    global_timestep = trainer.optimizer.num_steps_sampled
    result.update(
        timesteps_this_iter=global_timestep - trainer.train_start_timestep,
        info=dict({
            "exploration_infos": trainer.exploration_infos,
            "num_target_updates": trainer.state["num_target_updates"],
        }, **trainer.optimizer.stats()))


def update_target_if_needed(trainer, fetches):
    """Update the target network in configured intervals."""
    global_timestep = trainer.optimizer.num_steps_sampled
    if global_timestep - trainer.state["last_target_update_ts"] > \
            trainer.config["target_network_update_freq"]:
        trainer.workers.local_worker().foreach_trainable_policy(
            lambda p, _: p.update_target())
        trainer.state["last_target_update_ts"] = global_timestep
        trainer.state["num_target_updates"] += 1


GenericOffPolicyTrainer = build_trainer(
    name="GenericOffPolicyAlgorithm",
    default_policy=None,
    default_config=DEFAULT_CONFIG,
    validate_config=validate_config_and_setup_param_noise,
    get_initial_state=get_initial_state,
    make_policy_optimizer=make_policy_optimizer,
    before_train_step=update_worker_exploration,
    after_optimizer_step=update_target_if_needed,
    after_train_result=after_train_result)

DQNTrainer = GenericOffPolicyTrainer.with_updates(
    name="DQN", default_policy=DQNTFPolicy, default_config=DEFAULT_CONFIG)

SimpleQTrainer = DQNTrainer.with_updates(default_policy=SimpleQPolicy)
