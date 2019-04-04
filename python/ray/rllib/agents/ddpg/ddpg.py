from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.agent import with_common_config
from ray.rllib.agents.dqn.dqn import DQNAgent
from ray.rllib.agents.ddpg.ddpg_policy_graph import DDPGPolicyGraph
from ray.rllib.utils.annotations import override
from ray.rllib.utils.schedules import ConstantSchedule, LinearSchedule

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Twin Delayed DDPG (TD3) and Soft Actor-Critic (SAC) tricks ===
    # TD3: https://spinningup.openai.com/en/latest/algorithms/td3.html
    # twin Q-net
    "twin_q": False,
    # delayed policy update
    "policy_delay": 1,
    # target policy smoothing
    # this also forces the use of gaussian instead of OU noise for exploration
    "smooth_target_policy": False,
    # gaussian stddev of act noise
    "act_noise": 0.1,
    # gaussian stddev of target noise
    "target_noise": 0.2,
    # target noise limit (bound)
    "noise_clip": 0.5,

    # === Evaluation ===
    # Evaluate with epsilon=0 every `evaluation_interval` training iterations.
    # The evaluation stats will be reported under the "evaluation" metric key.
    # Note that evaluation is currently not parallelized, and that for Ape-X
    # metrics are already only reported for the lowest epsilon workers.
    "evaluation_interval": None,
    # Number of episodes to run per evaluation period.
    "evaluation_num_episodes": 10,

    # === Model ===
    # Postprocess the policy network model output with these hidden layers
    "actor_hiddens": [64, 64],
    # Hidden layers activation of the policy network
    "actor_hidden_activation": "relu",
    # Postprocess the critic network model output with these hidden layers
    "critic_hiddens": [64, 64],
    # Hidden layers activation of the critic network
    "critic_hidden_activation": "relu",
    # N-step Q learning
    "n_step": 1,

    # === Exploration ===
    # Max num timesteps for annealing schedules. Exploration is annealed from
    # 1.0 to exploration_fraction over this number of timesteps scaled by
    # exploration_fraction
    "schedule_max_timesteps": 100000,
    # Number of env steps to optimize for before returning
    "timesteps_per_iteration": 1000,
    # Fraction of entire training period over which the exploration rate is
    # annealed
    "exploration_fraction": 0.1,
    # Final value of random action probability
    "exploration_final_eps": 0.02,
    # OU-noise scale
    "noise_scale": 0.1,
    # theta
    "exploration_theta": 0.15,
    # sigma
    "exploration_sigma": 0.2,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 0,
    # Update the target by \tau * policy + (1-\tau) * target_policy
    "tau": 0.002,
    # If True parameter space noise will be used for exploration
    # See https://blog.openai.com/better-exploration-with-parameter-noise/
    "parameter_noise": False,

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
    # Epsilon to add to the TD errors when updating priorities.
    "prioritized_replay_eps": 1e-6,
    # Whether to LZ4 compress observations
    "compress_observations": False,

    # === Optimization ===
    # Learning rate for adam optimizer.
    # Instead of using two optimizers, we use two different loss coefficients
    "lr": 1e-3,
    "actor_loss_coeff": 0.1,
    "critic_loss_coeff": 1.0,
    # If True, use huber loss instead of squared loss for critic network
    # Conventionally, no need to clip gradients if using a huber loss
    "use_huber": False,
    # Threshold of a huber loss
    "huber_threshold": 1.0,
    # Weights for L2 regularization
    "l2_reg": 1e-6,
    # If not None, clip gradients during optimization at this value
    "grad_norm_clipping": None,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1500,
    # Update the replay buffer with this many samples at once. Note that this
    # setting applies per-worker if num_workers > 1.
    "sample_batch_size": 1,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 256,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Optimizer class to use.
    "optimizer_class": "SyncReplayOptimizer",
    # Whether to use a distribution of epsilons across workers for exploration.
    "per_worker_exploration": False,
    # Whether to compute priorities on workers.
    "worker_side_prioritization": False,
    # Prevent iterations from going lower than this time span
    "min_iter_time_s": 1,
})
# __sphinx_doc_end__
# yapf: enable


class DDPGAgent(DQNAgent):
    """DDPG implementation in TensorFlow."""
    _agent_name = "DDPG"
    _default_config = DEFAULT_CONFIG
    _policy_graph = DDPGPolicyGraph

    @override(DQNAgent)
    def _make_exploration_schedule(self, worker_index):
        # Override DQN's schedule to take into account `noise_scale`
        if self.config["per_worker_exploration"]:
            assert self.config["num_workers"] > 1, \
                "This requires multiple workers"
            if worker_index >= 0:
                exponent = (
                    1 +
                    worker_index / float(self.config["num_workers"] - 1) * 7)
                return ConstantSchedule(
                    self.config["noise_scale"] * 0.4**exponent)
            else:
                # local ev should have zero exploration so that eval rollouts
                # run properly
                return ConstantSchedule(0.0)
        else:
            return LinearSchedule(
                schedule_timesteps=int(self.config["exploration_fraction"] *
                                       self.config["schedule_max_timesteps"]),
                initial_p=self.config["noise_scale"] * 1.0,
                final_p=self.config["noise_scale"] *
                self.config["exploration_final_eps"])
