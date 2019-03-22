from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

from ray import tune
from ray.rllib import optimizers
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.agents.dqn.dqn_policy_graph import DQNPolicyGraph
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.utils.annotations import override
from ray.rllib.utils.schedules import ConstantSchedule, LinearSchedule

logger = logging.getLogger(__name__)

OPTIMIZER_SHARED_CONFIGS = [
    "buffer_size", "prioritized_replay", "prioritized_replay_alpha",
    "prioritized_replay_beta", "schedule_max_timesteps",
    "beta_annealing_fraction", "final_prioritized_replay_beta",
    "prioritized_replay_eps", "sample_batch_size", "train_batch_size",
    "learning_starts"
]

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

    # === Evaluation ===
    # Evaluate with epsilon=0 every `evaluation_interval` training iterations.
    # The evaluation stats will be reported under the "evaluation" metric key.
    # Note that evaluation is currently not parallelized, and that for Ape-X
    # metrics are already only reported for the lowest epsilon workers.
    "evaluation_interval": None,
    # Number of episodes to run per evaluation period.
    "evaluation_num_episodes": 10,

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
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 500,
    # Use softmax for sampling actions. Required for off policy estimation.
    "soft_q": False,
    # Softmax temperature. Q values are divided by this value prior to softmax.
    # Softmax approaches argmax as the temperature drops to zero.
    "softmax_temp": 1.0,
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
    # Fraction of entire training period over which the beta parameter is
    # annealed
    "beta_annealing_fraction": 0.2,
    # Final value of beta
    "final_prioritized_replay_beta": 0.4,
    # Epsilon to add to the TD errors when updating priorities.
    "prioritized_replay_eps": 1e-6,
    # Whether to LZ4 compress observations
    "compress_observations": True,

    # === Optimization ===
    # Learning rate for adam optimizer
    "lr": 5e-4,
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


class DQNAgent(Agent):
    """DQN implementation in TensorFlow."""

    _agent_name = "DQN"
    _default_config = DEFAULT_CONFIG
    _policy_graph = DQNPolicyGraph
    _optimizer_shared_configs = OPTIMIZER_SHARED_CONFIGS

    @override(Agent)
    def _init(self):
        self._validate_config()

        # Update effective batch size to include n-step
        adjusted_batch_size = max(self.config["sample_batch_size"],
                                  self.config.get("n_step", 1))
        self.config["sample_batch_size"] = adjusted_batch_size

        self.exploration0 = self._make_exploration_schedule(-1)
        self.explorations = [
            self._make_exploration_schedule(i)
            for i in range(self.config["num_workers"])
        ]

        for k in self._optimizer_shared_configs:
            if self._agent_name != "DQN" and k in [
                    "schedule_max_timesteps", "beta_annealing_fraction",
                    "final_prioritized_replay_beta"
            ]:
                # only Rainbow needs annealing prioritized_replay_beta
                continue
            if k not in self.config["optimizer"]:
                self.config["optimizer"][k] = self.config[k]

        if self.config.get("parameter_noise", False):
            if self.config["callbacks"]["on_episode_start"]:
                start_callback = self.config["callbacks"]["on_episode_start"]
            else:
                start_callback = None

            def on_episode_start(info):
                # as a callback function to sample and pose parameter space
                # noise on the parameters of network
                policies = info["policy"]
                for pi in policies.values():
                    pi.add_parameter_noise()
                if start_callback:
                    start_callback(info)

            self.config["callbacks"]["on_episode_start"] = tune.function(
                on_episode_start)
            if self.config["callbacks"]["on_episode_end"]:
                end_callback = self.config["callbacks"]["on_episode_end"]
            else:
                end_callback = None

            def on_episode_end(info):
                # as a callback function to monitor the distance
                # between noisy policy and original policy
                policies = info["policy"]
                episode = info["episode"]
                episode.custom_metrics["policy_distance"] = policies[
                    "default"].pi_distance
                if end_callback:
                    end_callback(info)

            self.config["callbacks"]["on_episode_end"] = tune.function(
                on_episode_end)

        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, self._policy_graph)

        if self.config["evaluation_interval"]:
            self.evaluation_ev = self.make_local_evaluator(
                self.env_creator,
                self._policy_graph,
                extra_config={
                    "batch_mode": "complete_episodes",
                    "batch_steps": 1,
                })
            self.evaluation_metrics = self._evaluate()

        def create_remote_evaluators():
            return self.make_remote_evaluators(self.env_creator,
                                               self._policy_graph,
                                               self.config["num_workers"])

        if self.config["optimizer_class"] != "AsyncReplayOptimizer":
            self.remote_evaluators = create_remote_evaluators()
        else:
            # Hack to workaround https://github.com/ray-project/ray/issues/2541
            self.remote_evaluators = None

        self.optimizer = getattr(optimizers, self.config["optimizer_class"])(
            self.local_evaluator, self.remote_evaluators,
            self.config["optimizer"])
        # Create the remote evaluators *after* the replay actors
        if self.remote_evaluators is None:
            self.remote_evaluators = create_remote_evaluators()
            self.optimizer._set_evaluators(self.remote_evaluators)

        self.last_target_update_ts = 0
        self.num_target_updates = 0

    @override(Agent)
    def _train(self):
        start_timestep = self.global_timestep

        # Update worker explorations
        exp_vals = [self.exploration0.value(self.global_timestep)]
        self.local_evaluator.foreach_trainable_policy(
            lambda p, _: p.set_epsilon(exp_vals[0]))
        for i, e in enumerate(self.remote_evaluators):
            exp_val = self.explorations[i].value(self.global_timestep)
            e.foreach_trainable_policy.remote(
                lambda p, _: p.set_epsilon(exp_val))
            exp_vals.append(exp_val)

        # Do optimization steps
        start = time.time()
        while (self.global_timestep - start_timestep <
               self.config["timesteps_per_iteration"]
               ) or time.time() - start < self.config["min_iter_time_s"]:
            self.optimizer.step()
            self.update_target_if_needed()

        if self.config["per_worker_exploration"]:
            # Only collect metrics from the third of workers with lowest eps
            result = self.collect_metrics(
                selected_evaluators=self.remote_evaluators[
                    -len(self.remote_evaluators) // 3:])
        else:
            result = self.collect_metrics()

        result.update(
            timesteps_this_iter=self.global_timestep - start_timestep,
            info=dict({
                "min_exploration": min(exp_vals),
                "max_exploration": max(exp_vals),
                "num_target_updates": self.num_target_updates,
            }, **self.optimizer.stats()))

        if self.config["evaluation_interval"]:
            if self.iteration % self.config["evaluation_interval"] == 0:
                self.evaluation_metrics = self._evaluate()
            result.update(self.evaluation_metrics)

        return result

    def update_target_if_needed(self):
        if self.global_timestep - self.last_target_update_ts > \
                self.config["target_network_update_freq"]:
            self.local_evaluator.foreach_trainable_policy(
                lambda p, _: p.update_target())
            self.last_target_update_ts = self.global_timestep
            self.num_target_updates += 1

    @property
    def global_timestep(self):
        return self.optimizer.num_steps_sampled

    def _evaluate(self):
        logger.info("Evaluating current policy for {} episodes".format(
            self.config["evaluation_num_episodes"]))
        self.evaluation_ev.restore(self.local_evaluator.save())
        self.evaluation_ev.foreach_policy(lambda p, _: p.set_epsilon(0))
        for _ in range(self.config["evaluation_num_episodes"]):
            self.evaluation_ev.sample()
        metrics = collect_metrics(self.evaluation_ev)
        return {"evaluation": metrics}

    def _make_exploration_schedule(self, worker_index):
        # Use either a different `eps` per worker, or a linear schedule.
        if self.config["per_worker_exploration"]:
            assert self.config["num_workers"] > 1, \
                "This requires multiple workers"
            if worker_index >= 0:
                exponent = (
                    1 +
                    worker_index / float(self.config["num_workers"] - 1) * 7)
                return ConstantSchedule(0.4**exponent)
            else:
                # local ev should have zero exploration so that eval rollouts
                # run properly
                return ConstantSchedule(0.0)
        return LinearSchedule(
            schedule_timesteps=int(self.config["exploration_fraction"] *
                                   self.config["schedule_max_timesteps"]),
            initial_p=1.0,
            final_p=self.config["exploration_final_eps"])

    def __getstate__(self):
        state = Agent.__getstate__(self)
        state.update({
            "num_target_updates": self.num_target_updates,
            "last_target_update_ts": self.last_target_update_ts,
        })
        return state

    def __setstate__(self, state):
        Agent.__setstate__(self, state)
        self.num_target_updates = state["num_target_updates"]
        self.last_target_update_ts = state["last_target_update_ts"]

    def _validate_config(self):
        if self.config.get("parameter_noise", False):
            if self.config["batch_mode"] != "complete_episodes":
                raise ValueError(
                    "Exploration with parameter space noise requires "
                    "batch_mode to be complete_episodes.")
            if self.config.get("noisy", False):
                raise ValueError(
                    "Exploration with parameter space noise and noisy network "
                    "cannot be used at the same time.")
