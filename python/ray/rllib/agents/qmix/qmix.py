from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib import optimizers
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.agents.qmix.qmix_policy_graph import QMixPolicyGraph
from ray.rllib.utils.annotations import override
from ray.rllib.utils.schedules import LinearSchedule

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    "num_workers": 0,
    "double_q": False,
    "grad_norm_clip": 10.0,
    "lr": 0.0005,
    "optim_alpha": 0.99,
    "optim_eps": 0.00001,
    "timesteps_per_iteration": 1000,
    "min_iter_time_s": 5,
    "sample_batch_size": 32,
    "train_batch_size": 32,
    "optimizer_class": "SyncBatchReplayOptimizer",
    "optimizer": {
        "buffer_size": 1024,
    },

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
})
# __sphinx_doc_end__
# yapf: enable


class QMixAgent(Agent):
    """QMix implementation in PyTorch."""

    _agent_name = "QMIX"
    _default_config = DEFAULT_CONFIG
    _policy_graph = QMixPolicyGraph

    @override(Agent)
    def _init(self):
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, self._policy_graph)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, self._policy_graph, self.config["num_workers"])
        self.optimizer = getattr(optimizers, self.config["optimizer_class"])(
            self.local_evaluator, self.remote_evaluators,
            self.config["optimizer"])
        self.last_target_update_ts = 0
        self.num_target_updates = 0
        self.exploration0 = self._make_exploration_schedule(-1)
        self.explorations = [
            self._make_exploration_schedule(i)
            for i in range(self.config["num_workers"])
        ]

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

        result = self.optimizer.collect_metrics(
            timeout_seconds=self.config["collect_metrics_timeout"])

        result.update(
            timesteps_this_iter=self.global_timestep - start_timestep,
            info=dict({
                "num_target_updates": self.num_target_updates,
            }, **self.optimizer.stats()))
        result.update(
            timesteps_this_iter=self.global_timestep - start_timestep,
            info=dict({
                "min_exploration": min(exp_vals),
                "max_exploration": max(exp_vals),
                "num_target_updates": self.num_target_updates,
            }, **self.optimizer.stats()))
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

    def _make_exploration_schedule(self, worker_index):
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


if __name__ == "__main__":
    import ray
    from ray.rllib.agents.qmix.twostep_game import TwoStepGame
    from ray.tune import run_experiments

    ray.init()
    run_experiments({
        "two_step": {
            "run": "QMIX",
            "env": TwoStepGame,
            "config": {
            },
        }
    })
