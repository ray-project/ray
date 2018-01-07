from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat


@ray.remote
class ReplayActor(object):
    def __init__(self, config):
        self.config = config
        assert config["buffer_size"] > config["learning_starts"]
        if config["prioritized_replay"]:
            self.replay_buffer = PrioritizedReplayBuffer(
                config["buffer_size"],
                alpha=config["prioritized_replay_alpha"])
        else:
            self.replay_buffer = ReplayBuffer(config["buffer_size"])

    def add_batch(self, batch):
        for row in batch.rows():
            self.replay_buffer.add(
                row["obs"], row["actions"], row["rewards"], row["new_obs"],
                row["dones"], row["weights"])

    def sample(self):
        if len(self.replay_buffer) < self.config["learning_starts"]:
            return None

        (obses_t, actions, rewards, obses_tp1,
            dones, weights, batch_indexes) = self.replay_buffer.sample(
                self.config["train_batch_size"],
                beta=self.config["prioritized_replay_beta"])

        return SampleBatch({
            "obs": obses_t, "actions": actions, "rewards": rewards,
            "new_obs": obses_tp1, "dones": dones, "weights": weights,
            "batch_indexes": batch_indexes})

    def update_batch_priorities(self, batch, td_errors):
        new_priorities = (
            np.abs(td_errors) + self.config["prioritized_replay_eps"])
        self.replay_buffer.update_priorities(
            batch["batch_indexes"], new_priorities)


class ApexOptimizer(Optimizer):

    def _init(self):
        assert hasattr(self.local_evaluator, "compute_td_error")
        self.grad_timer = TimerStat()
        self.priorities_timer = TimerStat()
        self.sample_tasks_done = RunningStat(())
        self.replay_actor = ReplayActor.remote(self.config)

        # The pipelined fetch from the replay buffer
        self.next_batch_future = None

        # Mapping of SampleBatch ObjectID -> Evaluator working on the task
        self.sample_tasks = {}

        # Number of worker steps since the last weight update
        self.steps_since_update = {}

        # Number of weight sync RPCs to workers
        self.num_weight_syncs = 0

        # Number of iterations in which we've skipped learning
        self.num_skipped_learning_iters = 0

    def step(self):
        timesteps_this_iter = 0
        weights = ray.put(self.local_evaluator.get_weights())

        # Kick off background operations if needed
        if not self.sample_tasks:
            for ev in self.remote_evaluators:
                ev.set_weights.remote(weights)
                self.num_weight_syncs += 1
                self.steps_since_update[ev] = 0
                self.sample_tasks[ev.sample.remote()] = ev

        if not self.next_batch_future:
            self.next_batch_future = self.replay_actor.sample.remote()

        # Fetch next batch to optimize over
        samples = ray.get(self.next_batch_future)
        self.next_batch_future = self.replay_actor.sample.remote()

        # Process any completed sample requests
        pending = list(self.sample_tasks)
        assert len(pending) == len(self.remote_evaluators)
        ready, _ = ray.wait(pending, num_returns=len(pending), timeout=0)
        print("Num sample tasks ready: ", ready)
        if ready:
            self.sample_tasks_done.push(len(ready))

        for sample_batch in ready:
            ev = self.sample_tasks.pop(sample_batch)
            timesteps_this_iter += self.config["sample_batch_size"]

            # Send the data to the replay buffer
            self.replay_actor.add_batch.remote(sample_batch)

            # Update weights if needed
            self.steps_since_update[ev] += self.config["sample_batch_size"]
            if (self.steps_since_update[ev] >=
                    self.config["max_weight_sync_delay"]):
                ev.set_weights.remote(weights)
                self.steps_since_update[ev] = 0

            # Kick off another sample request
            self.sample_tasks[ev.sample.remote()] = ev

        # Compute gradients if learning has started
        if samples is None:
            self.num_skipped_learning_iters += 1
            return timesteps_this_iter

        with self.grad_timer:
            grad = self.local_evaluator.compute_gradients(samples)
            self.local_evaluator.apply_gradients(grad)
            self.grad_timer.push_units_processed(samples.count)

        with self.priorities_timer:
            td_error = self.local_evaluator.compute_td_error(samples)
            self.replay_actor.update_batch_priorities.remote(samples, td_error)

        return timesteps_this_iter

    def stats(self):
        return {
            "sample_tasks_done": round(float(self.sample_tasks_done.mean), 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "priorities_time_ms": round(1000 * self.priorities_timer.mean, 3),
            "opt_peak_throughput": round(self.grad_timer.mean_throughput, 3),
            "opt_samples": round(self.grad_timer.mean_units_processed, 3),
            "skipped_learning_iters": self.num_skipped_learning_iters,
        }
