from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import random

import ray
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.timer import TimerStat


class TaskPool(object):
    def __init__(self):
        self._tasks = {}
        self._completed = []

    def add(self, worker, obj_id):
        self._tasks[obj_id] = worker

    def completed(self):
        pending = list(self._tasks)
        ready, _ = ray.wait(pending, num_returns=len(pending), timeout=10)
        for obj_id in ready:
            yield (self._tasks.pop(obj_id), obj_id)

    def wait_one(self):
        if not self._completed:
            pending = list(self._tasks)
            for worker, obj_id in self.completed():
                self._completed.append((worker, obj_id))
        if self._completed:
            return self._completed.pop(0)
        else:
            [obj_id], _ = ray.wait(pending, num_returns=1)
            return (self._tasks.pop(obj_id), obj_id)

    @property
    def count(self):
        return len(self._tasks)


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

    def replay(self):
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
        self.replay_actors = [
            ReplayActor.remote(self.config)
            for _ in range(self.config["num_replay_buffer_shards"])]
        self.grad_evaluators = self.remote_evaluators[
            :self.config["num_gradient_worker_shards"]]
        self.sample_evaluators = self.remote_evaluators[
            self.config["num_gradient_worker_shards"]:]
        # assert len(self.grad_evaluators) > 0
        assert len(self.sample_evaluators) > 0

        # Stats
        self.grad_timer = TimerStat()
        self.get_batch_timer = TimerStat()
        self.priorities_timer = TimerStat()
        self.processing_timer = TimerStat()
        self.num_weight_syncs = 0
        self.num_sample_only_iters = 0
        self.num_samples_added = 0
        self.num_samples_trained = 0

        # Number of worker steps since the last weight update
        self.steps_since_update = {}

        # Tracking for async tasks running in the background
        self.sample_tasks = TaskPool()
        self.replay_tasks = TaskPool()

        # Kick off sampling from the replay buffer
        for ra in self.replay_actors:
            self.replay_tasks.add(ra, ra.replay.remote())

        # Kick off background sampling to fill the replay buffer
        weights = ray.put(self.local_evaluator.get_weights())
        for ev in self.sample_evaluators:
            ev.set_weights.remote(weights)
            self.num_weight_syncs += 1
            self.steps_since_update[ev] = 0
            self.sample_tasks.add(ev, ev.sample.remote())

    @property
    def train_to_learn_ratio(self):
        return max(1, self.num_samples_trained) / float(
            max(1, self.num_samples_added - self.config["learning_starts"]))

    def step(self):
        timesteps_this_iter = 0

        # Fetch next batch to optimize over
        with self.get_batch_timer:
            origin_actor, samples = self.replay_tasks.wait_one()
            samples = ray.get(samples)
            self.replay_tasks.add(origin_actor, origin_actor.replay.remote())

        # Process any completed sample requests
        with self.processing_timer:
            weights = None
            for ev, sample_batch in self.sample_tasks.completed():
                if not weights:
                    weights = ray.put(self.local_evaluator.get_weights())

                timesteps_this_iter += self.config["sample_batch_size"]
                self.num_samples_added += self.config["sample_batch_size"]

                # Send the data to the replay buffer
                random.choice(self.replay_actors).add_batch.remote(
                    sample_batch)

                # Update weights if needed
                self.steps_since_update[ev] += self.config["sample_batch_size"]
                if (self.steps_since_update[ev] >=
                        self.config["max_weight_sync_delay"]):
                    ev.set_weights.remote(weights)
                    self.num_weight_syncs += 1
                    self.steps_since_update[ev] = 0

                # Kick off another sample request
                self.sample_tasks.add(ev, ev.sample.remote())

                if (self.train_to_learn_ratio <
                        self.config["min_train_to_sample_ratio"]):
                    print(
                        "Throttling sampling since learner is falling behind",
                        self.train_to_learn_ratio)
                    break  # throttle sampling until training catches up

        # Compute gradients if learning has started
        if samples is None:
            self.num_sample_only_iters += 1
            return timesteps_this_iter

        with self.grad_timer:
            grad = self.local_evaluator.compute_gradients(samples)
            self.local_evaluator.apply_gradients(grad)
            self.grad_timer.push_units_processed(samples.count)
            self.num_samples_trained += self.config["train_batch_size"]

        # TODO(ekl) consider fusing this into compute_gradients
        with self.priorities_timer:
            td_error = self.local_evaluator.compute_td_error(samples)
            origin_actor.update_batch_priorities.remote(
                samples, td_error)

        return timesteps_this_iter

    def stats(self):
        return {
            "processing_time_ms": round(1000 * self.processing_timer.mean, 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "get_batch_time_ms": round(1000 * self.get_batch_timer.mean, 3),
            "priorities_time_ms": round(1000 * self.priorities_timer.mean, 3),
            "opt_peak_throughput": round(self.grad_timer.mean_throughput, 3),
            "opt_samples": round(self.grad_timer.mean_units_processed, 3),
            "num_weight_syncs": self.num_weight_syncs,
            "num_sample_only_iters": self.num_sample_only_iters,
            "num_samples_trained": self.num_samples_trained,
            "pending_replay_tasks": self.replay_tasks.count,
            "pending_sample_tasks": self.sample_tasks.count,
            "train_to_sample_ratio": round(self.train_to_learn_ratio, 3),
        }
