from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import random
import time

import ray
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.timer import TimerStat


class TaskPool(object):
    def __init__(self):
        self._tasks = {}

    def add(self, worker, obj_id):
        self._tasks[obj_id] = worker

    def completed(self):
        pending = list(self._tasks)
        ready, _ = ray.wait(pending, num_returns=len(pending), timeout=10)
        for obj_id in ready:
            yield (self._tasks.pop(obj_id), obj_id)

    @property
    def count(self):
        return len(self._tasks)


@ray.remote
class ReplayActor(object):
    def __init__(self, config, replay_starts):
        self.config = config
        self.replay_starts = replay_starts
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
        if len(self.replay_buffer) < self.replay_starts:
            return None

        (obses_t, actions, rewards, obses_tp1,
            dones, weights, batch_indexes) = self.replay_buffer.sample(
                self.config["train_batch_size"],
                beta=self.config["prioritized_replay_beta"])

        return SampleBatch({
            "obs": obses_t, "actions": actions, "rewards": rewards,
            "new_obs": obses_tp1, "dones": dones, "weights": weights,
            "batch_indexes": batch_indexes})

    def update_priorities(self, batch, td_errors):
        new_priorities = (
            np.abs(td_errors) + self.config["prioritized_replay_eps"])
        self.replay_buffer.update_priorities(
            batch["batch_indexes"], new_priorities)


class ApexOptimizer(Optimizer):

    def _init(self):
        num_replay_actors = self.config["num_replay_buffer_shards"]
        self.replay_actors = [
            ReplayActor.remote(
                self.config,
                self.config["learning_starts"] // num_replay_actors)
            for _ in range(num_replay_actors)]
        self.grad_evaluators = self.remote_evaluators[
            :self.config["num_gradient_worker_shards"]]
        self.sample_evaluators = self.remote_evaluators[
            self.config["num_gradient_worker_shards"]:]
        assert len(self.grad_evaluators) > 0
        assert len(self.sample_evaluators) > 0

        # Stats
        self.processing_grad_timer = TimerStat()
        self.processing_sample_timer = TimerStat()
        self.get_grad_timer = TimerStat()
        self.apply_grad_timer = TimerStat()
        self.put_weights_timer = TimerStat()
        self.train_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.num_weight_syncs = 0
        self.num_samples_added = 0
        self.num_samples_trained = 0
        self.throttling_count = 0

        # Number of worker steps since the last weight update
        self.steps_since_update = {}

        # Tracking for async tasks running in the background
        self.sample_tasks = TaskPool()
        self.grad_tasks = TaskPool()
        self.grads_to_samples = {}

        # Kick off gradient computation
        weights = ray.put(self.local_evaluator.get_weights())
        for ev in self.grad_evaluators:
            ev.set_weights.remote(weights)
            ra = random.choice(self.replay_actors)
            replay_task = ra.replay.remote()
            grad_task = ev.compute_gradients.remote(replay_task)
            self.grads_to_samples[grad_task] = (ra, replay_task)
            self.grad_tasks.add(ev, grad_task)

        # Kick off background sampling to fill the replay buffer
        for ev in self.sample_evaluators:
            ev.set_weights.remote(weights)
            self.steps_since_update[ev] = 0
            self.sample_tasks.add(ev, ev.sample.remote())

    @property
    def train_to_learn_ratio(self):
        return max(1, self.num_samples_trained) / float(
            max(1, self.num_samples_added - self.config["learning_starts"]))

    def step(self):
        start = time.time()
        sample_timesteps, train_timesteps = self._step()
        time_delta = time.time() - start
        self.sample_timer.push(time_delta)
        self.sample_timer.push_units_processed(sample_timesteps)
        if train_timesteps > 0:
            self.train_timer.push(time_delta)
            self.train_timer.push_units_processed(train_timesteps)
        return sample_timesteps

    def _step(self):
        sample_timesteps, train_timesteps = 0, 0
        with self.put_weights_timer:
            weights = ray.put(self.local_evaluator.get_weights())

        with self.processing_grad_timer:
            for ev, obj_id in self.grad_tasks.completed():
                # Apply the gradient, if possible
                with self.get_grad_timer:
                    grad, td_error = ray.get(obj_id)
                if grad is not None:
                    with self.apply_grad_timer:
                        self.local_evaluator.apply_gradients(grad)
                    orig_ra, orig_samples = self.grads_to_samples.pop(obj_id)
                    orig_ra.update_priorities.remote(orig_samples, td_error)
                    train_timesteps += self.config["train_batch_size"]
                    self.num_samples_trained += self.config["train_batch_size"]
                    weights = ray.put(self.local_evaluator.get_weights())
                    ev.set_weights.remote(weights)

                # Update the evaluator and kick off another grad task
                ra = random.choice(self.replay_actors)
                replay_task = ra.replay.remote()
                grad_task = ev.compute_gradients.remote(replay_task)
                self.grads_to_samples[grad_task] = (ra, replay_task)
                self.grad_tasks.add(ev, grad_task)

        with self.processing_sample_timer:
            if (self.train_to_learn_ratio <
                    self.config["min_train_to_sample_ratio"]):
                self.throttling_count += 1
                completed = []  # throttle sampling until training catches up
            else:
                completed = self.sample_tasks.completed()

            for ev, sample_batch in completed:
                sample_timesteps += self.config["sample_batch_size"]
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

        return sample_timesteps, train_timesteps

    def stats(self):
        return {
            "_processing_sample_time_ms": round(
                1000 * self.processing_sample_timer.mean, 3),
            "_processing_grad_time_ms": round(
                1000 * self.processing_grad_timer.mean, 3),
            "_put_weights_time_ms": round(
                1000 * self.put_weights_timer.mean, 3),
            "_get_grad_time_ms": round(1000 * self.get_grad_timer.mean, 3),
            "_apply_grad_time_ms": round(1000 * self.apply_grad_timer.mean, 3),
            "sample_throughput": round(self.sample_timer.mean_throughput, 3),
            "train_throughput": round(self.train_timer.mean_throughput, 3),
            "num_weight_syncs": self.num_weight_syncs,
            "num_samples_trained": self.num_samples_trained,
            "pending_sample_tasks": self.sample_tasks.count,
            "pending_grad_tasks": self.grad_tasks.count,
            "throttling_count": self.throttling_count,
            "train_to_sample_ratio": round(self.train_to_learn_ratio, 3),
        }
