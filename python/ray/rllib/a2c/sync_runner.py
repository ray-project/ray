from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pickle
import tensorflow as tf
import six.moves.queue as queue
import os

import ray
from ray.rllib.a3c.runner import process_rollout, env_runner
from ray.rllib.a3c.envs import create_env


DEFAULT_CONFIG = {
    "num_workers": 4,
    "num_batches_per_iteration": 100,
    "batch_size": 10
}


@ray.remote
class SyncRunner(object):
    """Actor object to start running simulation on workers.

    The gradient computation is also executed from this object.
    """
    def __init__(self, env_name, policy_cls, actor_id, batch_size, logdir):
        env = create_env(env_name)
        self.id = actor_id
        # TODO(rliaw): should change this to be just env.observation_space
        self.policy = policy_cls(env.observation_space.shape, env.action_space)
        self.runner = SyncRunnerThread(env, self.policy, batch_size)
        self.env = env
        self.logdir = logdir
        self.start()

    def pull_batch_from_queue(self):
        """Take a rollout from the queue of the thread runner."""
        rollout = self.runner.queue.get(timeout=600.0)
        if isinstance(rollout, BaseException):
            raise rollout
        while not rollout.terminal:
            try:
                part = self.runner.queue.get_nowait()
                if isinstance(part, BaseException):
                    raise rollout
                rollout.extend(part)
            except queue.Empty:
                break
        return rollout

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        completed = []
        while True:
            try:
                completed.append(self.runner.metrics_queue.get_nowait())
            except queue.Empty:
                break
        return completed

    def start(self):
        summary_writer = tf.summary.FileWriter(
            os.path.join(self.logdir, "agent_%d" % self.id))
        self.summary_writer = summary_writer
        self.runner.start_sync(self.policy.sess, summary_writer)

    def compute_gradient(self, params):
        self.policy.set_weights(params)
        rollout = self.pull_batch_from_queue()
        batch = process_rollout(rollout, gamma=0.99, lambda_=1.0)
        gradient, info = self.policy.get_gradients(batch)
        if "summary" in info:
            self.summary_writer.add_summary(
                tf.Summary.FromString(info['summary']),
                self.policy.local_steps)
            self.summary_writer.flush()
        info = {"id": self.id,
                "size": len(batch.a)}
        return gradient, info


class SyncRunnerThread():
    """This thread interacts with the environment and tells it what to do."""
    def __init__(self, env, policy, num_local_steps, visualise=False):
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.metrics_queue = queue.Queue()
        self.num_local_steps = num_local_steps
        self.env = env
        self.last_features = None
        self.policy = policy
        self.daemon = True
        self.sess = None
        self.summary_writer = None
        self.visualise = visualise

    def start_sync(self, sess, summary_writer):
        self.sess = sess
        self.summary_writer = summary_writer
        self.rollout_provider = env_runner(
            self.env, self.policy, self.num_local_steps,
            self.summary_writer, self.visualise)

    def sync_run(self):
        while True:
            # The timeout variable exists because apparently, if one worker
            # dies, the other workers won't die with it, unless the timeout is
            # set to some large number. This is an empirical observation.
            item = next(self.rollout_provider)
            if isinstance(item, CompletedRollout):
                self.metrics_queue.put(item)
            else:
                self.queue.put(item, timeout=600.0)
                return