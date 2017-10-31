from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.a3c.envs import create_and_wrap
import tensorflow as tf
import six.moves.queue as queue
from ray.rllib.a3c.runner_thread import RunnerThread
from ray.rllib.a3c.common import process_rollout
from ray.rllib.a3c.tfpolicy import TFPolicy
import ray
import os


class Runner(object):
    """Actor object to start running simulation on workers.

    The gradient computation is also executed from this object.
    """
    def __init__(self, env_creator, policy_cls, actor_id, batch_size,
                 preprocess_config, logdir):
        env = create_and_wrap(env_creator, preprocess_config)
        self.id = actor_id
        # TODO(rliaw): should change this to be just env.observation_space
        self.policy = policy_cls(env.observation_space.shape, env.action_space)
        self.runner = RunnerThread(env, self.policy, batch_size)
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
        if isinstance(self.policy, TFPolicy):
            self.runner.start_runner(self.policy.sess, summary_writer)
        else:
            self.runner.start_runner(tf.Session(), summary_writer)

    def compute_gradient(self, params):
        self.policy.set_weights(params)
        rollout = self.pull_batch_from_queue()
        batch = process_rollout(rollout, gamma=0.99, lambda_=1.0)
        gradient, info = self.policy.compute_gradients(batch)
        if "summary" in info:
            self.summary_writer.add_summary(
                tf.Summary.FromString(info['summary']),
                self.policy.local_steps)
            self.summary_writer.flush()
        info = {"id": self.id,
                "size": len(batch.a)}
        return gradient, info


RemoteRunner = ray.remote(Runner)
