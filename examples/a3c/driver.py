from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import numpy as np
from runner import RunnerThread, process_rollout
from LSTM import LSTMPolicy
import tensorflow as tf
import six.moves.queue as queue
import gym
import sys
import os
from datetime import datetime, timedelta
from misc import *
from envs import create_env

@ray.actor
class Runner(object):
    """Actor object to start running simulation on workers.
        Gradient computation is also executed from this object."""
    def __init__(self, env_name, actor_id, logdir="results/", start=True):
        env = create_env(env_name)
        self.id = actor_id
        num_actions = env.action_space.n
        self.policy = LSTMPolicy(env.observation_space.shape, num_actions, actor_id)
        self.runner = RunnerThread(env, self.policy, 20)
        self.env = env
        self.logdir = logdir
        if start:
            self.start()

    def pull_batch_from_queue(self):
        """ self explanatory:  take a rollout from the queue of the thread runner. """
        rollout = self.runner.queue.get(timeout=600.0)
        while not rollout.terminal:
            try:
                rollout.extend(self.runner.queue.get_nowait())
            except queue.Empty:
                break
        return rollout

    def start(self):
        summary_writer = tf.summary.FileWriter(os.path.join(self.logdir, "agent_%d" % self.id))
        self.summary_writer = summary_writer
        self.runner.start_runner(self.policy.sess, summary_writer)

    def compute_gradient(self, params):
        self.policy.set_weights(params)
        rollout = self.pull_batch_from_queue()
        batch = process_rollout(rollout, gamma=0.99, lambda_=1.0)
        gradient = self.policy.get_gradients(batch)
        info = {"id": self.id,
                "size": len(batch.a)}
        return gradient, info


def train(num_workers, env_name="PongDeterministic-v3"):
    env = create_env(env_name)
    policy = LSTMPolicy(env.observation_space.shape, env.action_space.n, 0)
    agents = [Runner(env_name, i) for i in range(num_workers)]
    parameters = policy.get_weights()
    gradient_list = [agent.compute_gradient(parameters) for agent in agents]
    steps = 0
    obs = 0
    while True:
        done_id, gradient_list = ray.wait(gradient_list)
        gradient, info = ray.get(done_id)[0]
        policy.model_update(gradient)
        parameters = policy.get_weights()
        steps += 1
        obs += info["size"]
        gradient_list.extend([agents[info["id"]].compute_gradient(parameters)])
    return policy

if __name__ == '__main__':
    # if gym.__version__[:3] == '0.8':
    #   raise Exception("This example currently does not work with gym==0.8.0. "
    #                   "Please downgrade to gym==0.7.4.");
    num_workers = int(sys.argv[1])
    ray.init(num_cpus=num_workers)
    train(num_workers)
