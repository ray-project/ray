from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.a3c.envs import create_and_wrap
from ray.rllib.a3c.runner_thread import AsyncSampler
from ray.rllib.a3c.common import process_rollout

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
        self.sampler = AsyncSampler(env, self.policy, batch_size)
        self.env = env
        self.logdir = logdir
        self.start()

    def get_data(self):
        return self.sampler.get_data()

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

    def start(self):
        self.sampler.start_runner()

    def compute_gradient(self, params):
        self.policy.set_weights(params)
        rollout = self.sampler.get_data()
        batch = process_rollout(rollout, gamma=0.99, lambda_=1.0)
        gradient, info = self.policy.compute_gradients(batch)
        return gradient, info


RemoteRunner = ray.remote(Runner)
