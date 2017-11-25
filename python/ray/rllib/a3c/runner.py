from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.a3c.envs import create_and_wrap
from ray.rllib.a3c.runner_thread import AsyncSampler
from ray.rllib.a3c.common import process_rollout, get_filter, get_policy_cls


class Runner(object):
    """Actor object to start running simulation on workers.

    The gradient computation is also executed from this object.

    Attributes:
        policy: Copy of graph used for policy. Used by sampler and gradients.
        rew_filter: Reward filter used in rollout post-processing.
        sampler: Component for interacting with environment and generating
            rollouts.
        logdir: Directory for logging.
    """
    def __init__(self, env_creator, config, logdir):
        self.env = env = create_and_wrap(env_creator, config["model"])
        policy_cls = get_policy_cls(config)
        # TODO(rliaw): should change this to be just env.observation_space
        self.policy = policy_cls(env.observation_space.shape, env.action_space)
        self.rew_filter = get_filter(
            config["reward_filter"], 1)

        # TODO(rliaw): Convert this to a cataloged object.
        self.sampler = AsyncSampler(env, self.policy, config["batch_size"],
                                    config["observation_filter"])
        self.logdir = logdir
        if self.sampler.async:
            self.sampler.start_runner()

    def get_data(self):
        """
        Returns:
            trajectory: trajectory information
            obs_filter: Current state of observation filter
            rew_filter: Current state of reward filter"""
        rollout, obs_filter = self.sampler.get_data()
        return rollout, obs_filter, self.rew_filter

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

    def compute_gradient(self):
        rollout, obsf_snapshot = self.sampler.get_data()
        batch = process_rollout(
            rollout, self.rew_filter, gamma=0.99, lambda_=1.0)
        gradient, info = self.policy.compute_gradients(batch)
        info["obs_filter"] = obsf_snapshot
        info["rew_filter"] = self.rew_filter
        return gradient, info

    def set_weights(self, params):
        self.policy.set_weights(params)

    def update_filters(self, obs_filter=None, rew_filter=None):
        if rew_filter:
            # No special handling required since outside of threaded code
            self.rew_filter = rew_filter.copy()
        if obs_filter:
            self.sampler.update_obs_filter(obs_filter)


RemoteRunner = ray.remote(Runner)
