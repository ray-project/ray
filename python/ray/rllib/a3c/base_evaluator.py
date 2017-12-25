from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle

import ray
from ray.rllib.envs import create_and_wrap
from ray.rllib.optimizers import Evaluator
from ray.rllib.a3c.common import get_policy_cls
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.sampler import AsyncSampler
from ray.rllib.utils.process_rollout import process_rollout


class A3CEvaluator(Evaluator):
    """Actor object to start running simulation on workers.

    The gradient computation is also executed from this object.

    Attributes:
        policy: Copy of graph used for policy. Used by sampler and gradients.
        obs_filter: Observation filter used in environment sampling
        rew_filter: Reward filter used in rollout post-processing.
        sampler: Component for interacting with environment and generating
            rollouts.
        logdir: Directory for logging.
    """
    def __init__(self, env_creator, config, logdir, start_sampler=True):
        self.env = env = create_and_wrap(env_creator, config["preprocessing"])
        policy_cls = get_policy_cls(config)
        # TODO(rliaw): should change this to be just env.observation_space
        self.policy = policy_cls(
            env.observation_space.shape, env.action_space, config)
        self.config = config

        # Technically not needed when not remote
        self.obs_filter = get_filter(
            config["observation_filter"], env.observation_space.shape)
        self.rew_filter = get_filter(config["reward_filter"], ())
        self.sampler = AsyncSampler(env, self.policy, self.obs_filter,
                                    config["batch_size"])
        if start_sampler and self.sampler.async:
            self.sampler.start()
        self.logdir = logdir

    @ray.method(num_return_vals=2)
    def sample(self):
        """Returns experience samples from this Evaluator. Observation
        filter and reward filters are flushed here.

        Returns:
            SampleBatch: A columnar batch of experiences.
            info (dict): Extra return values - observation and reward filters
        """
        rollout = self.sampler.get_data()
        samples = process_rollout(
            rollout, self.rew_filter, gamma=self.config["gamma"],
            lambda_=self.config["lambda"], use_gae=True)

        obs_filter, rew_filter = self.get_filters(flush_after=True)
        info = {"obs_filter": obs_filter, "rew_filter": rew_filter}
        return samples, info

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

    @ray.method(num_return_vals=2)
    def compute_gradients(self, samples):
        gradient, info = self.policy.compute_gradients(samples)
        return gradient, info

    def apply_gradients(self, grads):
        self.policy.apply_gradients(grads)

    def get_weights(self):
        return self.policy.get_weights()

    def set_weights(self, params):
        self.policy.set_weights(params)

    def save(self):
        obs_filter, rew_filter = self.get_filters(flush_after=True)
        weights = self.get_weights()
        return pickle.dumps({
            "obs_filter": obs_filter,
            "rew_filter": rew_filter,
            "weights": weights})

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.sync_filters(objs["obs_filter"], objs["rew_filter"])
        self.set_weights(objs["weights"])


RemoteA3CEvaluator = ray.remote(A3CEvaluator)
