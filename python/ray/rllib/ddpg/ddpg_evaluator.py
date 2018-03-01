# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.ddpg.models import DDPGModel
from ray.rllib.ddpg.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.optimizers import SampleBatch
from ray.rllib.optimizers import Evaluator
from ray.rllib.utils.filter import NoFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler
from ray.tune.registry import get_registry

import tensorflow as tf


class DDPGEvaluator(Evaluator):


    def __init__(self, registry, env_creator, config):
        self.registry = registry
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]), config["actor_model"])
        self.config = config
        # don't need to refer to the actor and critic here; just provide all
        # functionality through model
        self.model = DDPGModel(self.registry, self.env, self.config)
        self.replay_buffer = ReplayBuffer(config["buffer_size"])
        self.sampler = SyncSampler(
                        self.env, self.model, NoFilter(),
                        config["batch_size"], horizon=config["horizon"])


    #TODO: Add batch normalization?
    def sample(self, no_replay = True):
        """Returns a batch of samples."""
        # act in the environment, generate new samples
        rollout = self.sampler.get_data()
        samples = process_rollout(
                    rollout, NoFilter(),
                    gamma=self.config["gamma"], use_gae=False)
        # either do what DQN did, and manually step without using the sampler,
        # or construct new_obs directly from obs

        # Add samples to replay buffer.
        print (samples)
        #for s in samples:
        #for row in samples.rows():
        #    self.replay_buffer.add(
        #        row["obs"], row["actions"], row["rewards"], row["new_obs"],
        #        row["dones"])

        if no_replay:
            return SampleBatch.concat_samples(samples)

        # Then return a batch sampled from the buffer; copied from DQN
        obses_t, actions, rewards, obses_tp1, dones = \
            self.replay_buffer.sample(self.config["train_batch_size"])
        batch = SampleBatch({
            "obs": obses_t, "actions": actions, "rewards": rewards,
            "new_obs": obses_tp1, "dones": dones,
            "weights": np.ones_like(rewards)})
        return batch

    def compute_gradients(self, samples):
        """ Returns gradient w.r.t. samples."""
        gradient = self.model.compute_gradients(samples)
        return gradient

    def apply_gradients(self, grads):
        """Applies gradients to evaluator weights."""
        # We can apply gradients to actor and critic separately?
        self.model.apply_gradients(grads)

    def get_weights(self):
        """Returns model weights."""
        return self.model.get_weights()

    def set_weights(self, weights):
        """Sets model weights."""
        self.model.set_weights(weights)

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

RemoteDDPGEvaluator = ray.remote(DDPGEvaluator)

if __name__ == '__main__':
    ray.init()
    import gym
    config = ray.rllib.ddpg.DEFAULT_CONFIG.copy()
    DDPGEvaluator(get_registry(), lambda config: gym.make("CartPole-v0"),
        config)
