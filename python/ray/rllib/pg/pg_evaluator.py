import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers import Evaluator
from ray.rllib.pg.policy import PGPolicy
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler

# Evaluator

class PGEvaluator(Evaluator):
    """Actor for simple policy gradient."""

    def __init__(self, registry, env_creator, config):
        self.env = ModelCatalog.get_preprocessor_as_wrapper(registry, env_creator(config["env_config"]), config["model"])
        self.config = config
        self.registry = registry

        self.policy = PGPolicy(registry, self.env.observation_space,
                               self.env.action_space, config)

        # Processing for observations, rewards
        self.obs_filter = get_filter(
            config["observation_filter"], self.env.observation_space)
        self.rew_filter = get_filter(config["reward_filter"], ())
        self.filters = {"obs_filter": self.obs_filter,
                        "rew_filter": self.rew_filter}

        # Sampler
        self.sampler = SyncSampler(self.env, self.policy, self.obs_filter, config["batch_size"])

    def sample(self):
        rollout = self.sampler.get_data()
        samples = process_rollout(rollout, self.rew_filter,
                    gamma=self.config["gamma"], lambda_=self.config["lambda"])
        return samples

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

    def compute_gradients(self, samples):
        """ Returns gradient w.r.t. samples."""
        gradient, info = self.policy.compute_gradients(samples)
        return gradient

    def apply_gradients(self, grads):
        """Applies gradients to evaluator weights."""
        self.policy.apply_gradients(grads)

    def get_weights(self):
        """Returns model weights."""
        return self.policy.get_weights()

    def set_weights(self, weights):
        """Sets model weights."""
        return self.policy.set_weights(weights)

RemotePGEvaluator = ray.remote(PGEvaluator)
