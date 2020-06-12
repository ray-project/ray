"""Example of using rollout worker classes directly to implement training.

Instead of using the built-in Trainer classes provided by RLlib, here we define
a custom Policy class and manually coordinate distributed sample
collection and policy optimization.
"""

import argparse
import gym
import numpy as np

import ray
from ray import tune
from ray.rllib.evaluation import RolloutWorker
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch

parser = argparse.ArgumentParser()
parser.add_argument("--gpu", action="store_true")
parser.add_argument("--num-iters", type=int, default=20)
parser.add_argument("--num-workers", type=int, default=2)
parser.add_argument("--num-cpus", type=int, default=0)


class CustomPolicy(Policy):
    """Example of a custom policy written from scratch.

    You might find it more convenient to extend TF/TorchPolicy instead
    for a real policy.
    """

    def __init__(self, observation_space, action_space, config):
        super().__init__(observation_space, action_space, config)
        # example parameter
        self.w = 1.0

    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        # return random actions
        return np.array([self.action_space.sample()
                         for _ in obs_batch]), [], {}

    def learn_on_batch(self, samples):
        # implement your learning code here
        return {}

    def update_some_value(self, w):
        # can also call other methods on policies
        self.w = w

    def get_weights(self):
        return {"w": self.w}

    def set_weights(self, weights):
        self.w = weights["w"]


def training_workflow(config, reporter):
    # Setup policy and policy evaluation actors
    env = gym.make("CartPole-v0")
    policy = CustomPolicy(env.observation_space, env.action_space, {})
    workers = [
        RolloutWorker.as_remote().remote(lambda c: gym.make("CartPole-v0"),
                                         CustomPolicy)
        for _ in range(config["num_workers"])
    ]

    for _ in range(config["num_iters"]):
        # Broadcast weights to the policy evaluation workers
        weights = ray.put({"default_policy": policy.get_weights()})
        for w in workers:
            w.set_weights.remote(weights)

        # Gather a batch of samples
        T1 = SampleBatch.concat_samples(
            ray.get([w.sample.remote() for w in workers]))

        # Update the remote policy replicas and gather another batch of samples
        new_value = policy.w * 2.0
        for w in workers:
            w.for_policy.remote(lambda p: p.update_some_value(new_value))

        # Gather another batch of samples
        T2 = SampleBatch.concat_samples(
            ray.get([w.sample.remote() for w in workers]))

        # Improve the policy using the T1 batch
        policy.learn_on_batch(T1)

        # Do some arbitrary updates based on the T2 batch
        policy.update_some_value(sum(T2["rewards"]))

        reporter(**collect_metrics(remote_workers=workers))


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)

    tune.run(
        training_workflow,
        resources_per_trial={
            "gpu": 1 if args.gpu else 0,
            "cpu": 1,
            "extra_cpu": args.num_workers,
        },
        config={
            "num_workers": args.num_workers,
            "num_iters": args.num_iters,
        },
    )
