import random

from ray.rllib.policy.policy import Policy


class TestPolicy(Policy):
    """
    A dummy Policy that returns a random (batched) int for compute_actions
    and implements all other abstract methods of Policy with "pass".
    """

    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        episodes=None,
                        explore=None,
                        timestep=None,
                        **kwargs):
        return [random.choice([0, 1])] * len(obs_batch), [], {}
