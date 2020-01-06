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
                        deterministic=None,
                        explore=True,
                        time_step=None,
                        **kwargs):
        return [random.choice([0, 1])] * len(obs_batch), [], {}

    def compute_gradients(self, postprocessed_batch):
        pass

    def apply_gradients(self, gradients):
        pass

    def get_weights(self):
        pass

    def set_weights(self, weights):
        pass

    def export_checkpoint(self, export_dir):
        pass

    def export_model(self, export_dir):
        pass

    def num_state_tensors(self):
        return 0
