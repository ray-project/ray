from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimate
from ray.rllib.offline.estimators.direct_method import DirectMethod
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.numpy import convert_to_numpy
import numpy as np

from typing import Optional


@DeveloperAPI
class DoublyRobust(DirectMethod):
    """The Doubly Robust (DR) estimator.

    DR estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    @override(DirectMethod)
    def estimate(
        self,
        eval_batch: SampleBatchType,
        train_batch: Optional[SampleBatchType] = None,
    ) -> OffPolicyEstimate:
        self.check_can_estimate_for(eval_batch)
        estimates = []

        # Train Q-function
        if train_batch:
            self.check_can_estimate_for(train_batch)
            self.model.reset()
            losses = self.train(train_batch)
            self.losses.append(losses)

        # Calculate doubly robust OPE estimates
        for episode in eval_batch.split_by_episode():
            rewards, old_prob = episode["rewards"], episode["action_prob"]
            new_prob = np.exp(self.action_log_likelihood(episode))

            v_old = 0.0
            v_new = 0.0
            q_values = self.model.estimate_q(
                episode[SampleBatch.OBS], episode[SampleBatch.ACTIONS]
            )
            q_values = convert_to_numpy(q_values)

            all_actions = np.zeros([episode.count, self.policy.action_space.n])
            all_actions[:] = np.arange(self.policy.action_space.n)
            # Two transposes required for torch.distributions to work
            tmp_episode = episode.copy()
            tmp_episode[SampleBatch.ACTIONS] = all_actions.T
            action_probs = np.exp(self.action_log_likelihood(tmp_episode)).T
            v_values = self.model.estimate_v(episode[SampleBatch.OBS], action_probs)
            v_values = convert_to_numpy(v_values)

            for t in reversed(range(episode.count)):
                v_old = rewards[t] + self.gamma * v_old
                v_new = v_values[t] + (new_prob[t] / old_prob[t]) * (
                    rewards[t] + self.gamma * v_new - q_values[t]
                )
            v_new = v_new.item()

            estimates.append(
                OffPolicyEstimate(
                    self.name,
                    {
                        "v_old": v_old,
                        "v_new": v_new,
                        "v_gain": v_new / max(1e-8, v_old),
                    },
                )
            )
        return estimates
