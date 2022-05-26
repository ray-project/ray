from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimate
from ray.rllib.offline.estimators.direct_method import DirectMethod, k_fold_cv
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.numpy import convert_to_numpy
import numpy as np


class DoublyRobust(DirectMethod):
    """The Doubly Robust (DR) estimator with Q-Reg Q-function.

    DR estimator described in https://arxiv.org/pdf/1511.03722.pdf,
    Q-Reg DR in https://arxiv.org/pdf/1911.06854.pdf"""

    @override(DirectMethod)
    def estimate(self, batch: SampleBatchType) -> OffPolicyEstimate:
        self.check_can_estimate_for(batch)
        estimates = []
        # Split data into train and test using k-fold cross validation
        for train_episodes, test_episodes in k_fold_cv(batch, self.k):
            # Reinitialize model
            self.model.reset()

            # Train Q-function
            if train_episodes:
                train_batch = train_episodes[0].concat_samples(train_episodes)
                losses = self.train(train_batch)  # noqa: F841

            # Calculate doubly robust OPE estimates
            for episode in test_episodes:
                rewards, old_prob = episode["rewards"], episode["action_prob"]
                new_prob = np.exp(self.action_log_likelihood(episode))

                v_old = 0.0
                v_dr = 0.0
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

                for t in range(episode.count - 1, -1, -1):
                    v_old = rewards[t] + self.gamma * v_old
                    v_dr = v_values[t] + (new_prob[t] / old_prob[t]) * (
                        rewards[t] + self.gamma * v_dr - q_values[t]
                    )

                estimates.append(
                    OffPolicyEstimate(
                        "doubly_robust",
                        {
                            "v_old": v_old,
                            "v_dr": v_dr,
                            "v_gain": v_dr / max(1e-8, v_old),
                        },
                    )
                )
        return estimates
