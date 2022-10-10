from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.policy.sample_batch import SampleBatch
from typing import Dict, List


@DeveloperAPI
class ImportanceSampling(OffPolicyEstimator):
    r"""The step-wise IS estimator.

    Let s_t, a_t, and r_t be the state, action, and reward at timestep t.

    For behavior policy \pi_b and evaluation policy \pi_e, define the
    cumulative importance ratio at timestep t as:
    p_t = \sum_{t'=0}^t (\pi_e(a_{t'} | s_{t'}) / \pi_b(a_{t'} | s_{t'})).

    This estimator computes the expected return for \pi_e for an episode as:
    V^{\pi_e}(s_0) = \sum_t \gamma ^ {t} * p_t * r_t
    and returns the mean and standard deviation over episodes.

    For more information refer to https://arxiv.org/pdf/1911.06854.pdf"""

    @override(OffPolicyEstimator)
    def estimate_on_single_episode(self, episode: SampleBatch) -> Dict[str, float]:
        estimates_per_epsiode = {}

        rewards, old_prob = episode["rewards"], episode["action_prob"]
        new_prob = self.compute_action_probs(episode)

        # calculate importance ratios
        p = []
        for t in range(episode.count):
            if t == 0:
                pt_prev = 1.0
            else:
                pt_prev = p[t - 1]
            p.append(pt_prev * new_prob[t] / old_prob[t])

        # calculate stepwise IS estimate
        v_behavior = 0.0
        v_target = 0.0
        for t in range(episode.count):
            v_behavior += rewards[t] * self.gamma ** t
            v_target += p[t] * rewards[t] * self.gamma ** t

        estimates_per_epsiode["v_behavior"] = v_behavior
        estimates_per_epsiode["v_target"] = v_target

        return estimates_per_epsiode

    @override(OffPolicyEstimator)
    def estimate_on_single_step_samples(
        self, batch: SampleBatch
    ) -> Dict[str, List[float]]:
        estimates_per_epsiode = {}

        rewards, old_prob = batch["rewards"], batch["action_prob"]
        new_prob = self.compute_action_probs(batch)

        weights = new_prob / old_prob
        v_behavior = rewards
        v_target = weights * rewards

        estimates_per_epsiode["v_behavior"] = v_behavior
        estimates_per_epsiode["v_target"] = v_target

        return estimates_per_epsiode
