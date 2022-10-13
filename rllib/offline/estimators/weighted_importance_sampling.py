from typing import Dict, Any, List
import numpy as np

from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override, DeveloperAPI


@DeveloperAPI
class WeightedImportanceSampling(OffPolicyEstimator):
    r"""The step-wise WIS estimator.

    Let s_t, a_t, and r_t be the state, action, and reward at timestep t.

    For behavior policy \pi_b and evaluation policy \pi_e, define the
    cumulative importance ratio at timestep t as:
    p_t = \sum_{t'=0}^t (\pi_e(a_{t'} | s_{t'}) / \pi_b(a_{t'} | s_{t'})).

    Define the average importance ratio over episodes i in the dataset D as:
    w_t = \sum_{i \in D} p^(i)_t / |D|

    This estimator computes the expected return for \pi_e for an episode as:
    V^{\pi_e}(s_0) = \E[\sum_t \gamma ^ {t} * (p_t / w_t) * r_t]
    and returns the mean and standard deviation over episodes.

    For more information refer to https://arxiv.org/pdf/1911.06854.pdf"""

    @override(OffPolicyEstimator)
    def __init__(self, policy: Policy, gamma: float, epsilon_greedy: float = 0.0):
        super().__init__(policy, gamma, epsilon_greedy)
        # map from time to cummulative propensity values
        self.cummulative_ips_values = []
        # map from time to number of episodes that reached this time
        self.episode_timestep_count = []
        # map from eps id to mapping from time to propensity values
        self.p = {}

    @override(OffPolicyEstimator)
    def estimate_on_single_episode(self, episode: SampleBatch) -> Dict[str, Any]:
        estimates_per_epsiode = {}
        rewards = episode["rewards"]

        eps_id = episode[SampleBatch.EPS_ID][0]
        if eps_id not in self.p:
            raise ValueError(
                f"Cannot find target weight for episode {eps_id}. "
                f"Did it go though the peek_on_single_episode() function?"
            )

        # calculate stepwise weighted IS estimate
        v_behavior = 0.0
        v_target = 0.0
        episode_p = self.p[eps_id]
        for t in range(episode.count):
            v_behavior += rewards[t] * self.gamma ** t
            w_t = self.cummulative_ips_values[t] / self.episode_timestep_count[t]
            v_target += episode_p[t] / w_t * rewards[t] * self.gamma ** t

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
        v_target = weights * rewards / np.mean(weights)

        estimates_per_epsiode["v_behavior"] = v_behavior
        estimates_per_epsiode["v_target"] = v_target

        return estimates_per_epsiode

    @override(OffPolicyEstimator)
    def on_before_split_batch_by_episode(
        self, sample_batch: SampleBatch
    ) -> SampleBatch:
        self.cummulative_ips_values = []
        self.episode_timestep_count = []
        self.p = {}

        return sample_batch

    @override(OffPolicyEstimator)
    def peek_on_single_episode(self, episode: SampleBatch) -> None:
        old_prob = episode["action_prob"]
        new_prob = self.compute_action_probs(episode)

        # calculate importance ratios
        episode_p = []
        for t in range(episode.count):
            if t == 0:
                pt_prev = 1.0
            else:
                pt_prev = episode_p[t - 1]
            episode_p.append(pt_prev * new_prob[t] / old_prob[t])

        for t, p_t in enumerate(episode_p):
            if t >= len(self.cummulative_ips_values):
                self.cummulative_ips_values.append(p_t)
                self.episode_timestep_count.append(1.0)
            else:
                self.cummulative_ips_values[t] += p_t
                self.episode_timestep_count[t] += 1.0

        eps_id = episode[SampleBatch.EPS_ID][0]
        if eps_id in self.p:
            raise ValueError(
                f"eps_id {eps_id} was already passed to the peek function."
                f"Make sure dataset contains only unique episodes with unique ids."
            )
        self.p[eps_id] = episode_p
