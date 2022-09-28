from typing import Dict, Any
import numpy as np
import tree

from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator
from ray.rllib.policy.sample_batch import SampleBatch, SampleBatchType
from ray.rllib.utils.policy import compute_log_likelihoods_from_input_dict
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.numpy import convert_to_numpy


@DeveloperAPI
class WeightedImportanceSampling(OffPolicyEstimator):
    """The step-wise WIS estimator.

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
    def __init__(self, policy: Policy, gamma: float):
        super().__init__(policy, gamma)
        self.filter_values = []  # map from time to cummulative propensity values
        # map from time to number of episodes that reached this time
        self.filter_counts = []
        self.p = {}  # map from eps id to mapping from time to propensity values

    @override(OffPolicyEstimator)
    def estimate_on_episode(self, episode: SampleBatch) -> Dict[str, float]:
        estimates_per_epsiode = {"v_behavior": None, "v_target": None}
        rewards = episode["rewards"]

        eps_id = episode[SampleBatch.EPS_ID][0]
        if eps_id not in self.p:
            raise ValueError(f"Episode {eps_id} not passed through the fit function")

        # calculate stepwise weighted IS estimate
        v_behavior = 0.0
        v_target = 0.0
        episode_p = self.p[eps_id]
        for t in range(episode.count):
            v_behavior += rewards[t] * self.gamma ** t
            w_t = self.filter_values[t] / self.filter_counts[t]
            v_target += episode_p[t] / w_t * rewards[t] * self.gamma ** t

        estimates_per_epsiode["v_behavior"] = v_behavior
        estimates_per_epsiode["v_target"] = v_target

        return estimates_per_epsiode

    @override(OffPolicyEstimator)
    def estimate_single_step(self, batch: SampleBatch) -> Dict[str, float]:
        estimates_per_epsiode = {"v_behavior": None, "v_target": None}
        rewards, old_prob = batch["rewards"], batch["action_prob"]
        log_likelihoods = compute_log_likelihoods_from_input_dict(self.policy, batch)
        new_prob = np.exp(convert_to_numpy(log_likelihoods))

        weights = new_prob / old_prob
        v_behavior = rewards
        v_target = weights * rewards / np.mean(weights)

        estimates_per_epsiode["v_behavior"] = v_behavior
        estimates_per_epsiode["v_target"] = v_target

        return estimates_per_epsiode

    def _reset_stats(self):
        self.filter_values = []
        self.filter_counts = []
        self.p = {}

    def _fit_on_episode(self, episode: SampleBatch) -> None:
        old_prob = episode["action_prob"]
        log_likelihoods = compute_log_likelihoods_from_input_dict(self.policy, episode)
        new_prob = np.exp(convert_to_numpy(log_likelihoods))

        # calculate importance ratios
        episode_p = []
        for t in range(episode.count):
            if t == 0:
                pt_prev = 1.0
            else:
                pt_prev = episode_p[t - 1]
            episode_p.append(pt_prev * new_prob[t] / old_prob[t])

        for t, p_t in enumerate(episode_p):
            if t >= len(self.filter_values):
                self.filter_values.append(p_t)
                self.filter_counts.append(1.0)
            else:
                self.filter_values[t] += p_t
                self.filter_counts[t] += 1.0

        eps_id = episode[SampleBatch.EPS_ID][0]
        if eps_id in self.p:
            raise ValueError(
                f"Episode {eps_id} already paseed through the fit function"
            )
        self.p[eps_id] = episode_p

    @DeveloperAPI
    def estimate(
        self, batch: SampleBatchType, split_batch_by_episode: bool = True
    ) -> Dict[str, Any]:
        """Compute off-policy estimates.

        Args:
            batch: The batch to calculate the off-policy estimates (OPE) on. The
            batch must contain the fields "obs", "actions", and "action_prob".
            split_batch_by_episode: Whether to split the batch by episode.

        Returns:
            The off-policy estimates (OPE) calculated on the given batch. The returned
            dict can be any arbitrary mapping of strings to metrics.
            The dict consists of the following metrics:
            - v_behavior: The discounted return averaged over episodes in the batch
            - v_behavior_std: The standard deviation corresponding to v_behavior
            - v_target: The estimated discounted return for `self.policy`,
            averaged over episodes in the batch
            - v_target_std: The standard deviation corresponding to v_target
            - v_gain: v_target / max(v_behavior, 1e-8)
            - v_delta: The difference between v_target and v_behavior.
        """
        batch = self.convert_ma_batch_to_sample_batch(batch)
        self.check_action_prob_in_batch(batch)
        estimates_per_epsiode = []
        if split_batch_by_episode:
            self._reset_stats()
            all_episodes = batch.split_by_episode()
            for episode in all_episodes:
                assert len(set(episode[SampleBatch.EPS_ID])) == 1, (
                    "The episode must contain only one episode id. For some reason "
                    "the split_by_episode() method could not successfully split "
                    "the batch by episodes. Each row in the dataset should be "
                    "one episode. Check your evaluation dataset for errors."
                )
                self._fit_on_episode(episode)

            for episode in all_episodes:
                estimate_step_results = self.estimate_on_episode(episode)
                estimates_per_epsiode.append(estimate_step_results)

            # turn a list of identical dicts into a dict of lists
            estimates_per_epsiode = tree.map_structure(
                lambda *x: list(x), *estimates_per_epsiode
            )
        else:
            estimates_per_epsiode = self.estimate_single_step(batch)

        estimates = {
            "v_behavior": np.mean(estimates_per_epsiode["v_behavior"]),
            "v_behavior_std": np.std(estimates_per_epsiode["v_behavior"]),
            "v_target": np.mean(estimates_per_epsiode["v_target"]),
            "v_target_std": np.std(estimates_per_epsiode["v_target"]),
        }
        estimates["v_gain"] = estimates["v_target"] / max(estimates["v_behavior"], 1e-8)
        estimates["v_delta"] = estimates["v_target"] - estimates["v_behavior"]

        return estimates
