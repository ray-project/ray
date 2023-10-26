from typing import Dict, List, Any
import math

from ray.data import Dataset

from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.offline.offline_evaluator import OfflineEvaluator
from ray.rllib.offline.offline_evaluation_utils import (
    remove_time_dim,
    compute_is_weights,
)
from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator
from ray.rllib.policy.sample_batch import SampleBatch


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
            v_behavior += rewards[t] * self.gamma**t
            v_target += p[t] * rewards[t] * self.gamma**t

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

    @override(OfflineEvaluator)
    def estimate_on_dataset(
        self, dataset: Dataset, *, n_parallelism: int = ...
    ) -> Dict[str, Any]:
        """Computes the Importance sampling estimate on the given dataset.

        Note: This estimate works for both continuous and discrete action spaces.

        Args:
            dataset: Dataset to compute the estimate on. Each record in dataset should
                include the following columns: `obs`, `actions`, `action_prob` and
                `rewards`. The `obs` on each row shoud be a vector of D dimensions.
            n_parallelism: The number of parallel workers to use.

        Returns:
            A dictionary containing the following keys:
                v_target: The estimated value of the target policy.
                v_behavior: The estimated value of the behavior policy.
                v_gain_mean: The mean of the gain of the target policy over the
                    behavior policy.
                v_gain_ste: The standard error of the gain of the target policy over
                    the behavior policy.
        """
        batch_size = max(dataset.count() // n_parallelism, 1)
        dataset = dataset.map_batches(
            remove_time_dim, batch_size=batch_size, batch_format="pandas"
        )
        updated_ds = dataset.map_batches(
            compute_is_weights,
            batch_size=batch_size,
            batch_format="pandas",
            fn_kwargs={
                "policy_state": self.policy.get_state(),
                "estimator_class": self.__class__,
            },
        )
        v_target = updated_ds.mean("weighted_rewards")
        v_behavior = updated_ds.mean("rewards")
        v_gain_mean = v_target / v_behavior
        v_gain_ste = (
            updated_ds.std("weighted_rewards") / v_behavior / math.sqrt(dataset.count())
        )

        return {
            "v_target": v_target,
            "v_behavior": v_behavior,
            "v_gain_mean": v_gain_mean,
            "v_gain_ste": v_gain_ste,
        }
