from typing import Dict, Any, List, Optional, Union
import os

from ray.air.checkpoint import Checkpoint
from ray.data import Dataset

from ray.rllib.offline.offline_evaluator import OfflineEvaluator
from ray.rllib.offline.offline_evalution_utils import (
    remove_time_dim, compute_is_weights
)
from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator
from ray.rllib.utils.annotations import override, DeveloperAPI
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
        self, 
        dataset: Dataset, 
        checkpoint: Union[str, Checkpoint] = None,
        policy_state: Optional[Dict[str, Any]] = None,
        *, 
        n_parallelism: int = os.cpu_count(),
    ):        
        dsize = dataset.count()
        batch_size = max(dsize // n_parallelism, 1)
        # step 1: clean the dataset and remove the time dimension from bandits
        updated_ds = dataset.map_batches(remove_time_dim, batch_size=batch_size)
        # step 2: compute the weights and weighted rewards
        batch_size = max(updated_ds.count() // n_parallelism, 1)
        updated_ds = updated_ds.map_batches(
            compute_is_weights, 
            batch_size=batch_size, 
            fn_kwargs={
                "checkpoint": checkpoint, 
                "policy_state": policy_state, 
                "estimator_class": self.__class__
            }
        )
        v_target = updated_ds.mean("weighted_rewards")
        v_behavior = updated_ds.mean("rewards")
        v_gain = v_target / v_behavior
        # TODO (Kourosh): Fix the STD
        v_std = updated_ds.std("weighted_rewards")
        
        return {
            "v_target": v_target,
            "v_behavior": v_behavior,
            "v_gain": v_gain,
            "v_std": v_std,
        }
