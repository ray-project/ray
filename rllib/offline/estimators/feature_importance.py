# TODO (@Kourosh) move this to a better location and consolidate the parent class with
# OPE

from typing import Callable, List
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy import Policy
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.offline.estimators.off_policy_estimator import (
    OffPolicyEstimator,
    OffPolicyEstimate,
)

import numpy as np
import copy


def perturb_fn(batch: np.ndarray, index: int):
    # shuffle the indexth column features
    random_inds = np.random.permutation(batch.shape[0])
    batch[:, index] = batch[random_inds, index]


class FeatureImportance(OffPolicyEstimator):
    def __init__(
        self,
        name: str,
        policy: Policy,
        gamma: float,
        repeat: int = 1,
        perturb_fn: Callable[[np.ndarray, int], None] = perturb_fn,
    ):
        """
        Args:
            name: string to save the feature importance results under.
            policy: the policy to use for feature importance.
            repeat: number of times to repeat the perturbation.
            gamma: dummy discount factor to be passed to the super class.
            perturb_fn: function to perturb the features. By default reshuffle the features within the batch.
        """
        super().__init__(name, policy, gamma=gamma)
        self.repeat = repeat
        self.perturb_fn = perturb_fn

    def estimate(self, batch: SampleBatchType) -> List[OffPolicyEstimate]:

        obs_batch = batch["obs"]
        n_features = obs_batch.shape[-1]
        importance = np.zeros((self.repeat, n_features))

        ref_actions, _, _ = self.policy.compute_actions(obs_batch)
        for r in range(self.repeat):
            for i in range(n_features):
                copy_obs_batch = copy.deepcopy(obs_batch)
                perturb_fn(copy_obs_batch, index=i)
                perturbed_actions, _, _ = self.policy.compute_actions(copy_obs_batch)

                importance[r, i] = np.mean(np.abs(perturbed_actions - ref_actions))

        # take an average across repeats
        importance = importance.mean(0)
        metrics = {f"feature_{i}": importance[i] for i in range(len(importance))}
        ret = [OffPolicyEstimate(self.name, metrics)]

        return ret
