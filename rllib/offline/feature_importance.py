from typing import Callable, Dict, Any
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.offline.offline_evaluator import OfflineEvaluator

import numpy as np
import copy


@DeveloperAPI
def perturb_fn(batch: np.ndarray, index: int):
    # shuffle the indexth column features
    random_inds = np.random.permutation(batch.shape[0])
    batch[:, index] = batch[random_inds, index]


@DeveloperAPI
class FeatureImportance(OfflineEvaluator):
    @override(OfflineEvaluator)
    def __init__(
        self,
        policy: Policy,
        repeat: int = 1,
        perturb_fn: Callable[[np.ndarray, int], None] = perturb_fn,
    ):
        """Feature importance in a model inspection technique that can be used for any
        fitted predictor when the data is tablular.

        This implementation is also known as permutation importance that is defined to
        be the variation of the model's prediction when a single feature value is
        randomly shuffled. In RLlib it is implemented as a custom OffPolicyEstimator
        which is used to evaluate RLlib policies without performing environment
        interactions.

        Example usage: In the example below the feature importance module is used to
        evaluate the policy and the each feature's importance is computed after each
        training iteration. The permutation are repeated `self.repeat` times and the
        results are averages across repeats.

        ```python
            config = (
                AlgorithmConfig()
                .offline_data(
                    off_policy_estimation_methods=
                        {
                            "feature_importance": {
                                "type": FeatureImportance,
                                "repeat": 10
                            }
                        }
                )
            )

            algorithm = DQN(config=config)
            results = algorithm.train()
        ```

        Args:
            policy: the policy to use for feature importance.
            repeat: number of times to repeat the perturbation.
            perturb_fn: function to perturb the features. By default reshuffle the
            features within the batch.
        """
        super().__init__(policy)
        self.repeat = repeat
        self.perturb_fn = perturb_fn

    def estimate(self, batch: SampleBatchType, **kwargs) -> Dict[str, Any]:
        """Estimate the feature importance of the policy.

        Given a batch of tabular observations, the importance of each feature is
        computed by perturbing each feature and computing the difference between the
        perturbed policy and the reference policy. The importance is computed for each
        feature and each perturbation is repeated `self.repeat` times.

        Args:
            batch: the batch of data to use for feature importance.

        Returns:
            A dict mapping each feature index string to its importance.
        """

        obs_batch = batch["obs"]
        n_features = obs_batch.shape[-1]
        importance = np.zeros((self.repeat, n_features))

        ref_actions, _, _ = self.policy.compute_actions(obs_batch, explore=False)
        for r in range(self.repeat):
            for i in range(n_features):
                copy_obs_batch = copy.deepcopy(obs_batch)
                perturb_fn(copy_obs_batch, index=i)
                perturbed_actions, _, _ = self.policy.compute_actions(
                    copy_obs_batch, explore=False
                )

                importance[r, i] = np.mean(np.abs(perturbed_actions - ref_actions))

        # take an average across repeats
        importance = importance.mean(0)
        metrics = {f"feature_{i}": importance[i] for i in range(len(importance))}

        return metrics
