from typing import Callable, Dict, Any

import ray
from ray.data import Dataset

from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override, DeveloperAPI, ExperimentalAPI
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.offline.offline_evaluator import OfflineEvaluator
from ray.rllib.offline.offline_evalution_utils import remove_time_dim

import pandas as pd
import numpy as np
import copy


@DeveloperAPI
def _perturb_fn(batch: np.ndarray, index: int):
    # shuffle the indexth column features
    random_inds = np.random.permutation(batch.shape[0])
    batch[:, index] = batch[random_inds, index]


@ExperimentalAPI
def _perturb_df(batch: pd.DataFrame, index: int):
    obs_batch = np.vstack(batch["obs"].values)
    _perturb_fn(obs_batch, index)
    batch["perturbed_obs"] = list(obs_batch)
    return batch


def _compute_actions(
    batch: pd.DataFrame,
    policy_state: Dict[str, Any],
    input_key: str = "",
    output_key: str = "",
):
    if not input_key:
        input_key = SampleBatch.OBS

    policy = Policy.from_state(policy_state)
    sample_batch = SampleBatch(
        {
            SampleBatch.OBS: np.vstack(batch[input_key].values),
        }
    )
    actions, _, _ = policy.compute_actions_from_input_dict(sample_batch, explore=False)

    if not output_key:
        output_key = "predicted_actions"
    batch[output_key] = actions

    return batch


@ray.remote
def get_feature_importance_on_index(dataset, *, index, perturb_fn, bsize, policy_state):
    perturbed_ds = dataset.map_batches(
        perturb_fn, batch_size=bsize, fn_kwargs={"index": index}
    )
    perturbed_actions = perturbed_ds.map_batches(
        _compute_actions,
        batch_size=bsize,
        fn_kwargs={
            "output_key": "perturbed_actions",
            "input_key": "perturbed_obs",
            "policy_state": policy_state,
        },
    )

    def delta_fn(batch):
        # take the abs difference between columns 'ref_actions` and `perturbed_actions`
        # and store it in `diff`
        batch["delta"] = np.abs(batch["ref_actions"] - batch["perturbed_actions"])
        return batch

    diff = perturbed_actions.map_batches(delta_fn, batch_size=bsize)

    return diff


@DeveloperAPI
class FeatureImportance(OfflineEvaluator):
    @override(OfflineEvaluator)
    def __init__(
        self,
        policy: Policy,
        repeat: int = 1,
        limit_fraction: float = 1.0,
        perturb_fn: Callable[[pd.DataFrame, int], pd.DataFrame] = _perturb_df,
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
                                "repeat": 10,
                                "limit_fraction": 0.1,
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
            limit_fraction: fraction of the dataset to use for feature importance
                (to be used only in estimate_on_dataset)
        """
        super().__init__(policy)
        self.repeat = repeat
        self.perturb_fn = perturb_fn
        self.limit_fraction = limit_fraction

    def estimate(self, batch: SampleBatchType) -> Dict[str, Any]:
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
                _perturb_fn(copy_obs_batch, index=i)
                perturbed_actions, _, _ = self.policy.compute_actions(
                    copy_obs_batch, explore=False
                )
                importance[r, i] = np.mean(np.abs(perturbed_actions - ref_actions))

        # take an average across repeats
        importance = importance.mean(0)
        metrics = {f"feature_{i}": importance[i] for i in range(len(importance))}

        return metrics

    def estimate_on_dataset(
        self, dataset: Dataset, *, n_parallelism: int = ...
    ) -> Dict[str, Any]:

        policy_state = self.policy.get_state()
        # step 1: limit the dataset to a few first rows
        ds = dataset.limit(int(self.limit_fraction * dataset.count()))

        # step 2: remove the time dimension from the dataset
        bsize = max(1, ds.count() // n_parallelism)
        updated_ds = ds.map_batches(remove_time_dim, batch_size=bsize)

        # step 3: compute the reference actions
        bsize = max(1, updated_ds.count() // n_parallelism)
        actions_ds = updated_ds.map_batches(
            _compute_actions,
            batch_size=bsize,
            fn_kwargs={
                "output_key": "ref_actions",
                "policy_state": policy_state,
            },
        )

        n_features = updated_ds.take(1)[0]["obs"].shape[-1]
        importance = np.zeros((self.repeat, n_features))
        for r in range(self.repeat):
            # shuffle the entire dataset
            shuffled_ds = actions_ds.random_shuffle()
            bsize_per_task = max(1, (shuffled_ds.count() * n_features) // n_parallelism)

            # for each index perturb the dataset and compute the feat importance score
            remote_fns = [
                get_feature_importance_on_index.remote(
                    dataset=shuffled_ds,
                    index=i,
                    perturb_fn=self.perturb_fn,
                    bsize=bsize_per_task,
                    policy_state=policy_state,
                )
                for i in range(n_features)
            ]
            ds_w_fi_scores = ray.get(remote_fns)
            importance[r] = np.array([d.mean("delta") for d in ds_w_fi_scores])

        importance = importance.mean(0)
        metrics = {f"feature_{i}": importance[i] for i in range(len(importance))}

        return metrics
