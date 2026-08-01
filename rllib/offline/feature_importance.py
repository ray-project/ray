import copy
from typing import Any, Callable, Dict

import numpy as np
import pandas as pd

import ray
from ray.data import Dataset
from ray.rllib.offline.offline_evaluator import OfflineEvaluator
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch, convert_ma_batch_to_sample_batch
from ray.rllib.utils.annotations import DeveloperAPI, ExperimentalAPI, override
from ray.rllib.utils.typing import SampleBatchType


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
    """A custom local function to do batch prediction of a policy.

    Given the policy state the action predictions are computed as a function of
    `input_key` and stored in the `output_key` column.

    Args:
        batch: A sub-batch from the dataset.
        policy_state: The state of the policy to use for the prediction.
        input_key: The key to use for the input to the policy. If not given, the
            default is SampleBatch.OBS.
        output_key: The key to use for the output of the policy. If not given, the
            default is "predicted_actions".

    Returns:
        The modified batch with the predicted actions added as a column.
    """
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
def get_feature_importance_on_index(
    dataset: ray.data.Dataset,
    *,
    index: int,
    perturb_fn: Callable[[pd.DataFrame, int], None],
    batch_size: int,
    policy_state: Dict[str, Any],
):
    """A remote function to compute the feature importance of a given index.

    Args:
        dataset: The dataset to use for the computation. The dataset should have `obs`
            and `actions` columns. Each record should be flat d-dimensional array.
        index: The index of the feature to compute the importance for.
        perturb_fn: The function to use for perturbing the dataset at the given index.
        batch_size: The batch size to use for the computation.
        policy_state: The state of the policy to use for the computation.

    Returns:
        The modified dataset that contains a `delta` column which is the absolute
        difference between the expected output and the output due to the perturbation.
    """
    perturbed_ds = dataset.map_batches(
        perturb_fn,
        batch_size=batch_size,
        batch_format="pandas",
        fn_kwargs={"index": index},
    )
    perturbed_actions = perturbed_ds.map_batches(
        _compute_actions,
        batch_size=batch_size,
        batch_format="pandas",
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

    delta = perturbed_actions.map_batches(
        delta_fn, batch_size=batch_size, batch_format="pandas"
    )

    return delta


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
                This is only used in estimate_on_dataset when the dataset is too large
                to compute feature importance on.
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
        batch = convert_ma_batch_to_sample_batch(batch)
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

    @override(OfflineEvaluator)
    def estimate_on_dataset(
        self, dataset: Dataset, *, n_parallelism: int = ...
    ) -> Dict[str, Any]:
        """Estimate the feature importance of the policy given a dataset.

        For each feature in the dataset, the importance is computed by applying
        perturbations to each feature and computing the difference between the
        perturbed prediction and the reference prediction. The importance
        computation for each feature and each perturbation is repeated `self.repeat`
        times. If dataset is large the user can initialize the estimator with a
        `limit_fraction` to limit the dataset to a fraction of the original dataset.

        The dataset should include a column named `obs` where each row is a vector of D
        dimensions. The importance is computed for each dimension of the vector.

        Note (Implementation detail): The computation across features are distributed
        with ray workers since each feature is independent of each other.

        Args:
            dataset: the dataset to use for feature importance.
            n_parallelism: number of parallel workers to use for feature importance.

        Returns:
            A dict mapping each feature index string to its importance.
        """

        policy_state = self.policy.get_state()
        # step 1: limit the dataset to a few first rows
        ds = dataset.limit(int(self.limit_fraction * dataset.count()))

        # step 2: compute the reference actions
        bsize = max(1, ds.count() // n_parallelism)
        actions_ds = ds.map_batches(
            _compute_actions,
            batch_size=bsize,
            fn_kwargs={
                "output_key": "ref_actions",
                "policy_state": policy_state,
            },
        )

        # step 3: compute the feature importance
        n_features = ds.take(1)[0][SampleBatch.OBS].shape[-1]
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
