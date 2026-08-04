from typing import TYPE_CHECKING, Any, Dict, Type

import numpy as np
import pandas as pd

from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.numpy import convert_to_numpy

if TYPE_CHECKING:
    from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
    from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator


@DeveloperAPI
def compute_q_and_v_values(
    batch: pd.DataFrame,
    model_class: Type["FQETorchModel"],
    model_state: Dict[str, Any],
    compute_q_values: bool = True,
) -> pd.DataFrame:
    """Computes the Q and V values for the given batch of samples.

    This function is to be used with map_batches() to perform a batch prediction on a
    dataset of records with `obs` and `actions` columns.

    Args:
        batch: A sub-batch from the dataset.
        model_class: The model class to use for the prediction. This class should be a
            sub-class of FQEModel that implements the estimate_q() and estimate_v()
            methods.
        model_state: The state of the model to use for the prediction.
        compute_q_values: Whether to compute the Q values or not. If False, only the V
            is computed and returned.

    Returns:
        The modified batch with the Q and V values added as columns.
    """
    model = model_class.from_state(model_state)

    sample_batch = SampleBatch(
        {
            SampleBatch.OBS: np.vstack(batch[SampleBatch.OBS]),
            SampleBatch.ACTIONS: np.vstack(batch[SampleBatch.ACTIONS]).squeeze(-1),
        }
    )

    v_values = model.estimate_v(sample_batch)
    v_values = convert_to_numpy(v_values)
    batch["v_values"] = v_values

    if compute_q_values:
        q_values = model.estimate_q(sample_batch)
        q_values = convert_to_numpy(q_values)
        batch["q_values"] = q_values

    return batch


@DeveloperAPI
def compute_is_weights(
    batch: pd.DataFrame,
    policy_state: Dict[str, Any],
    estimator_class: Type["OffPolicyEstimator"],
) -> pd.DataFrame:
    """Computes the importance sampling weights for the given batch of samples.

    For a lot of off-policy estimators, the importance sampling weights are computed as
    the propensity score ratio between the new and old policies
    (i.e. new_pi(act|obs) / old_pi(act|obs)). This function is to be used with
    map_batches() to perform a batch prediction on a dataset of records with `obs`,
    `actions`, `action_prob` and `rewards` columns.

    Args:
        batch: A sub-batch from the dataset.
        policy_state: The state of the policy to use for the prediction.
        estimator_class: The estimator class to use for the prediction. This class

    Returns:
        The modified batch with the importance sampling weights, weighted rewards, new
        and old propensities added as columns.
    """
    policy = Policy.from_state(policy_state)
    estimator = estimator_class(policy=policy, gamma=0, epsilon_greedy=0)
    sample_batch = SampleBatch(
        {
            SampleBatch.OBS: np.vstack(batch["obs"].values),
            SampleBatch.ACTIONS: np.vstack(batch["actions"].values).squeeze(-1),
            SampleBatch.ACTION_PROB: np.vstack(batch["action_prob"].values).squeeze(-1),
            SampleBatch.REWARDS: np.vstack(batch["rewards"].values).squeeze(-1),
        }
    )
    new_prob = estimator.compute_action_probs(sample_batch)
    old_prob = sample_batch[SampleBatch.ACTION_PROB]
    rewards = sample_batch[SampleBatch.REWARDS]
    weights = new_prob / old_prob
    weighted_rewards = weights * rewards

    batch["weights"] = weights
    batch["weighted_rewards"] = weighted_rewards
    batch["new_prob"] = new_prob
    batch["old_prob"] = old_prob

    return batch


@DeveloperAPI
def remove_time_dim(batch: pd.DataFrame) -> pd.DataFrame:
    """Removes the time dimension from the given sub-batch of the dataset.

    If each row in a dataset has a time dimension ([T, D]), and T=1, this function will
    remove the T dimension to convert each row to of shape [D]. If T > 1, the row is
    left unchanged. This function is to be used with map_batches().

    Args:
        batch: The batch to remove the time dimension from.
    Returns:
        The modified batch with the time dimension removed (when applicable)
    """
    BATCHED_KEYS = {
        SampleBatch.OBS,
        SampleBatch.ACTIONS,
        SampleBatch.ACTION_PROB,
        SampleBatch.REWARDS,
        SampleBatch.NEXT_OBS,
        SampleBatch.DONES,
    }
    for k in batch.columns:
        if k in BATCHED_KEYS:
            batch[k] = batch[k].apply(lambda x: x[0] if len(x) == 1 else x)
    return batch
