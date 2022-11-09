
import pandas as pd
from typing import Any, Dict, Type, TYPE_CHECKING
import numpy as np

from ray.rllib.utils.numpy import convert_to_numpy

if TYPE_CHECKING:
    from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy import Policy


def compute_q_and_v_values(
    batch: pd.DataFrame, 
    model_class: Type["FQETorchModel"], 
    model_state: Dict[str, Any],
    compute_q_values: bool = True,
):
    model = model_class.from_state(model_state)

    sample_batch = SampleBatch({
        SampleBatch.OBS: np.vstack(batch[SampleBatch.OBS]),
        SampleBatch.ACTIONS: np.vstack(batch[SampleBatch.ACTIONS]).squeeze(-1),
    })

    v_values = model.estimate_v(sample_batch)
    v_values = convert_to_numpy(v_values)
    batch["v_values"] = v_values

    if compute_q_values:
        q_values = model.estimate_q(sample_batch)
        q_values = convert_to_numpy(q_values)
        batch["q_values"] = q_values

    return batch


def compute_is_weights(
    batch, 
    policy_state,
    estimator_class,
):  
    """Computes importance sampling weights for the given batch of samples."""
    policy = policy = Policy.from_state(policy_state)
    estimator = estimator_class(policy=policy, gamma=0, epsilon_greedy=0)
    sample_batch = SampleBatch({
        SampleBatch.OBS: np.vstack(batch["obs"].values),
        SampleBatch.ACTIONS: np.vstack(batch["actions"].values).squeeze(-1),
        SampleBatch.ACTION_PROB: np.vstack(batch["action_prob"].values).squeeze(-1),
        SampleBatch.REWARDS: np.vstack(batch["rewards"].values).squeeze(-1),
    })
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

    
def remove_time_dim(batch: pd.DataFrame) -> pd.DataFrame:
    """Removes the time dimension from the given sub-batch of the dataset.
    
    RLlib assumes each record in the dataset is a single episode. 
    However, for bandits, each episode is only a single timestep. This function removes 
    the time dimension from the given sub-batch of the dataset.

    Args:
        batch: The batch to remove the time dimension from.
    Returns:
        The batch with the time dimension removed.
    """
    for k in [
        SampleBatch.OBS,
        SampleBatch.ACTIONS,
        SampleBatch.ACTION_PROB,
        SampleBatch.REWARDS,
        SampleBatch.NEXT_OBS,
        SampleBatch.DONES,
    ]:
        batch[k] = batch[k].apply(lambda x: x[0])
    return batch

