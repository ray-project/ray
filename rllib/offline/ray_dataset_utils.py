
import pandas as pd
from typing import Union, Any, Dict, Optional

from ray.air.checkpoint import Checkpoint

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy import Policy


def get_policy_from_checkpoint_or_state(
    policy: Policy, checkpoint: Union[str, Checkpoint] = None,
    policy_state: Optional[Dict[str, Any]] = None,
) -> Policy:
    """Returns a policy from the given checkpoint or policy state.
    
    Args:
        policy: The policy to use as a template.
        checkpoint: The checkpoint to restore the policy from.
        policy_state: The policy state to restore the policy from.
    Returns:
        The policy restored from the given checkpoint or policy state.
    """
    if checkpoint and policy_state:
        raise ValueError("Only one of checkpoint and policy_state can be provided.")

    policy = None
    if checkpoint:
        policy = Policy.from_checkpoint(checkpoint)["default_policy"]
    if policy_state:
        policy = Policy.from_state(policy_state)
    if not policy:
        raise ValueError("Either checkpoint or policy_state must be provided.")

    return policy
    
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

