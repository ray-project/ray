from typing import List, Optional

from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch


def post_process_advantages(
        policy: Policy,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[List[SampleBatch]] = None,
        episode: Optional[MultiAgentEpisode] = None) -> SampleBatch:
    """Adds the "advantages" column to `sample_batch`.

    Args:
        policy (Policy): The Policy object to do post-processing for.
        sample_batch (SampleBatch): The actual sample batch to post-process.
        other_agent_batches (Optional[List[SampleBatch]]): Optional list of
            other agents' SampleBatch objects.
        episode (MultiAgentEpisode): The multi-agent episode object, from which
            `sample_batch` was generated.

    Returns:
        SampleBatch: The SampleBatch enhanced by the added ADVANTAGES field.
    """

    # Calculates advantage values based on the rewards in the sample batch.
    # The value of the last observation is assumed to be 0.0 (no value function
    # estimation at the end of the sampled chunk).
    return compute_advantages(
        rollout=sample_batch,
        last_r=0.0,
        gamma=policy.config["gamma"],
        use_gae=False,
        use_critic=False)
