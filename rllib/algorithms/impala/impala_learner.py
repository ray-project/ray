from dataclasses import dataclass
from typing import Any, List, Mapping

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.core.learner.learner import Learner, LearnerHyperparameters
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.typing import ResultDict


@dataclass
class ImpalaHyperparameters(LearnerHyperparameters):
    """Hyperparameters for the ImpalaLearner sub-classes (framework specific).

    These should never be set directly by the user. Instead, use the IMPALAConfig
    class to configure your algorithm.
    See `ray.rllib.algorithms.impala.impala::IMPALAConfig::training()` for more details
    on the individual properties.

    Attributes:
        rollout_frag_or_episode_len: The length of a rollout fragment or episode.
            Used when making SampleBatches time major for computing loss.
        recurrent_seq_len: The length of a recurrent sequence. Used when making
            SampleBatches time major for computing loss.
    """

    rollout_frag_or_episode_len: int = None
    recurrent_seq_len: int = None
    discount_factor: float = None
    vtrace_clip_rho_threshold: float = None
    vtrace_clip_pg_rho_threshold: float = None
    vtrace_drop_last_ts: bool = None
    vf_loss_coeff: float = None
    entropy_coeff: float = None


class ImpalaLearner(Learner):
    @override(Learner)
    def compile_results(
        self,
        batch: MultiAgentBatch,
        fwd_out: Mapping[str, Any],
        postprocessed_loss: Mapping[str, Any],
        postprocessed_gradients: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        results = super().compile_results(
            batch, fwd_out, postprocessed_loss, postprocessed_gradients
        )
        results[ALL_MODULES][NUM_AGENT_STEPS_TRAINED] = batch.agent_steps()
        results[ALL_MODULES][NUM_ENV_STEPS_TRAINED] = batch.env_steps()
        return results


def _reduce_impala_results(results: List[ResultDict]) -> ResultDict:
    """Reduce/Aggregate a list of results from Impala Learners.

    Average the values of the result dicts. Add keys for the number of agent and env
    steps trained.

    Args:
        results: result dicts to reduce.

    Returns:
        A reduced result dict.
    """
    result = tree.map_structure(lambda *x: np.mean(x), *results)
    agent_steps_trained = sum(r[ALL_MODULES][NUM_AGENT_STEPS_TRAINED] for r in results)
    env_steps_trained = sum(r[ALL_MODULES][NUM_ENV_STEPS_TRAINED] for r in results)
    result[ALL_MODULES][NUM_AGENT_STEPS_TRAINED] = agent_steps_trained
    result[ALL_MODULES][NUM_ENV_STEPS_TRAINED] = env_steps_trained
    return result
