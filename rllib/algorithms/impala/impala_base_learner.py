from dataclasses import dataclass
import numpy as np
from typing import Any, List, Mapping
import tree

from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.core.learner.learner import LearnerHPs
from ray.rllib.utils.annotations import override
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.typing import ResultDict


@dataclass
class ImpalaHPs(LearnerHPs):
    """Hyper-parameters for IMPALA.

    Attributes:
        rollout_frag_or_episode_len: The length of a rollout fragment or episode.
            Used when making SampleBatches time major for computing loss.
        recurrent_seq_len: The length of a recurrent sequence. Used when making
            SampleBatches time major for computing loss.
        discount_factor: The discount factor to use for computing returns.
        vtrace_clip_rho_threshold: The rho threshold to use for clipping the
            importance weights.
        vtrace_clip_pg_rho_threshold: The rho threshold to use for clipping the
            importance weights when computing the policy_gradient loss.
        vtrace_drop_last_ts: Whether to drop the last timestep when computing the loss.
            This is useful for stabilizing the loss.
            NOTE: This shouldn't be True when training on environments where the rewards
            come at the end of the episode.
        vf_loss_coeff: The amount to weight the value function loss by when computing
            the total loss.
        entropy_coeff: The amount to weight the average entropy of the actions in the
            SampleBatch towards the total_loss for module updates. The higher this
            coefficient, the more that the policy network will be encouraged to output
            distributions with higher entropy/std deviation, which will encourage
            greater exploration.

    """

    rollout_frag_or_episode_len: int = None
    recurrent_seq_len: int = None
    discount_factor: float = 0.99
    vtrace_clip_rho_threshold: float = 1.0
    vtrace_clip_pg_rho_threshold: float = 1.0
    vtrace_drop_last_ts: bool = True
    vf_loss_coeff: float = 0.5
    entropy_coeff: float = 0.01


class ImpalaBaseLearner(Learner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.vtrace_clip_rho_threshold = self._hps.vtrace_clip_rho_threshold
        self.vtrace_clip_pg_rho_threshold = self._hps.vtrace_clip_pg_rho_threshold
        self.vtrace_drop_last_ts = self._hps.vtrace_drop_last_ts
        self.vf_loss_coeff = self._hps.vf_loss_coeff
        self.entropy_coeff = self._hps.entropy_coeff
        self.rollout_frag_or_episode_len = self._hps.rollout_frag_or_episode_len
        self.recurrent_seq_len = self._hps.recurrent_seq_len
        self.discount_factor = self._hps.discount_factor
        assert (
            self.rollout_frag_or_episode_len is not None
            or self.recurrent_seq_len is not None
        ) and not (self.rollout_frag_or_episode_len and self.recurrent_seq_len), (
            "Either rollout_frag_or_episode_len or recurrent_seq_len"
            " must be set in the IMPALA HParams. "
        )

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
    agent_steps_trained = sum(
        [r[ALL_MODULES][NUM_AGENT_STEPS_TRAINED] for r in results]
    )
    env_steps_trained = sum([r[ALL_MODULES][NUM_ENV_STEPS_TRAINED] for r in results])
    result[ALL_MODULES][NUM_AGENT_STEPS_TRAINED] = agent_steps_trained
    result[ALL_MODULES][NUM_ENV_STEPS_TRAINED] = env_steps_trained
    return result
