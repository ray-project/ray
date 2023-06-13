from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.core.learner.learner import (
    Learner,
    LearnerHyperparameters,
)
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ResultDict


LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY = "curr_entropy_coeff"


@dataclass
class ImpalaLearnerHyperparameters(LearnerHyperparameters):
    """LearnerHyperparameters for the ImpalaLearner sub-classes (framework specific).

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
    vf_loss_coeff: float = None
    entropy_coeff: float = None
    entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = None


class ImpalaLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Dict mapping module IDs to the respective entropy Scheduler instance.
        self.entropy_coeff_schedulers_per_module: Dict[
            ModuleID, Scheduler
        ] = LambdaDefaultDict(
            lambda module_id: Scheduler(
                fixed_value_or_schedule=(
                    self.hps.get_hps_for_module(module_id).entropy_coeff
                ),
                framework=self.framework,
                device=self._device,
            )
        )

    @override(Learner)
    def remove_module(self, module_id: str):
        super().remove_module(module_id)
        self.entropy_coeff_schedulers_per_module.pop(module_id)

    @override(Learner)
    def additional_update_for_module(
        self, *, module_id: ModuleID, hps: ImpalaLearnerHyperparameters, timestep: int
    ) -> Dict[str, Any]:
        results = super().additional_update_for_module(
            module_id=module_id, hps=hps, timestep=timestep
        )

        # Update entropy coefficient via our Scheduler.
        new_entropy_coeff = self.entropy_coeff_schedulers_per_module[module_id].update(
            timestep=timestep
        )
        results.update({LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY: new_entropy_coeff})

        return results


def _reduce_impala_results(results: List[ResultDict]) -> ResultDict:
    """Reduce/Aggregate a list of results from Impala Learners.

    Average the values of the result dicts. Add keys for the number of agent and env
    steps trained (on all modules).

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
