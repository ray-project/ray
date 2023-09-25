from typing import Optional
from ray.rllib.core.models.base import Encoder
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override


class TargetEncoder(Encoder):
    """Implements the logic for the Target network in DQN."""

    @override(Encoder)
    def get_input_specs(self) -> Optional[Spec]:
        """The Target network uses the next observations."""
        return [SampleBatch.NEXT_OBS]
