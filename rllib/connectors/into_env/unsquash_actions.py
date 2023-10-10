from typing import Any

from ray.rllib.connectors.connector import Connector, ConnectorContextV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import (
    get_base_struct_from_space,
    unsquash_action,
)
from ray.rllib.utils.typing import ActionConnectorDataType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class UnsquashActions(Connector):
    """A connector that unsquashes actions in input data, according to action space.

    Useful for mapping RLModule action outputs (normalized between -1.0 and 1.0) to an
    env's action space. Unsquashing the outputs results in cont. action component values
    between the given Space's bounds (`low` and `high`). This only applies to Box
    components within the action space, whose dtype is float32 or float64.

    input_type: INPUT_OUTPUT_TYPE.DATA (operates directly on all leaves of the input
        and searches for the SampleBatch.ACTIONS key to normalize all actions (Box
        components) in place).
    output_type: INPUT_OUTPUT_TYPE.DATA (same as input: data in, data out).
    """

    def __init__(self, ctx=None, *, action_space, **kwargs):
        super().__init__(action_space=action_space, **kwargs)
        self._action_space_struct = get_base_struct_from_space(self.action_space)

    @override(Connector)
    def __call__(self, input_: Any, ctx: ConnectorContextV2) -> Any:
        # Convert actions in-place using the `unsquash_action` utility.
        input_[SampleBatch.ACTIONS] = unsquash_action(
            input_[SampleBatch.ACTIONS], self._action_space_struct
        )
        return input_

    # @override(Connector)
    # def serialize(self):
    #    return ClipActions.__name__, None

    # @staticmethod
    # TODO
    # def from_state(ctx: ConnectorContext, params: Any):
    #    return ClipActions(ctx)
