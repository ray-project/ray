from typing import Any

from ray.rllib.connectors.connector import Connector, ConnectorContextV2
from ray.rllib.connectors.input_output_types import INPUT_OUTPUT_TYPE
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import clip_action, get_base_struct_from_space
from ray.rllib.utils.typing import ActionConnectorDataType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ClipActions(Connector):
    """A connector that clips actions in input data, according to action space.
    
    input_type: INPUT_OUTPUT_TYPE.DATA (operates directly on all leaves of the input
        and searches for the SampleBatch.ACTIONS key to clip all actions (Box components)
        in place).
    output_type: INPUT_OUTPUT_TYPE.DATA (same as input: data in, data out).
    """
    def __init__(self, ctx=None, *, action_space, **kwargs):
        super().__init__(action_space=action_space, **kwargs)
        self._action_space_struct = get_base_struct_from_space(self.action_space)

    @override(Connector)
    def __call__(self, input_: Any, ctx: ConnectorContextV2) -> Any:
        # Convert actions in-place using the `clip_action` utility.
        input_[SampleBatch.ACTIONS] = clip_action(
            input_[SampleBatch.ACTIONS], self._action_space_struct
        )
        return input_

    #@override(Connector)
    #def serialize(self):
    #    return ClipActions.__name__, None

    #@staticmethod
    #TODO
    #def from_state(ctx: ConnectorContext, params: Any):
    #    return ClipActions(ctx)
