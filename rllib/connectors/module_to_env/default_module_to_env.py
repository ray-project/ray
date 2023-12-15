from typing import Any, List, Optional

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class DefaultModuleToEnv(ConnectorV2):
    """Default connector piece added by RLlib to the end of any module-to-env pipeline.

    If necessary, this connector samples actions, given action dist. inputs and a
    dist. class.
    The connector will only sample from the action distribution, if the
    SampleBatch.ACTIONS key cannot be found in `input_`. Otherwise, it'll behave
    as pass through (noop). If SampleBatch.ACTIONS is not present, but
    SampleBatch.ACTION_DIST_INPUTS are, the connector will create a new action
    distribution using the RLModule in the connector context and sample from this
    distribution (deterministically, if we are not exploring, stochastically, if we
    are).

    input_type: INPUT_OUTPUT_TYPES.DICT_OF_MODULE_IDS_TO_DATA
        Operates per RLModule as it will have to pull the action distribution from each
        in order to sample actions if necessary. Searches for the ACTIONS and
        ACTION_DIST_INPUTS keys in a module's outputs and - should ACTIONS not be
        found - sample actions from the module's action distribution.
    output_type: INPUT_OUTPUT_TYPES.DICT_OF_MODULE_IDS_TO_DATA (same as input: data in,
        data out, however, data
        out might contain an additional ACTIONS key if it was not previously present
        in the input).
    """

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        input_: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        persistent_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:

        # Loop through all modules that created some output.
        # for mid in input_.keys():
        #    sa_module = ctx.rl_module.get_module(module_id=mid)

        # If our RLModule is stateful, remove the T=1 axis from all model outputs
        # (except the state outs, which never have this extra time axis).
        if rl_module.is_stateful():
            state = input_.pop(STATE_OUT, None)
            input_ = tree.map_structure(lambda s: np.squeeze(s, axis=1), input_)
            if state:
                input_[STATE_OUT] = state

        # ACTION_DIST_INPUTS field returned by `forward_exploration|inference()` ->
        # Create a new action distribution object.
        action_dist = None
        if SampleBatch.ACTION_DIST_INPUTS in input_:
            if explore:
                action_dist_class = rl_module.get_exploration_action_dist_cls()
            else:
                action_dist_class = rl_module.get_inference_action_dist_cls()
            action_dist = action_dist_class.from_logits(
                input_[SampleBatch.ACTION_DIST_INPUTS]
            )

            # TODO (sven): Should this not already be taken care of by RLModule's
            #  `get_...action_dist_cls()` methods?
            if not explore:
                action_dist = action_dist.to_deterministic()

        # If `forward_...()` returned actions, use them here as-is.
        if SampleBatch.ACTIONS in input_:
            actions = input_[SampleBatch.ACTIONS]
        # Otherwise, sample actions from the distribution.
        else:
            if action_dist is None:
                raise KeyError(
                    "Your RLModule's `forward_[exploration|inference]()` methods must "
                    f"return a dict with either the '{SampleBatch.ACTIONS}' key or "
                    f"the '{SampleBatch.ACTION_DIST_INPUTS}' key in it (or both)!"
                )
            actions = action_dist.sample()
            input_[SampleBatch.ACTIONS] = actions

        # For convenience and if possible, compute action logp from distribution
        # and add to output.
        if action_dist is not None and SampleBatch.ACTION_LOGP not in input_:
            input_[SampleBatch.ACTION_LOGP] = action_dist.logp(actions)

        return input_

    # @override(Connector)
    # def serialize(self):
    #    return ClipActions.__name__, None

    # @staticmethod
    # TODO
    # def from_state(ctx: ConnectorContext, params: Any):
    #    return ClipActions(ctx)
