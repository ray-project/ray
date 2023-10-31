from typing import Any

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_context_v2 import  ConnectorContextV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class DefaultModuleToEnv(ConnectorV2):
    """A connector that samples actions given action dist. inputs and a dist. class.

    The connector will only sample from the distribution, if the ACTIONS key
    cannot be found in the connector's input. Otherwise, it'll behave simply as pass
    through (noop). If ACTIONS is not present, but ACTION_DIST_INPUTS are, will create
    a distribution from the RLModule and sample from it (deterministically, if
    we are not exploring, stochastically, if we are).

    input_type: INPUT_OUTPUT_TYPES.DICT_OF_MODULE_IDS_TO_DATA
        Operates per RLModule as it will have to pull the action distribution from each
        in order to sample actions if necessary. Searches for the ACTIONS and
        ACTION_DIST_INPUTS keys in a module's outputs and - should ACTIONS not be found -
        sample actions from the module's action distribution.
    output_type: INPUT_OUTPUT_TYPES.DICT_OF_MODULE_IDS_TO_DATA (same as input: data in, data out, however, data
        out might contain an additional ACTIONS key if it was not previously present
        in the input).
    """

    @override(ConnectorV2)
    def __call__(self, input_: Any, episodes, ctx: ConnectorContextV2) -> Any:

        # Loop through all modules that created some output.
        for mid in input_.keys():
            sa_module = ctx.rl_module.get_module(module_id=mid)

            # ACTION_DIST_INPUTS field returned by `forward_exploration()` ->
            # Create a distribution object.
            action_dist = None
            # The RLModule has already computed actions.
            if (
                SampleBatch.ACTION_DIST_INPUTS in input_[mid]
                and SampleBatch.ACTION_LOGP not in input_[mid]
            ):
                dist_inputs = input_[mid][SampleBatch.ACTION_DIST_INPUTS]
                if ctx.explore:
                    action_dist_class = sa_module.get_exploration_action_dist_cls()
                else:
                    action_dist_class = sa_module.get_inference_action_dist_cls()
                action_dist = action_dist_class.from_logits(dist_inputs)
                if not ctx.explore:
                    action_dist = action_dist.to_deterministic()

            # If `forward_...()` returned actions, use them here as-is.
            if SampleBatch.ACTIONS in input_[mid]:
                actions = input_[mid][SampleBatch.ACTIONS]
            # Otherwise, sample actions from the distribution.
            else:
                if action_dist is None:
                    raise KeyError(
                        "Your RLModule's `forward_[explore|inference]()` methods must "
                        f"return a dict with either the {SampleBatch.ACTIONS} key or "
                        f"the {SampleBatch.ACTION_DIST_INPUTS} key in it (or both)!"
                    )
                actions = action_dist.sample()

            # Compute action-logp and action-prob from distribution and add to
            # output, if possible.
            if action_dist is not None and SampleBatch.ACTION_LOGP not in input_[mid]:
                input_[mid][SampleBatch.ACTION_LOGP] = action_dist.logp(actions)

        return input_

    # @override(Connector)
    # def serialize(self):
    #    return ClipActions.__name__, None

    # @staticmethod
    # TODO
    # def from_state(ctx: ConnectorContext, params: Any):
    #    return ClipActions(ctx)
