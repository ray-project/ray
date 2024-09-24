import abc

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module import RLModule
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.utils.annotations import override
from ray.util.annotations import DeveloperAPI


@DeveloperAPI(stability="alpha")
class MARWILRLModule(RLModule, ValueFunctionAPI, abc.ABC):
    def setup(self):
        # Build models from catalog
        self.encoder = self.catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = self.catalog.build_pi_head(framework=self.framework)

        # Build the value head.
        self.vf = self.catalog.build_vf_head(framework=self.framework)

    @override(RLModule)
    def get_initial_state(self) -> dict:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        else:
            return {}

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        return [Columns.OBS]

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return [Columns.ACTION_DIST_INPUTS]

    @override(RLModule)
    def input_specs_exploration(self):
        return self.input_specs_inference()

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        return self.output_specs_inference()

    @override(RLModule)
    def input_specs_train(self) -> SpecDict:
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        return [
            Columns.VF_PREDS,
            Columns.ACTION_DIST_INPUTS,
        ]
