import abc
from typing import Any, Dict, List, Type, Union

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.typing import TensorType


@ExperimentalAPI
class BCRLModule(RLModule, abc.ABC):
    @override(RLModule)
    def setup(self):
        # __sphinx_doc_begin__
        catalog = self.config.get_catalog()

        # Build models from catalog
        self.encoder = catalog.build_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

    @override(RLModule)
    def get_train_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_inference_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_initial_state(self) -> Union[dict, List[TensorType]]:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        else:
            return {}

    @override(RLModule)
    def output_specs_inference(self) -> SpecType:
        return self.output_specs_exploration()

    @override(RLModule)
    def output_specs_exploration(self) -> SpecType:
        return [Columns.ACTION_DIST_INPUTS]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return self.output_specs_exploration()

    @override(RLModule)
    def _forward_inference(self, batch: Dict, **kwargs) -> Dict[str, Any]:
        """BC forward pass during inference.

        See the `BCTorchRLModule._forward_exploration` method for
        implementation details.
        """
        return self._forward_exploration(batch)

    @override(RLModule)
    def _forward_exploration(self, batch: Dict, **kwargs) -> Dict[str, Any]:
        """BC forward pass during exploration.

        Besides the action distribution this method also returns a possible
        state in case a stateful encoder is used.

        Note that for BC `_forward_train`, `_forward_exploration`, and
        `_forward_inference` return the same items and therefore only
        `_forward_exploration` is implemented and is used by the two other
        forward methods.
        """
        output = {}

        # State encodings.
        encoder_outs = self.encoder(batch)
        if Columns.STATE_OUT in encoder_outs:
            output[Columns.STATE_OUT] = encoder_outs[Columns.STATE_OUT]

        # Actions.
        action_logits = self.pi(encoder_outs[ENCODER_OUT])
        output[Columns.ACTION_DIST_INPUTS] = action_logits

        return output

    @override(RLModule)
    def _forward_train(self, batch: Dict, **kwargs) -> Dict[str, Any]:
        """BC forward pass during training.

        See the `BCTorchRLModule._forward_exploration` method for
        implementation details.
        """
        return self._forward_exploration(batch)
