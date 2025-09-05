import abc
from typing import List

from ray.rllib.core.models.configs import RecurrentEncoderConfig
from ray.rllib.core.rl_module.apis import InferenceOnlyAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DefaultMAPPORLModule(RLModule, InferenceOnlyAPI, abc.ABC):
    """Default RLModule used by PPO, if user does not specify a custom RLModule.

    Users who want to train their RLModules with PPO may implement any RLModule
    (or TorchRLModule) subclass as long as the custom class also implements the
    `ValueFunctionAPI` (see ray.rllib.core.rl_module.apis.value_function_api.py)
    """

    @override(RLModule)
    def setup(self):
        try:
          # __sphinx_doc_begin__
          # If we have a stateful model, states for the critic need to be collected
          # during sampling and `inference-only` needs to be `False`. Note, at this
          # point the encoder is not built, yet and therefore `is_stateful()` does
          # not work.
          is_stateful = isinstance(
              self.catalog.encoder_config.base_encoder_config,
              RecurrentEncoderConfig,
          )
          if is_stateful:
              self.inference_only = False
          # If this is an `inference_only` Module, we'll have to pass this information
          # to the encoder config as well.
          if self.inference_only and self.framework == "torch":
              self.catalog.encoder_config.inference_only = True
          # Build models from catalog.
          self.encoder = self.catalog.build_encoder(framework=self.framework)
          self.pi = self.catalog.build_pi_head(framework=self.framework)
        except Exception as e:
          print("Error in DefaultMAPPORLModule setup")
          print(e)
          raise e
        # __sphinx_doc_end__

    @override(RLModule)
    def get_initial_state(self) -> dict:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        else:
            return {}

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(InferenceOnlyAPI)
    def get_non_inference_attributes(self) -> List[str]:
        """Return attributes, which are NOT inference-only (only used for training)."""
        return []