import abc
from typing import Any, Dict, List, Tuple

from ray.rllib.algorithms.ppo.default_ppo_rl_module import DefaultPPORLModule
from ray.rllib.core.learner.utils import make_target_network
from ray.rllib.core.models.base import ACTOR
from ray.rllib.core.models.tf.encoder import ENCODER_OUT
from ray.rllib.core.rl_module.apis import (
    TARGET_NETWORK_ACTION_DIST_INPUTS,
    TargetNetworkAPI,
)
from ray.rllib.utils.typing import NetworkType

from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DefaultAPPORLModule(DefaultPPORLModule, TargetNetworkAPI, abc.ABC):
    """Default RLModule used by APPO, if user does not specify a custom RLModule.

    Users who want to train their RLModules with APPO may implement any RLModule
    (or TorchRLModule) subclass as long as the custom class also implements the
    `ValueFunctionAPI` (see ray.rllib.core.rl_module.apis.value_function_api.py)
    and the `TargetNetworkAPI` (see
    ray.rllib.core.rl_module.apis.target_network_api.py).
    """

    @override(TargetNetworkAPI)
    def make_target_networks(self):
        self._old_encoder = make_target_network(self.encoder)
        self._old_pi = make_target_network(self.pi)

    @override(TargetNetworkAPI)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        return [
            (self.encoder, self._old_encoder),
            (self.pi, self._old_pi),
        ]

    @override(TargetNetworkAPI)
    def forward_target(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        old_pi_inputs_encoded = self._old_encoder(batch)[ENCODER_OUT][ACTOR]
        old_action_dist_logits = self._old_pi(old_pi_inputs_encoded)
        return {TARGET_NETWORK_ACTION_DIST_INPUTS: old_action_dist_logits}

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(DefaultPPORLModule)
    def get_non_inference_attributes(self) -> List[str]:
        # Get the NON inference-only attributes from the parent class.
        ret = super().get_non_inference_attributes()
        # Add the two (APPO) target networks to it (NOT needed in
        # inference-only mode).
        ret += ["_old_encoder", "_old_pi"]
        return ret
