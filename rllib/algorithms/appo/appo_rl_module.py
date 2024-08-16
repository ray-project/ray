import abc
from typing import Any, Dict, List, Tuple

from ray.rllib.algorithms.ppo.ppo_rl_module import PPORLModule
from ray.rllib.algorithms.appo.appo import OLD_ACTION_DIST_LOGITS_KEY
from ray.rllib.core.learner.utils import make_target_network
from ray.rllib.core.models.base import ACTOR
from ray.rllib.core.models.tf.encoder import ENCODER_OUT
from ray.rllib.core.rl_module.apis.target_network_api import TargetNetworkAPI
from ray.rllib.utils.typing import NetworkType

from ray.rllib.utils.annotations import override


class APPORLModule(PPORLModule, TargetNetworkAPI, abc.ABC):
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
        return {OLD_ACTION_DIST_LOGITS_KEY: old_action_dist_logits}
