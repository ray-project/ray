from abc import abstractmethod
from typing import Any, Dict, List, Tuple

from ray.rllib.algorithms.sac.sac_learner import (
    ACTION_DIST_INPUTS_NEXT,
    QF_PREDS,
    QF_TWIN_PREDS,
)
from ray.rllib.core.learner.utils import make_target_network
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.apis import InferenceOnlyAPI, TargetNetworkAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.typing import NetworkType
from ray.util.annotations import DeveloperAPI


@DeveloperAPI(stability="alpha")
class SACRLModule(RLModule, InferenceOnlyAPI, TargetNetworkAPI):
    """`RLModule` for the Soft-Actor-Critic (SAC) algorithm.

    It consists of several architectures, each in turn composed of
    two networks: an encoder and a head.

    The policy (actor) contains a state encoder (`pi_encoder`) and
    a head (`pi_head`) that feeds into an action distribution (a
    squashed Gaussian, i.e. outputs define the location and the log
    scale parameters).

    In addition, two (or four in case `twin_q=True`) Q networks are
    defined, the second one (and fourth, if `twin_q=True`) of them the
    Q target network(s). All of these in turn are - similar to the
    policy network - composed of an encoder and a head network. Each of
    the encoders forms a state-action encoding that feeds into the
    corresponding value heads to result in an estimation of the soft
    action-value of SAC.

    The following graphics show the forward passes through this module:
    [obs] -> [pi_encoder] -> [pi_head] -> [action_dist_inputs]
    [obs, action] -> [qf_encoder] -> [qf_head] -> [q-value]
    [obs, action] -> [qf_target_encoder] -> [qf_target_head]
    -> [q-target-value]
    ---
    If `twin_q=True`:
    [obs, action] -> [qf_twin_encoder] -> [qf_twin_head] -> [q-twin-value]
    [obs, action] -> [qf_target_twin_encoder] -> [qf_target_twin_head]
    -> [q-target-twin-value]
    """

    @override(RLModule)
    def setup(self):
        # If a twin Q architecture should be used.
        self.twin_q = self.config.model_config_dict["twin_q"]

        # Build the encoder for the policy.
        self.pi_encoder = self.catalog.build_encoder(framework=self.framework)

        if not self.config.inference_only or self.framework != "torch":
            # SAC needs a separate Q network encoder (besides the pi network).
            # This is because the Q network also takes the action as input
            # (concatenated with the observations).
            self.qf_encoder = self.catalog.build_qf_encoder(framework=self.framework)

            # If necessary, build also a twin Q encoders.
            if self.twin_q:
                self.qf_twin_encoder = self.catalog.build_qf_encoder(
                    framework=self.framework
                )

        # Build heads.
        self.pi = self.catalog.build_pi_head(framework=self.framework)

        if not self.config.inference_only or self.framework != "torch":
            self.qf = self.catalog.build_qf_head(framework=self.framework)
            # If necessary build also a twin Q heads.
            if self.twin_q:
                self.qf_twin = self.catalog.build_qf_head(framework=self.framework)

    @override(TargetNetworkAPI)
    def make_target_networks(self):
        self.target_qf_encoder = make_target_network(self.qf_encoder)
        self.target_qf = make_target_network(self.qf)
        if self.twin_q:
            self.target_qf_twin_encoder = make_target_network(self.qf_twin_encoder)
            self.target_qf_twin = make_target_network(self.qf_twin)

    @override(InferenceOnlyAPI)
    def get_non_inference_attributes(self) -> List[str]:
        ret = ["qf", "target_qf", "qf_encoder", "target_qf_encoder"]
        if self.twin_q:
            ret += [
                "qf_twin",
                "target_qf_twin",
                "qf_twin_encoder",
                "target_qf_twin_encoder",
            ]
        return ret

    @override(TargetNetworkAPI)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        """Returns target Q and Q network(s) to update the target network(s)."""
        return [
            (self.qf_encoder, self.target_qf_encoder),
            (self.qf, self.target_qf),
        ] + (
            # If we have twin networks we need to update them, too.
            [
                (self.qf_twin_encoder, self.target_qf_twin_encoder),
                (self.qf_twin, self.target_qf_twin),
            ]
            if self.twin_q
            else []
        )

    # TODO (simon): SAC does not support RNNs, yet.
    @override(RLModule)
    def get_initial_state(self) -> dict:
        # if hasattr(self.pi_encoder, "get_initial_state"):
        #     return {
        #         ACTOR: self.pi_encoder.get_initial_state(),
        #         CRITIC: self.qf_encoder.get_initial_state(),
        #         CRITIC_TARGET: self.qf_target_encoder.get_initial_state(),
        #     }
        # else:
        #     return {}
        return {}

    @override(RLModule)
    def input_specs_exploration(self) -> SpecType:
        return [SampleBatch.OBS]

    @override(RLModule)
    def input_specs_inference(self) -> SpecType:
        return [SampleBatch.OBS]

    @override(RLModule)
    def input_specs_train(self) -> SpecType:
        return [
            SampleBatch.OBS,
            SampleBatch.ACTIONS,
            SampleBatch.NEXT_OBS,
        ]

    @override(RLModule)
    def output_specs_exploration(self) -> SpecType:
        return [SampleBatch.ACTION_DIST_INPUTS]

    @override(RLModule)
    def output_specs_inference(self) -> SpecType:
        return [SampleBatch.ACTION_DIST_INPUTS]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return (
            [
                QF_PREDS,
                SampleBatch.ACTION_DIST_INPUTS,
                ACTION_DIST_INPUTS_NEXT,
            ]
            + [QF_TWIN_PREDS]
            if self.twin_q
            else []
        )

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_forward_train_helper(
        self, batch: Dict[str, Any], encoder: Encoder, head: Model
    ) -> Dict[str, Any]:
        """Executes the forward pass for Q networks.

        Args:
            batch: Dict containing a concatenated tensor with observations
                and actions under the key `SampleBatch.OBS`.
            encoder: An `Encoder` model for the Q state-action encoder.
            head: A `Model` for the Q head.

        Returns:
            The estimated Q-value using the `encoder` and `head` networks.
        """
