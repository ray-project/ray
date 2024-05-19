from abc import abstractmethod
from typing import Any, Dict, Type
from ray.rllib.algorithms.sac.sac_catalog import SACCatalog

# from ray.rllib.algorithms.sac.sac_learner import QF_PREDS
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    override,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.nested_dict import NestedDict

CRITIC_TARGET = "critic_target"
QF_PREDS = "qf_preds"
QF_NEXT_PREDS = "qf_next_preds"
QF_TWIN_PREDS = "qf_twin_preds"
ACTION_DIST_INPUTS_NEXT = "action_dist_inputs_next"


@ExperimentalAPI
class SACRLModule(RLModule, RLModuleWithTargetNetworksInterface):
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
        # Get the SAC catalog.
        catalog: SACCatalog = self.config.get_catalog()

        # If a twin Q architecture should be used.
        self.twin_q = self.config.model_config_dict["twin_q"]

        # Build the encoder for the policy.
        self.pi_encoder = catalog.build_encoder(framework=self.framework)

        if not self.inference_only or self.framework != "torch":
            # SAC needs a separate Q network encoder (besides the pi network).
            # This is because the Q network also takes the action as input
            # (concatenated with the observations).
            self.qf_encoder = catalog.build_qf_encoder(framework=self.framework)

            # Build the target Q encoder as an exact copy of the Q encoder.
            # TODO (simon): Maybe merging encoders together for target and qf
            # and keep only the heads differently?
            self.qf_target_encoder = catalog.build_qf_encoder(framework=self.framework)
            # If necessary, build also a twin Q encoders.
            if self.twin_q:
                self.qf_twin_encoder = catalog.build_qf_encoder(
                    framework=self.framework
                )
                self.qf_target_twin_encoder = catalog.build_qf_encoder(
                    framework=self.framework
                )
            # Holds the parameter names to be removed or renamed when synching
            # from the learner to the inference module.
            self._inference_only_state_dict_keys = {}

        # Build heads.
        self.pi = catalog.build_pi_head(framework=self.framework)

        if not self.inference_only or self.framework != "torch":
            self.qf = catalog.build_qf_head(framework=self.framework)
            # The Q target network head is an identical copy of the Q network head.
            self.qf_target = catalog.build_qf_head(framework=self.framework)
            # If necessary build also a twin Q heads.
            if self.twin_q:
                self.qf_twin = catalog.build_qf_head(framework=self.framework)
                self.qf_target_twin = catalog.build_qf_head(framework=self.framework)

            # We do not want to train the target network.
            self.qf_target_encoder.trainable = False
            self.qf_target.trainable = False
            if self.twin_q:
                self.qf_target_twin_encoder.trainable = False
                self.qf_target_twin.trainable = False

        # Get the action distribution class.
        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

    @override(RLModule)
    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_inference_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_train_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

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
    def _qf_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through Q network.

        Note, this is only used in training.
        """

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_twin_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through twin Q network.

        Note, this is only used in training and only if `twin_q=True`.
        """

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_target_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through target Q network.

        Note, this is only used in training.
        """

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_target_twin_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through twin target Q network.

        Note, this is only used in training and only if `twin_q=True`.
        """

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_forward_train_helper(
        self, batch: NestedDict, encoder: Encoder, head: Model
    ) -> Dict[str, Any]:
        """Executes the forward pass for Q networks.

        Args:
            batch: NestedDict containing a concatencated tensor with observations
                and actions under the key `SampleBatch.OBS`.
            encoder: An `Encoder` model for the Q state-action encoder.
            head: A `Model` for the Q head.

        Returns:
            A `dict` cotnaining the estimated Q-values in the key `QF_PREDS`.
        """
