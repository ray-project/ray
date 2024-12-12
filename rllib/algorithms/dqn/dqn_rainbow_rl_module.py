import abc
from typing import Any, Dict, List, Tuple, Union

from ray.rllib.algorithms.sac.sac_learner import QF_PREDS
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.utils import make_target_network
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.apis import InferenceOnlyAPI, TargetNetworkAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import NetworkType, TensorType
from ray.util.annotations import DeveloperAPI


ATOMS = "atoms"
QF_LOGITS = "qf_logits"
QF_NEXT_PREDS = "qf_next_preds"
QF_PROBS = "qf_probs"
QF_TARGET_NEXT_PREDS = "qf_target_next_preds"
QF_TARGET_NEXT_PROBS = "qf_target_next_probs"


@DeveloperAPI(stability="alpha")
class DQNRainbowRLModule(RLModule, InferenceOnlyAPI, TargetNetworkAPI):
    @override(RLModule)
    def setup(self):
        if self.catalog is None and hasattr(self, "_catalog_ctor_error"):
            raise self._catalog_ctor_error

        # If a dueling architecture is used.
        self.uses_dueling: bool = self.model_config.get("dueling")
        # If double Q learning is used.
        self.uses_double_q: bool = self.model_config.get("double_q")
        # If we use noisy layers.
        self.uses_noisy: bool = self.model_config.get("noisy")
        # If we use a noisy encoder.
        self.uses_noisy_encoder: bool = False
        # The number of atoms for a distribution support.
        self.num_atoms: int = self.model_config.get("num_atoms")
        # If distributional learning is requested configure the support.
        if self.num_atoms > 1:
            self.v_min: float = self.model_config.get("v_min")
            self.v_max: float = self.model_config.get("v_max")
        # In case of noisy networks no need for epsilon greedy (see DQN Rainbow
        # paper).
        if not self.uses_noisy:
            # The epsilon scheduler for epsilon greedy exploration.
            self.epsilon_schedule = Scheduler(
                fixed_value_or_schedule=self.model_config["epsilon"],
                framework=self.framework,
            )

        # Build the encoder for the advantage and value streams. Note,
        # the same encoder is used.
        # Note further, by using the base encoder the correct encoder
        # is chosen for the observation space used.
        self.encoder = self.catalog.build_encoder(framework=self.framework)

        # Build heads.
        self.af = self.catalog.build_af_head(framework=self.framework)
        if self.uses_dueling:
            # If in a dueling setting setup the value function head.
            self.vf = self.catalog.build_vf_head(framework=self.framework)

    @override(TargetNetworkAPI)
    def make_target_networks(self) -> None:
        self._target_encoder = make_target_network(self.encoder)
        self._target_af = make_target_network(self.af)
        if self.uses_dueling:
            self._target_vf = make_target_network(self.vf)

    @override(InferenceOnlyAPI)
    def get_non_inference_attributes(self) -> List[str]:
        return ["_target_encoder", "_target_af"] + (
            ["_target_vf"] if self.uses_dueling else []
        )

    @override(TargetNetworkAPI)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        return [(self.encoder, self._target_encoder), (self.af, self._target_af)] + (
            # If we have a dueling architecture we need to update the value stream
            # target, too.
            [
                (self.vf, self._target_vf),
            ]
            if self.uses_dueling
            else []
        )

    @override(TargetNetworkAPI)
    def forward_target(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Computes Q-values from the target network.

        Note, these can be accompanied with logits and probabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.

        Args:
            batch: The batch received in the forward pass.

        Results:
            A dictionary containing the target Q-value predictions ("qf_preds")
            and in case of distributional Q-learning in addition to the target
            Q-value predictions ("qf_preds") the support atoms ("atoms"), the target
            Q-logits  ("qf_logits"), and the probabilities ("qf_probs").
        """
        # If we have a dueling architecture we have to add the value stream.
        return self._qf_forward_helper(
            batch,
            self._target_encoder,
            (
                {"af": self._target_af, "vf": self._target_vf}
                if self.uses_dueling
                else self._target_af
            ),
        )

    # TODO (simon): DQN Rainbow does not support RNNs, yet.
    @override(RLModule)
    def get_initial_state(self) -> Any:
        return {}

    @override(RLModule)
    def input_specs_exploration(self) -> SpecType:
        return [Columns.OBS]

    @override(RLModule)
    def input_specs_inference(self) -> SpecType:
        return [Columns.OBS]

    @override(RLModule)
    def input_specs_train(self) -> SpecType:
        return [
            Columns.OBS,
            Columns.ACTIONS,
            Columns.NEXT_OBS,
        ]

    @override(RLModule)
    def output_specs_exploration(self) -> SpecType:
        return [Columns.ACTIONS]

    @override(RLModule)
    def output_specs_inference(self) -> SpecType:
        return [Columns.ACTIONS]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return [
            QF_PREDS,
            QF_TARGET_NEXT_PREDS,
            # Add keys for double-Q setup.
            *([QF_NEXT_PREDS] if self.uses_double_q else []),
            # Add keys for distributional Q-learning.
            *(
                [
                    ATOMS,
                    QF_LOGITS,
                    QF_PROBS,
                    QF_TARGET_NEXT_PROBS,
                ]
                # We add these keys only when learning a distribution.
                if self.num_atoms > 1
                else []
            ),
        ]

    def _qf(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Computes Q-values, given encoder, q-net and (optionally), advantage net.

        Note, these can be accompanied by logits and probabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.

        Args:
            batch: The batch received in the forward pass.

        Results:
            A dictionary containing the Q-value predictions ("qf_preds")
            and in case of distributional Q-learning in addition to the Q-value
            predictions ("qf_preds") the support atoms ("atoms"), the Q-logits
            ("qf_logits"), and the probabilities ("qf_probs").
        """
        # If we have a dueling architecture we have to add the value stream.
        return self._qf_forward_helper(
            batch,
            self.encoder,
            {"af": self.af, "vf": self.vf} if self.uses_dueling else self.af,
        )

    @abc.abstractmethod
    @OverrideToImplementCustomLogic
    def _af_dist(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Compute the advantage distribution.

        Note this distribution is identical to the Q-distribution in
        case no dueling architecture is used.

        Args:
            batch: A dictionary containing a tensor with the outputs of the
                forward pass of the Q-head or advantage stream head.

        Returns:
            A `dict` containing the support of the discrete distribution for
            either Q-values or advantages (in case of a dueling architecture),
            ("atoms"), the logits per action and atom and the probabilities
            of the discrete distribution (per action and atom of the support).
        """

    @abc.abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_forward_helper(
        self,
        batch: Dict[str, TensorType],
        encoder: Encoder,
        head: Union[Model, Dict[str, Model]],
    ) -> Dict[str, TensorType]:
        """Computes Q-values.

        This is a helper function that takes care of all different cases,
        i.e. if we use a dueling architecture or not and if we use distributional
        Q-learning or not.

        Args:
            batch: The batch received in the forward pass.
            encoder: The encoder network to use. Here we have a single encoder
                for all heads (Q or advantages and value in case of a dueling
                architecture).
            head: Either a head model or a dictionary of head model (dueling
            architecture) containing advantage and value stream heads.

        Returns:
            In case of expectation learning the Q-value predictions ("qf_preds")
            and in case of distributional Q-learning in addition to the predictions
            the atoms ("atoms"), the Q-value predictions ("qf_preds"), the Q-logits
            ("qf_logits") and the probabilities for the support atoms ("qf_probs").
        """

    @abc.abstractmethod
    @OverrideToImplementCustomLogic
    def _reset_noise(self, target: bool = False):
        """Resets the noise for the noisy layers.

        In case of `uses_noisy=True` the noise of the noisy layers needs to be reset
        at each exploration step and before each training loop. This method is used
        to reset the noise at specific points in the `Algorithm.training_step()` or
        'RLModule.forward()` methods. For customization of noise resetting this
        function can be overridden (e.g. for resetting the noise at the beginning of
        each episode or at the beginning of each rollout).

        Args:
            target: If `True` the target network is reset.
        """
