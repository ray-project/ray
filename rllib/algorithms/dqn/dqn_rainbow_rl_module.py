from abc import abstractmethod
from typing import Any, Dict, Type, Union
from ray.rllib.algorithms.dqn.dqn_rainbow_catalog import DQNRainbowCatalog
from ray.rllib.algorithms.sac.sac_learner import QF_PREDS
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    override,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import TensorType

ATOMS = "atoms"
QF_LOGITS = "qf_logits"
QF_NEXT_PREDS = "qf_next_preds"
QF_PROBS = "qf_probs"
QF_TARGET_NEXT_PREDS = "qf_target_next_preds"
QF_TARGET_NEXT_PROBS = "qf_target_next_probs"


@ExperimentalAPI
class DQNRainbowRLModule(RLModule, RLModuleWithTargetNetworksInterface):
    @override(RLModule)
    def setup(self):
        # Get the DQN Rainbow catalog.
        catalog: DQNRainbowCatalog = self.config.get_catalog()

        # If a dueling architecture is used.
        self.uses_dueling: bool = self.config.model_config_dict.get("dueling")
        # If double Q learning is used.
        self.uses_double_q: bool = self.config.model_config_dict.get("double_q")
        # If we use noisy layers.
        self.uses_noisy: bool = self.config.model_config_dict.get("noisy")
        # If we use a noisy encoder.
        self.uses_noisy_encoder: bool = False
        # The number of atoms for a distribution support.
        self.num_atoms: int = self.config.model_config_dict.get("num_atoms")
        # If distributional learning is requested configure the support.
        if self.num_atoms > 1:
            self.v_min: float = self.config.model_config_dict.get("v_min")
            self.v_max: float = self.config.model_config_dict.get("v_max")
        # In case of noisy networks no need for epsilon greedy (see DQN Rainbow
        # paper).
        if not self.uses_noisy:
            # The epsilon scheduler for epsilon greedy exploration.
            self.epsilon_schedule = Scheduler(
                fixed_value_or_schedule=self.config.model_config_dict["epsilon"],
                framework=self.framework,
            )

        # Build the encoder for the advantage and value streams. Note,
        # the same encoder is used.
        # Note further, by using the base encoder the correct encoder
        # is chosen for the observation space used.
        self.encoder = catalog.build_encoder(framework=self.framework)
        # If not an inference-only module (e.g., for evaluation), set up the
        # target networks and state dict keys to be taken care of when syncing.
        if not self.inference_only or self.framework != "torch":
            # Build the same encoder for the target network(s).
            self.target_encoder = catalog.build_encoder(framework=self.framework)
            # Holds the parameter names to be removed or renamed when synching
            # from the learner to the inference module.
            self._inference_only_state_dict_keys = {}

        # Build heads.
        self.af = catalog.build_af_head(framework=self.framework)
        if self.uses_dueling:
            # If in a dueling setting setup the value function head.
            self.vf = catalog.build_vf_head(framework=self.framework)
        if not self.inference_only or self.framework != "torch":
            # Implement the same heads for the target network(s).
            self.af_target = catalog.build_af_head(framework=self.framework)
            if self.uses_dueling:
                # If in a dueling setting setup the target value function head.
                self.vf_target = catalog.build_vf_head(framework=self.framework)

        # Define the action distribution for sampling the exploit action
        # during exploration.
        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

    @override(RLModule)
    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        """Returns the action distribution class for exploration.

        Note, this class is used to sample the exploit action during
        exploration.
        """
        return self.action_dist_cls

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

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Computes Q-values.

        Note, these can be accompanied with logits and pobabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.

        Args:
            batch: The batch recevied in the forward pass.

        Results:
            A dictionary containing the Q-value predictions ("qf_preds")
            and in case of distributional Q-learning in addition to the Q-value
            predictions ("qf_preds") the support atoms ("atoms"), the Q-logits
            ("qf_logits"), and the probabilities ("qf_probs").
        """

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_target(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Computes Q-values from the target network.

        Note, these can be accompanied with logits and pobabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.

        Args:
            batch: The batch recevied in the forward pass.

        Results:
            A dictionary containing the target Q-value predictions ("qf_preds")
            and in case of distributional Q-learning in addition to the target
            Q-value predictions ("qf_preds") the support atoms ("atoms"), the target
            Q-logits  ("qf_logits"), and the probabilities ("qf_probs").
        """

    @abstractmethod
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

    @abstractmethod
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
            batch: The batch recevied in the forward pass.
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

    @abstractmethod
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
