import logging
from typing import Dict, Any
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import SampleBatchType
from gym.spaces import Discrete
import numpy as np

from ray.rllib.offline.estimators.direct_method import DirectMethod

torch, nn = try_import_torch()

logger = logging.getLogger()


@DeveloperAPI
class DMTrainable(DirectMethod):
    """The Direct Method estimator with a trainable Q-model.

    DM estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    @override(DirectMethod)
    def __init__(
        self,
        name: str,
        policy: Policy,
        gamma: float,
        q_model_config: Dict = None,
    ):
        """
        Initializes a Direct Method OPE Estimator.

        Args:
            name: string to save OPE results under
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
            q_model_type: Either "fqe" for Fitted Q-Evaluation
                or "qreg" for Q-Regression, or a custom model that implements:
                - `estimate_q(states, actions)`
                - `estimate_v(states, action_probs)`
            q_model_config: Arguments to specify the Q-model.
        """

        super().__init__(name, policy, gamma)
        q_model_config = q_model_config or {"type": "fqe"}
        model_cls = q_model_config.pop("type")
        # TODO (Rohan138): Add support for continuous action spaces
        assert isinstance(
            policy.action_space, Discrete
        ), "DM Estimator only supports discrete action spaces!"
        assert (
            policy.config["batch_mode"] == "complete_episodes"
        ), "DM Estimator only supports `batch_mode`=`complete_episodes`"

        self.model = model_cls(
            policy=policy,
            gamma=gamma,
            **q_model_config,
        )
        assert hasattr(
            self.model, "estimate_v"
        ), "self.model must implement `estimate_v`!"
        self.state_value_fn = lambda policy, batch: self.model.estimate_v(batch)

    def train(self, batch: SampleBatchType) -> Dict[str, Any]:
        losses = self.model.train(batch)
        return {self.name + "_loss", np.mean(losses)}
