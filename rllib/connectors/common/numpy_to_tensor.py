from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import EpisodeType


class NumpyToTensor(ConnectorV2):
    """Converts numpy arrays across the entire input data into (framework) tensors.

    The framework information is received via the provided `rl_module` arg in the
    `__call__`.
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        as_learner_connector: bool = False,
        pin_mempory: Optional[bool] = None,
        device: Optional[str] = None,
        **kwargs,
    ):
        """Initializes a NumpyToTensor instance.

        Args:
            as_learner_connector: Whether this ConnectorV2 piece is used inside a
                LearnerConnectorPipeline or not.
            pin_mempory: Whether to pin memory when creating (torch) tensors.
                If None (default), pins memory if `as_learner_connector` is True,
                otherwise doesn't pin memory.
            **kwargs:
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )
        self._as_learner_connector = as_learner_connector
        self._pin_memory = (
            pin_mempory if pin_mempory is not None else self._as_learner_connector
        )
        self._device = device

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Optional[Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        is_single_agent = False
        is_marl_module = isinstance(rl_module, MultiAgentRLModule)
        # `data` already a ModuleID to batch mapping format.
        if not (is_marl_module and all(c in rl_module._rl_modules for c in data)):
            is_single_agent = True
            data = {DEFAULT_MODULE_ID: data}

        # TODO (sven): Support specifying a device (e.g. GPU).
        for module_id, module_data in data.copy().items():
            infos = module_data.pop(Columns.INFOS, None)
            if rl_module.framework == "torch":
                module_data = convert_to_torch_tensor(
                    module_data, pin_memory=self._pin_memory, device=self._device
                )
            else:
                raise ValueError(
                    "`NumpyToTensor`does NOT support frameworks other than torch!"
                )
            if infos is not None:
                module_data[Columns.INFOS] = infos
            # Early out with data under(!) `DEFAULT_MODULE_ID`, b/c we are in plain
            # single-agent mode.
            if is_single_agent:
                return module_data
            data[module_id] = module_data

        return data
