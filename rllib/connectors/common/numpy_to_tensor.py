from typing import Any, Dict, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class NumpyToTensor(ConnectorV2):
    """Converts numpy arrays across the entire input data into (framework) tensors.

    The framework information is received via the provided `rl_module` arg in the
    `__call__()` method.

    Note: This is one of the default env-to-module or Learner ConnectorV2 pieces that
    are added automatically by RLlib into every env-to-module/Learner connector
    pipeline, unless `config.add_default_connectors_to_env_to_module_pipeline` or
    `config.add_default_connectors_to_learner_pipeline ` are set to
    False.

    The default env-to-module connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
        AddTimeDimToBatchAndZeroPad,
        AddStatesFromEpisodesToBatch,
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]
    The default Learner connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
        AddColumnsFromEpisodesToTrainBatch,
        AddTimeDimToBatchAndZeroPad,
        AddStatesFromEpisodesToBatch,
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]

    This ConnectorV2:
    - Loops through the input `data` and converts all found numpy arrays into
    framework-specific tensors (possibly on a GPU).
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        pin_memory: bool = False,
        device: Optional[str] = None,
        **kwargs,
    ):
        """Initializes a NumpyToTensor instance.

        Args:
            pin_memory: Whether to pin memory when creating (torch) tensors.
                If None (default), pins memory if `as_learner_connector` is True,
                otherwise doesn't pin memory.
            device: An optional device to move the resulting tensors to. If not
                provided, all data will be left on the CPU.
            **kwargs:
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )
        self._pin_memory = pin_memory
        self._device = device

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        is_single_agent = False
        is_multi_rl_module = isinstance(rl_module, MultiRLModule)
        # `data` already a ModuleID to batch mapping format.
        if not (is_multi_rl_module and all(c in rl_module._rl_modules for c in batch)):
            is_single_agent = True
            batch = {DEFAULT_MODULE_ID: batch}

        for module_id, module_data in batch.copy().items():
            # If `rl_module` is None, leave data in numpy format.
            if rl_module is not None:
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
            batch[module_id] = module_data

        return batch
