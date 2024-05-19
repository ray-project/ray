from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import convert_to_tensor
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
        **kwargs,
    ):
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )
        self._as_learner_connector = as_learner_connector

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
        is_multi_agent = isinstance(episodes[0], MultiAgentEpisode)

        if not is_multi_agent:
            data = {DEFAULT_MODULE_ID: data}

        # TODO (sven): Support specifying a device (e.g. GPU).
        for module_id, module_data in data.copy().items():
            infos = module_data.pop(Columns.INFOS, None)
            module_data = convert_to_tensor(module_data, framework=rl_module.framework)
            if infos is not None:
                module_data[Columns.INFOS] = infos
            # Early out if not multi-agent AND not learner connector (which
            # does always operate on a MARLModule).
            if not is_multi_agent and not self._as_learner_connector:
                return module_data
            data[module_id] = module_data

        return data
