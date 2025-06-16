from typing import Any, Dict, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2, ConnectorV2BatchFormats
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class TensorToNumpy(ConnectorV2):
    """Converts (framework) tensors across the entire input data into numpy arrays."""

    # Incoming batches have the format:
    # [moduleID] -> [column name] -> [tensors]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    INPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_MODULE_TO_COLUMN_TO_BATCHED
    )
    # Returned batches have the format:
    # [moduleID] -> [column name] -> [tensors]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    OUTPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_MODULE_TO_COLUMN_TO_BATCHED
    )

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
        return convert_to_numpy(batch)
