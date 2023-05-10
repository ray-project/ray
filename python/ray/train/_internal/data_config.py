from typing import Optional, Dict, List, TYPE_CHECKING


if TYPE_CHECKING:
    from ray.air.config import DatasetConfig
    from ray.data import Dataset, DataIterator, ExecutionOptions, NodeIdStr
    from ray.data.preprocessor import Preprocessor


class DataConfig:
    def __init__(
        self,
        datasets_to_split: Optional[List[str]] = None,
        execution_options: Optional[ExecutionOptions] = None,
    ):
        ...

    def configure(
        self,
        datasets: Dict[str, Dataset],
        world_size: int,
        worker_node_ids: Optional[List[NodeIdStr]],
    ) -> Dict[int, Dict[str, DataIterator]]:
        ...


class LegacyDataConfigWrapper(DataConfig):
    def __init__(self, config: Dict[str, DatasetConfig]):
        self._config = config

    def _legacy_preprocessing(
        self, datasets: Dict[str, Dataset], preprocessor: Optional[Preprocessor]
    ) -> Dict[str, Dataset]:
        ...
