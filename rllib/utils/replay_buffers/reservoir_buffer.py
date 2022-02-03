from typing import Any, Dict

# Import ray before psutil will make sure we use psutil's bundled version
import ray  # noqa F401
import psutil  # noqa E402

from ray.rllib.utils.annotations import DeveloperAPI, ExperimentalAPI, override
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer
from ray.rllib.utils.typing import SampleBatchType


@ExperimentalAPI
@DeveloperAPI
class ReservoirBuffer(ReplayBuffer):
    def __init__(self, capacity: int = 10000, storage_unit: str =
    "timesteps", **kwargs):
        super(ReservoirBuffer, self).__init__(capacity, storage_unit)
        raise NotImplementedError

    @ExperimentalAPI
    @DeveloperAPI
    @override(ReplayBuffer)
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        raise NotImplementedError

    @ExperimentalAPI
    @DeveloperAPI
    @override(ReplayBuffer)
    def sample(self, num_items: int, **kwargs) -> SampleBatchType:
        raise NotImplementedError

    @ExperimentalAPI
    @DeveloperAPI
    @override(ReplayBuffer)
    def stats(self, debug: bool = False) -> dict:
        raise NotImplementedError

    @ExperimentalAPI
    @DeveloperAPI
    @override(ReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        raise NotImplementedError

    @ExperimentalAPI
    @DeveloperAPI
    @override(ReplayBuffer)
    def set_state(self, state: Dict[str, Any]) -> None:
        raise NotImplementedError
