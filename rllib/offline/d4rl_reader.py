import logging
import gym

from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import SampleBatchType
from typing import Dict

logger = logging.getLogger(__name__)


@PublicAPI
class D4RLReader(InputReader):
    """Reader object that loads the dataset from the D4RL dataset."""

    @PublicAPI
    def __init__(self, inputs: str, ioctx: IOContext = None):
        """Initializes a D4RLReader instance.

        Args:
            inputs: String corresponding to the D4RL environment name.
            ioctx: Current IO context object.
        """
        import d4rl

        self.env = gym.make(inputs)
        self.dataset = _convert_to_batch(d4rl.qlearning_dataset(self.env))
        assert self.dataset.count >= 1
        self.counter = 0

    @override(InputReader)
    def next(self) -> SampleBatchType:
        if self.counter >= self.dataset.count:
            self.counter = 0

        self.counter += 1
        return self.dataset.slice(start=self.counter, end=self.counter + 1)


def _convert_to_batch(dataset: Dict) -> SampleBatchType:
    # Converts D4RL dataset to SampleBatch
    d = {}
    d[SampleBatch.OBS] = dataset["observations"]
    d[SampleBatch.ACTIONS] = dataset["actions"]
    d[SampleBatch.NEXT_OBS] = dataset["next_observations"]
    d[SampleBatch.REWARDS] = dataset["rewards"]
    d[SampleBatch.DONES] = dataset["terminals"]

    return SampleBatch(d)
