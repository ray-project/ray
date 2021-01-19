import glob
import json
import logging
import os
import random
from urllib.parse import urlparse
import gym

try:
    from smart_open import smart_open
except ImportError:
    smart_open = None

from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, MultiAgentBatch, \
    SampleBatch
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import FileType, SampleBatchType
from typing import List

logger = logging.getLogger(__name__)


@PublicAPI
class D4RLReader(InputReader):
    """Reader object that loads the dataset from the D4RL dataset."""

    @PublicAPI
    def __init__(self, inputs: str, ioctx: IOContext = None):
        """Initialize a D4RLReader.

        Args:
            inputs (str): String corresponding to D4RL environment name 
            ioctx (IOContext): Current IO context object.
        """
        import d4rl
        self.env = gym.make(inputs)
        self.dataset = convert_to_batch(d4rl.qlearning_dataset(self.env))
        assert self.dataset.count >=1
        self.dataset.shuffle()
        self.counter = 0 

    @override(InputReader)
    def next(self) -> SampleBatchType:
        if self.counter >= self.dataset.count:
            self.counter =0
            self.dataset.shuffle()

        self.counter += 1
        return self.dataset.slice(start=self.counter, end=self.counter+1)

def convert_to_batch(dataset) -> SampleBatchType:
    # Converts D4RL dataset to SampleBatch
    # Keys: dict_keys(['observations', 'actions', 'next_observations', 'rewards', 'terminals'])
    d = {}
    d[SampleBatch.OBS] = dataset['observations']
    d[SampleBatch.ACTIONS] = dataset['actions']
    d[SampleBatch.NEXT_OBS] = dataset['next_observations']
    d[SampleBatch.REWARDS] = dataset['rewards']
    d[SampleBatch.DONES] = dataset['terminals']

    return SampleBatch(d)

