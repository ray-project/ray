import unittest

from ray.rllib.utils.replay_buffers.distributed import ReplayReplayBufferManager
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch

class TestDistributedReplayBuffer(unittest.TestCase):
    def test_add_sample_by_ref(self):
        pass

    def test_add_sample_directly(self):
        pass

class TestReplayBufferManager(unittest.TestCase):
    def test_init_add_sample_collocated_buffer(self):
        pass

    def test_init_add_sample_distributed_buffer(self):
        pass


class TestPlacementGroups(unittest.TestCase)
    pass