import numpy as np

from ray.rllib import SampleBatch


class SegmentationBuffer:
    def __init__(self):
        pass

    def add(self, batch: SampleBatch):
        pass

    def next(self) -> SampleBatch:
        pass

