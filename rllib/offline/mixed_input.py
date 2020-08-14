import numpy as np

from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.json_reader import JsonReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.types import SampleBatchType
from typing import Dict


@DeveloperAPI
class MixedInput(InputReader):
    """Mixes input from a number of other input sources.

    Examples:
        >>> MixedInput({
            "sampler": 0.4,
            "/tmp/experiences/*.json": 0.4,
            "s3://bucket/expert.json": 0.2,
        }, ioctx)
    """

    @DeveloperAPI
    def __init__(self, dist: Dict[JsonReader, float], ioctx: IOContext):
        """Initialize a MixedInput.

        Arguments:
            dist (dict): dict mapping JSONReader paths or "sampler" to
                probabilities. The probabilities must sum to 1.0.
            ioctx (IOContext): current IO context object.
        """
        if sum(dist.values()) != 1.0:
            raise ValueError("Values must sum to 1.0: {}".format(dist))
        self.choices = []
        self.p = []
        for k, v in dist.items():
            if k == "sampler":
                self.choices.append(ioctx.default_sampler_input())
            else:
                self.choices.append(JsonReader(k))
            self.p.append(v)

    @override(InputReader)
    def next(self) -> SampleBatchType:
        source = np.random.choice(self.choices, p=self.p)
        return source.next()
