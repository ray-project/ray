from types import FunctionType
from typing import Dict

import numpy as np
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.offline.json_reader import JsonReader
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.typing import SampleBatchType
from ray.tune.registry import registry_get_input, registry_contains_input


@DeveloperAPI
class MixedInput(InputReader):
    """Mixes input from a number of other input sources.

    Examples:
        >>> from ray.rllib.offline.io_context import IOContext
        >>> from ray.rllib.offline.mixed_input import MixedInput
        >>> ioctx = IOContext(...) # doctest: +SKIP
        >>> MixedInput({ # doctest: +SKIP
        ...    "sampler": 0.4, # doctest: +SKIP
        ...    "/tmp/experiences/*.json": 0.4, # doctest: +SKIP
        ...    "s3://bucket/expert.json": 0.2, # doctest: +SKIP
        ... }, ioctx) # doctest: +SKIP
    """

    @DeveloperAPI
    def __init__(self, dist: Dict[JsonReader, float], ioctx: IOContext):
        """Initialize a MixedInput.

        Args:
            dist: dict mapping JSONReader paths or "sampler" to
                probabilities. The probabilities must sum to 1.0.
            ioctx: current IO context object.
        """
        if sum(dist.values()) != 1.0:
            raise ValueError("Values must sum to 1.0: {}".format(dist))
        self.choices = []
        self.p = []
        for k, v in dist.items():
            if k == "sampler":
                self.choices.append(ioctx.default_sampler_input())
            elif isinstance(k, FunctionType):
                self.choices.append(k(ioctx))
            elif isinstance(k, str) and registry_contains_input(k):
                input_creator = registry_get_input(k)
                self.choices.append(input_creator(ioctx))
            else:
                self.choices.append(JsonReader(k, ioctx))
            self.p.append(v)

    @override(InputReader)
    def next(self) -> SampleBatchType:
        source = np.random.choice(self.choices, p=self.p)
        return source.next()
