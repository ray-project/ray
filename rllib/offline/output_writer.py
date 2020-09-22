from ray.rllib.utils.annotations import override
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.typing import SampleBatchType


@PublicAPI
class OutputWriter:
    """Writer object for saving experiences from policy evaluation."""

    @PublicAPI
    def write(self, sample_batch: SampleBatchType):
        """Save a batch of experiences.

        Args:
            sample_batch: SampleBatch or MultiAgentBatch to save.
        """
        raise NotImplementedError


class NoopOutput(OutputWriter):
    """Output writer that discards its outputs."""

    @override(OutputWriter)
    def write(self, sample_batch: SampleBatchType):
        pass
