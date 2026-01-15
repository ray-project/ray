from ray.rllib.utils.annotations import PublicAPI, override
from ray.rllib.utils.typing import SampleBatchType


@PublicAPI
class OutputWriter:
    """Writer API for saving experiences from policy evaluation."""

    @PublicAPI
    def write(self, sample_batch: SampleBatchType):
        """Saves a batch of experiences.

        Args:
            sample_batch: SampleBatch or MultiAgentBatch to save.
        """
        raise NotImplementedError


@PublicAPI
class NoopOutput(OutputWriter):
    """Output writer that discards its outputs."""

    @override(OutputWriter)
    def write(self, sample_batch: SampleBatchType):
        # Do nothing.
        pass
