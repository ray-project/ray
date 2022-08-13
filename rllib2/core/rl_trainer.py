


BatchType = Union[SampleBatch, MultiAgentBatch]


class RLTrainer:
    def update(self, samples: BatchType, **kwargs) -> Any:
        pass
