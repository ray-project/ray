import pytest

from ray import tune
from ray.train.examples.pytorch.torch_linear_example import LinearDataset
from ray.train.torch.torch_trainer import TorchTrainer


class LinearDatasetDict(LinearDataset):
    """Modifies the LinearDataset to return a Dict instead of a Tuple."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": self.y[index, None]}


class NonTensorDataset(LinearDataset):
    """Modifies the LinearDataset to also return non-tensor objects."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": 2}


# Currently in DataParallelTrainers we only report metrics from rank 0.
# For testing purposes here, we need to be able to report from all
# workers.
class TorchTrainerPatchedMultipleReturns(TorchTrainer):
    def _report(self, training_iterator) -> None:
        for results in training_iterator:
            tune.report(results=results)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
