import pytest

import ray
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor, DataBatchType, PredictorNotSerializableException


class DummyPredictor(Predictor):
    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "DummyPredictor":
        return DummyPredictor()

    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        return data


def test_serialization():
    """Tests that Predictor instances are not serializable."""

    # Class is serializable.
    ray.put(DummyPredictor)

    # Instance is not serializable.
    predictor = DummyPredictor()
    with pytest.raises(PredictorNotSerializableException):
        ray.put(predictor)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
