import numpy as np
import pytest
from torchvision import models

from ray.air.util.tensor_extensions.utils import create_ragged_ndarray
from ray.train.torch import TorchDetectionPredictor


@pytest.fixture(name="predictor")
def predictor_fixture():
    model = models.detection.maskrcnn_resnet50_fpn()
    yield TorchDetectionPredictor(model=model)


@pytest.mark.parametrize(
    "data",
    [
        np.zeros((1, 3, 32, 32), dtype=np.float32),
        {"image": np.zeros((1, 3, 32, 32), dtype=np.float32)},
        create_ragged_ndarray(
            [
                np.zeros((3, 32, 32), dtype=np.float32),
                np.zeros((3, 64, 64), dtype=np.float32),
            ]
        ),
    ],
)
def test_predict(predictor, data):
    predictions = predictor.predict(data)

    assert all(len(value) == len(data) for value in predictions.values())
    # Boxes should have shape `(# detections, 4)`.
    assert all(boxes.ndim == 2 for boxes in predictions["pred_boxes"])
    assert all(boxes.shape[-1] == 4 for boxes in predictions["pred_boxes"])
    # Labels should have shape `(# detections,)`.
    assert all(labels.ndim == 1 for labels in predictions["pred_labels"])
    # Scores should have shape `(# detections,)`.
    assert all(scores.ndim == 1 for scores in predictions["pred_scores"])


def test_multi_column_batch_raises_value_error(predictor):
    data = {
        "image": np.zeros((2, 3, 32, 32), dtype=np.float32),
        "boxes": np.zeros((2, 0, 4), dtype=np.float32),
        "labels": np.zeros((2, 0), dtype=np.int64),
    }
    with pytest.raises(ValueError):
        # `data` should only contain one key. Otherwise, `TorchDetectionPredictor`
        # doesn't know which column contains the input images.
        predictor.predict(data)


def test_invalid_dtype_raises_value_error(predictor):
    data = np.zeros((1, 3, 32, 32), dtype=np.float32)
    with pytest.raises(ValueError):
        # `dtype` should be a single `torch.dtype`.
        predictor.predict(data, dtype=np.float32)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
