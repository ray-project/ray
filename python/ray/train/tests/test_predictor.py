import uuid
from typing import Dict, Optional, Union
from unittest import mock

import numpy as np
import pandas as pd
import pytest

import ray
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.data_batch_conversion import BatchFormat
from ray.data import Preprocessor
from ray.train._internal.framework_checkpoint import FrameworkCheckpoint
from ray.train.predictor import Predictor, PredictorNotSerializableException
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint


class DummyPreprocessor(Preprocessor):
    def __init__(self, multiplier=2):
        self.multiplier = multiplier
        self.inputs = []
        self.outputs = []
        self.id = uuid.uuid4()

    def fit_status(self) -> Preprocessor.FitStatus:
        """Override fit status to test full transform_batch path."""
        return Preprocessor.FitStatus.FITTED

    def _transform_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        self.inputs.append(df)
        rst = df * self.multiplier
        self.outputs.append(rst)
        return rst


class DummyWithNumpyPreprocessor(DummyPreprocessor):
    def _transform_numpy(
        self, np_data: Union[np.ndarray, Dict[str, np.ndarray]]
    ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
        self.inputs.append(np_data)
        assert isinstance(np_data, np.ndarray)
        rst = np_data * self.multiplier
        self.outputs.append(rst)
        return rst

    @classmethod
    def preferred_batch_format(cls) -> BatchFormat:
        return BatchFormat.NUMPY


class DummyPredictor(Predictor):
    def __init__(
        self, factor: float = 1.0, preprocessor: Optional[Preprocessor] = None
    ):
        self.factor = factor
        super().__init__(preprocessor)

    @classmethod
    def from_checkpoint(
        cls, checkpoint: FrameworkCheckpoint, **kwargs
    ) -> "DummyPredictor":
        checkpoint_data = load_dict_checkpoint(checkpoint)
        preprocessor = checkpoint.get_preprocessor()
        return cls(checkpoint_data["factor"], preprocessor)

    def _predict_pandas(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return pd.DataFrame({"predictions": data.iloc[:, 0] * self.factor})


class DummyWithNumpyPredictor(DummyPredictor):
    def _predict_numpy(
        self, data: Union[np.ndarray, Dict[str, np.ndarray]], **kwargs
    ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
        return data * self.factor

    @classmethod
    def preferred_batch_format(cls):
        return BatchFormat.NUMPY


def test_serialization():
    """Tests that Predictor instances are not serializable."""

    # Class is serializable.
    ray.put(DummyPredictor)

    # Instance is not serializable.
    predictor = DummyPredictor()
    with pytest.raises(PredictorNotSerializableException):
        ray.put(predictor)


def test_from_checkpoint():
    with create_dict_checkpoint(
        {"factor": 2.0}, checkpoint_cls=FrameworkCheckpoint
    ) as checkpoint:
        assert DummyPredictor.from_checkpoint(checkpoint).factor == 2.0


def test_predict_pandas_with_pandas_data():
    """Data batch level predictor test where both input data and prediction
    batch format are pandas dataframes.
    """
    input = pd.DataFrame({"x": [1, 2, 3]})
    with create_dict_checkpoint(
        {"factor": 2.0}, checkpoint_cls=FrameworkCheckpoint
    ) as checkpoint:
        checkpoint.set_preprocessor(DummyPreprocessor())
        predictor = DummyPredictor.from_checkpoint(checkpoint)

    actual_output = predictor.predict(input)
    pd.testing.assert_frame_equal(
        actual_output, pd.DataFrame({"predictions": [4.0, 8.0, 12.0]})
    )
    pd.testing.assert_frame_equal(
        predictor.get_preprocessor().inputs[0],
        pd.DataFrame({"x": [1, 2, 3]}),
    )
    pd.testing.assert_frame_equal(
        predictor.get_preprocessor().outputs[0],
        pd.DataFrame({"x": [2, 4, 6]}),
    )

    # Test predict with both Numpy and Pandas preprocessor available
    with create_dict_checkpoint(
        {"factor": 2.0}, checkpoint_cls=FrameworkCheckpoint
    ) as checkpoint:
        checkpoint.set_preprocessor(DummyWithNumpyPreprocessor())
        predictor = DummyPredictor.from_checkpoint(checkpoint)

    actual_output = predictor.predict(input)
    pd.testing.assert_frame_equal(
        actual_output, pd.DataFrame({"predictions": [4.0, 8.0, 12.0]})
    )
    # This Preprocessor has Numpy as the batch format preference.
    np.testing.assert_array_equal(
        predictor.get_preprocessor().inputs[0], np.array([1, 2, 3])
    )

    np.testing.assert_array_equal(
        predictor.get_preprocessor().outputs[0], np.array([2, 4, 6])
    )


def test_predict_numpy_with_numpy_data():
    """Data batch level predictor test where both input data and prediction
    batch format are numpy formats.
    """
    input = np.array([1, 2, 3])
    # Test predict with only Pandas preprocessor
    with create_dict_checkpoint(
        {"factor": 2.0}, checkpoint_cls=FrameworkCheckpoint
    ) as checkpoint:
        checkpoint.set_preprocessor(DummyPreprocessor())
        predictor = DummyPredictor.from_checkpoint(checkpoint)

    actual_output = predictor.predict(input)
    # Numpy is the preferred batch format for prediction.
    # Multiply by 2 from preprocessor, another multiply by 2.0 from predictor
    np.testing.assert_array_equal(actual_output, np.array([4.0, 8.0, 12.0]))

    # Preprocessing is still done via Pandas path.
    pd.testing.assert_frame_equal(
        predictor.get_preprocessor().inputs[0],
        pd.DataFrame({TENSOR_COLUMN_NAME: [1, 2, 3]}),
    )
    pd.testing.assert_frame_equal(
        predictor.get_preprocessor().outputs[0],
        pd.DataFrame({TENSOR_COLUMN_NAME: [2, 4, 6]}),
    )

    # Test predict with Numpy as preferred batch format for both Predictor and
    # Preprocessor.
    with create_dict_checkpoint(
        {"factor": 2.0}, checkpoint_cls=FrameworkCheckpoint
    ) as checkpoint:
        checkpoint.set_preprocessor(DummyWithNumpyPreprocessor())
        predictor = DummyPredictor.from_checkpoint(checkpoint)

    actual_output = predictor.predict(input)
    np.testing.assert_equal(actual_output, np.array([4.0, 8.0, 12.0]))
    np.testing.assert_equal(predictor.get_preprocessor().inputs[0], np.array([1, 2, 3]))
    np.testing.assert_equal(
        predictor.get_preprocessor().outputs[0], np.array([2, 4, 6])
    )


def test_predict_pandas_with_numpy_data():
    """Data batch level predictor test where both input data is numpy format but
    predictor only has _predict_pandas implementation.
    """
    input = np.array([1, 2, 3])
    # Test predict with only Pandas preprocessor
    with create_dict_checkpoint(
        {"factor": 2.0}, checkpoint_cls=FrameworkCheckpoint
    ) as checkpoint:
        checkpoint.set_preprocessor(DummyPreprocessor())
        predictor = DummyPredictor.from_checkpoint(checkpoint)

    actual_output = predictor.predict(input)

    # Predictor should return in the same format as the input.
    # Multiply by 2 from preprocessor, another multiply by 2.0 from predictor
    np.testing.assert_array_equal(actual_output, np.array([4.0, 8.0, 12.0]))

    # Preprocessing should go through Pandas path.
    pd.testing.assert_frame_equal(
        predictor.get_preprocessor().inputs[0],
        pd.DataFrame({TENSOR_COLUMN_NAME: [1, 2, 3]}),
    )
    pd.testing.assert_frame_equal(
        predictor.get_preprocessor().outputs[0],
        pd.DataFrame({TENSOR_COLUMN_NAME: [2, 4, 6]}),
    )

    # Test predict with both Numpy and Pandas preprocessor available
    with create_dict_checkpoint(
        {"factor": 2.0}, checkpoint_cls=FrameworkCheckpoint
    ) as checkpoint:
        checkpoint.set_preprocessor(DummyWithNumpyPreprocessor())
        predictor = DummyPredictor.from_checkpoint(checkpoint)

    actual_output = predictor.predict(input)
    np.testing.assert_equal(actual_output, np.array([4.0, 8.0, 12.0]))
    # Preprocessor should go through Numpy path since it is the preferred batch type.
    np.testing.assert_equal(predictor.get_preprocessor().inputs[0], np.array([1, 2, 3]))
    np.testing.assert_equal(
        predictor.get_preprocessor().outputs[0], np.array([2, 4, 6])
    )


def test_from_udf():
    def check_truth(df, all_true=False):
        if all_true:
            return pd.DataFrame({"bool": [True] * len(df)})
        return pd.DataFrame({"bool": df["a"] == df["b"]})

    predictor = Predictor.from_pandas_udf(check_truth)

    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 5, 6]})

    output = predictor.predict(df)
    output = output["bool"].tolist()
    assert output == [True, False, False]

    output = predictor.predict(df, all_true=True)
    output = output["bool"].tolist()
    assert output == [True, True, True]


@mock.patch.object(DummyPredictor, "_predict_pandas", return_value=mock.DEFAULT)
def test_kwargs(predict_pandas_mock):
    with create_dict_checkpoint(
        {"factor": 2.0}, checkpoint_cls=FrameworkCheckpoint
    ) as checkpoint:
        predictor = DummyPredictor.from_checkpoint(checkpoint)

    input = pd.DataFrame({"x": [1, 2, 3]})
    predictor.predict(input, extra_arg=1)

    # Second element in call_args is the kwargs.
    assert "extra_arg" in predict_pandas_mock.call_args[1]
    assert predict_pandas_mock.call_args[1]["extra_arg"] == 1


def test_get_and_set_preprocessor():
    """Test preprocessor can be set and get."""

    preprocessor = DummyPreprocessor(1)
    with create_dict_checkpoint(
        {"factor": 2.0}, checkpoint_cls=FrameworkCheckpoint
    ) as checkpoint:
        checkpoint.set_preprocessor(preprocessor)
        predictor = DummyPredictor.from_checkpoint(checkpoint)

    assert predictor.get_preprocessor().id == preprocessor.id

    test_dataset = pd.DataFrame(range(4))
    output_df = predictor.predict(test_dataset)
    assert output_df.to_numpy().squeeze().tolist() == [
        0.0,
        2.0,
        4.0,
        6.0,
    ]

    preprocessor2 = DummyPreprocessor(2)
    predictor.set_preprocessor(preprocessor2)
    assert predictor.get_preprocessor().id == preprocessor2.id

    output_df = predictor.predict(test_dataset)
    assert output_df.to_numpy().squeeze().tolist() == [
        0.0,
        4.0,
        8.0,
        12.0,
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
