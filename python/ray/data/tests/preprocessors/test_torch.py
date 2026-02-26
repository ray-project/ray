import numpy as np
import pytest
import torch
from torchvision import transforms

import ray
from ray.data.exceptions import UserCodeException
from ray.data.preprocessors import TorchVisionPreprocessor


class TestTorchVisionPreprocessor:
    def test_repr(self):
        class StubTransform:
            def __call__(self, tensor):
                return tensor

            def __repr__(self):
                return "StubTransform()"

        preprocessor = TorchVisionPreprocessor(
            columns=["spam"], transform=StubTransform()
        )
        assert repr(preprocessor) == (
            "TorchVisionPreprocessor(columns=['spam'], "
            "output_columns=['spam'], transform=StubTransform())"
        )

    @pytest.mark.parametrize(
        "transform",
        [
            transforms.ToTensor(),  # `ToTensor` accepts an `np.ndarray` as input
            transforms.Lambda(lambda tensor: tensor.permute(2, 0, 1)),
        ],
    )
    def test_transform_images(self, transform):
        dataset = ray.data.from_items(
            [
                {"image": np.zeros((32, 32, 3)), "label": 0},
                {"image": np.zeros((32, 32, 3)), "label": 1},
            ]
        )
        preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)

        transformed_dataset = preprocessor.transform(dataset)

        assert transformed_dataset.schema().names == ["image", "label"]
        transformed_images = [
            record["image"] for record in transformed_dataset.take_all()
        ]
        assert all(image.shape == (3, 32, 32) for image in transformed_images)
        assert all(image.dtype == np.double for image in transformed_images)
        labels = {record["label"] for record in transformed_dataset.take_all()}
        assert labels == {0, 1}

    def test_batch_transform_images(self):
        dataset = ray.data.from_items(
            [
                {"image": np.zeros((32, 32, 3)), "label": 0},
                {"image": np.zeros((32, 32, 3)), "label": 1},
            ]
        )
        transform = transforms.Compose(
            [
                transforms.Lambda(
                    lambda batch: torch.as_tensor(batch).permute(0, 3, 1, 2)
                ),
                transforms.Resize(64),
            ]
        )
        preprocessor = TorchVisionPreprocessor(
            columns=["image"], transform=transform, batched=True
        )

        transformed_dataset = preprocessor.transform(dataset)

        assert transformed_dataset.schema().names == ["image", "label"]
        transformed_images = [
            record["image"] for record in transformed_dataset.take_all()
        ]
        assert all(image.shape == (3, 64, 64) for image in transformed_images)
        assert all(image.dtype == np.double for image in transformed_images)
        labels = {record["label"] for record in transformed_dataset.take_all()}
        assert labels == {0, 1}

    def test_transform_ragged_images(self):
        dataset = ray.data.from_items(
            [
                {"image": np.zeros((16, 16, 3)), "label": 0},
                {"image": np.zeros((32, 32, 3)), "label": 1},
            ]
        )
        transform = transforms.ToTensor()
        preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)

        transformed_dataset = preprocessor.transform(dataset)

        assert transformed_dataset.schema().names == ["image", "label"]
        transformed_images = [
            record["image"] for record in transformed_dataset.take_all()
        ]
        assert sorted(image.shape for image in transformed_images) == [
            (3, 16, 16),
            (3, 32, 32),
        ]
        assert all(image.dtype == np.double for image in transformed_images)
        labels = {record["label"] for record in transformed_dataset.take_all()}
        assert labels == {0, 1}

    def test_invalid_transform_raises_value_error(self):
        dataset = ray.data.from_items(
            [
                {"image": np.zeros((32, 32, 3)), "label": 0},
                {"image": np.zeros((32, 32, 3)), "label": 1},
            ]
        )
        transform = transforms.Lambda(lambda tensor: "BLAH BLAH INVALID")
        preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)

        with pytest.raises((UserCodeException, ValueError)):
            preprocessor.transform(dataset).materialize()


def test_torchvision_preprocessor_serialization():
    """Test TorchVisionPreprocessor serialization and deserialization functionality."""
    from torchvision import transforms

    from ray.data.preprocessor import SerializablePreprocessorBase

    # Create preprocessor
    transform = transforms.Compose([transforms.ToTensor()])
    preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)

    # Serialize using CloudPickle
    serialized = preprocessor.serialize()

    # Verify it's binary CloudPickle format
    assert isinstance(serialized, bytes)
    assert serialized.startswith(SerializablePreprocessorBase.MAGIC_CLOUDPICKLE)

    # Deserialize
    deserialized = TorchVisionPreprocessor.deserialize(serialized)

    # Verify type and field values
    assert isinstance(deserialized, TorchVisionPreprocessor)
    assert deserialized.columns == ["image"]
    assert isinstance(deserialized.torchvision_transform, type(transform))

    # Verify it works correctly
    test_data = {"image": np.zeros((32, 32, 3), dtype=np.uint8)}
    result = deserialized.transform_batch(test_data)

    # Verify transformation was applied - ToTensor converts uint8 [0,255] to float [0.0, 1.0]
    assert "image" in result
    assert result["image"].dtype in (np.float32, np.float64)
    assert result["image"].min() >= 0.0 and result["image"].max() <= 1.0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
