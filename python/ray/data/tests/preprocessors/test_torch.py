import numpy as np
import torchvision

import ray
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
        assert (
            repr(preprocessor)
            == "TorchVisionPreprocessor(columns=['spam'], transform=StubTransform())"
        )

    def test_transform_images(self):
        dataset = ray.data.from_items(
            [
                {"image": np.zeros((32, 32, 3)), "label": 0},
                {"image": np.zeros((32, 32, 3)), "label": 1},
            ]
        )
        transform = torchvision.transforms.ToTensor()
        preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)

        transformed_dataset = preprocessor.transform(dataset)

        transformed_images = [
            record["image"] for record in transformed_dataset.take_all()
        ]
        assert all(image.shape == (3, 32, 32) for image in transformed_images)
        assert all(image.dtype == np.double for image in transformed_images)

    def test_transform_ragged_images(self):
        dataset = ray.data.from_items(
            [
                {"image": np.zeros((16, 16, 3)), "label": 0},
                {"image": np.zeros((32, 32, 3)), "label": 1},
            ]
        )
        transform = torchvision.transforms.ToTensor()
        preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)

        transformed_dataset = preprocessor.transform(dataset)

        assert transformed_dataset.schema().names == ["image"]
        transformed_images = [
            record["image"] for record in transformed_dataset.take_all()
        ]
        assert sorted(image.shape for image in transformed_images) == [
            (3, 16, 16),
            (3, 32, 32),
        ]
        assert all(image.dtype == np.double for image in transformed_images)
