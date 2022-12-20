import numpy as np
import torch
from torchvision import transforms

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
        transform = transforms.ToTensor()
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
