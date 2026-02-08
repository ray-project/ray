"""Integration tests for image namespace expressions.

These tests require Ray, torch, and torchvision to test end-to-end
image namespace expression evaluation.
"""

import numpy as np
import pyarrow as pa
import pytest
from packaging import version

import ray
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

pytestmark = [
    pytest.mark.skipif(
        version.parse(pa.__version__) < version.parse("19.0.0"),
        reason="Namespace expressions tests require PyArrow >= 19.0",
    ),
]


class TestImageNamespace:
    """Tests for image namespace operations."""

    def test_image_compose_single_transform(self, ray_start_regular_shared):
        """Test compose with a single transform."""
        from torchvision.transforms import v2

        ds = ray.data.from_items(
            [
                {"image": np.zeros((32, 32, 3), dtype=np.uint8)},
                {"image": np.ones((32, 32, 3), dtype=np.uint8) * 255},
            ]
        )

        augmentation = [v2.ToTensor()]

        result_ds = ds.with_column(
            "transformed", col("image").image.compose(augmentation)
        )
        results = result_ds.take_all()

        assert len(results) == 2
        for row in results:
            assert "transformed" in row
            assert "image" in row
            # ToTensor converts (H, W, C) -> (C, H, W) and normalizes to [0, 1]
            assert row["transformed"].shape == (3, 32, 32)
            assert row["transformed"].dtype in (np.float32, np.float64)

    def test_image_compose_multiple_transforms(self, ray_start_regular_shared):
        """Test compose with multiple transforms."""
        from torchvision.transforms import v2

        ds = ray.data.from_items(
            [
                {"image": np.ones((64, 64, 3), dtype=np.uint8) * 128},
                {"image": np.ones((64, 64, 3), dtype=np.uint8) * 64},
            ]
        )

        augmentation = [
            v2.ToTensor(),
            v2.Resize((32, 32)),
        ]

        result_ds = ds.with_column("resized", col("image").image.compose(augmentation))
        results = result_ds.take_all()

        assert len(results) == 2
        for row in results:
            assert "resized" in row
            # ToTensor converts (H, W, C) -> (C, H, W), then Resize to 32x32
            assert row["resized"].shape == (3, 32, 32)

    def test_image_compose_with_augmentation_pipeline(self, ray_start_regular_shared):
        """Test compose with a realistic augmentation pipeline."""
        from torchvision.transforms import v2

        ds = ray.data.from_items(
            [
                {"image": np.ones((64, 64, 3), dtype=np.uint8) * 128},
            ]
        )

        augmentation = [
            v2.ToTensor(),
            v2.RandomHorizontalFlip(p=1.0),  # Always flip for determinism
            v2.Resize((48, 48)),
        ]

        result_ds = ds.with_column(
            "augmented", col("image").image.compose(augmentation)
        )
        results = result_ds.take_all()

        assert len(results) == 1
        assert results[0]["augmented"].shape == (3, 48, 48)

    def test_image_compose_preserves_other_columns(self, ray_start_regular_shared):
        """Test that compose preserves other columns in the dataset."""
        from torchvision.transforms import v2

        ds = ray.data.from_items(
            [
                {
                    "image": np.zeros((32, 32, 3), dtype=np.uint8),
                    "label": 0,
                    "name": "a",
                },
                {
                    "image": np.ones((32, 32, 3), dtype=np.uint8),
                    "label": 1,
                    "name": "b",
                },
            ]
        )

        augmentation = [v2.ToTensor()]

        result_ds = ds.with_column(
            "transformed", col("image").image.compose(augmentation)
        )
        results = result_ds.take_all()

        assert len(results) == 2
        for i, row in enumerate(results):
            assert "image" in row
            assert "transformed" in row
            assert "label" in row
            assert "name" in row
            assert row["label"] == i
            assert row["name"] == ["a", "b"][i]

    def test_image_compose_ragged_images(self, ray_start_regular_shared):
        """Test compose with images of different sizes."""
        from torchvision.transforms import v2

        ds = ray.data.from_items(
            [
                {"image": np.zeros((16, 16, 3), dtype=np.uint8)},
                {"image": np.zeros((32, 32, 3), dtype=np.uint8)},
                {"image": np.zeros((64, 64, 3), dtype=np.uint8)},
            ]
        )

        augmentation = [
            v2.ToTensor(),
            v2.Resize((24, 24)),
        ]

        result_ds = ds.with_column("resized", col("image").image.compose(augmentation))
        results = result_ds.take_all()

        assert len(results) == 3
        for row in results:
            # All images should be resized to 24x24
            assert row["resized"].shape == (3, 24, 24)

    def test_image_compose_grayscale_to_rgb(self, ray_start_regular_shared):
        """Test compose can handle grayscale images with Lambda transform."""
        from torchvision.transforms import v2

        # Grayscale image (H, W) - no channel dimension
        ds = ray.data.from_items(
            [
                {"image": np.zeros((32, 32), dtype=np.uint8)},
            ]
        )

        augmentation = [
            # Add channel dimension and convert to RGB
            v2.Lambda(lambda x: np.stack([x, x, x], axis=-1)),
            v2.ToTensor(),
        ]

        result_ds = ds.with_column("rgb", col("image").image.compose(augmentation))
        results = result_ds.take_all()

        assert len(results) == 1
        assert results[0]["rgb"].shape == (3, 32, 32)

    def test_image_compose_empty_transforms_raises(self, ray_start_regular_shared):
        """Test that empty transforms list raises ValueError."""
        ds = ray.data.from_items(
            [
                {"image": np.zeros((32, 32, 3), dtype=np.uint8)},
            ]
        )

        with pytest.raises(ValueError, match="transforms list must not be empty"):
            ds.with_column("transformed", col("image").image.compose([]))

    def test_image_compose_invalid_dtype_raises(self, ray_start_regular_shared):
        """Test that image.compose on non-image column raises an error."""
        from torchvision.transforms import v2

        ds = ray.data.from_items([{"value": 1}, {"value": 2}])

        with pytest.raises(
            (ray.exceptions.RayTaskError, ray.exceptions.UserCodeException)
        ):
            ds.with_column(
                "transformed", col("value").image.compose([v2.ToTensor()])
            ).materialize()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
