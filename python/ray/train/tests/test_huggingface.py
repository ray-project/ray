import pytest
from unittest.mock import patch
import ray


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_huggingface_imports(ray_start_4_cpus):
    """Tests that HuggingFace integrations are lazily loaded."""

    # Mock missing torch import.
    with patch.dict("sys.modules", {"torch": None}):
        # Torch is eagerly loaded in ray.train.huggingface.
        with pytest.raises(ImportError, match="torch"):
            import ray.train.huggingface  # noqa: F401

    # Mock missing accelerate and transformers imports.
    with patch.dict("sys.modules", {"accelerate": None, "transformers": None}):

        # Verify that importing these modules do not fail due to import errors.
        import ray.train.huggingface  # noqa: F401, F811
        from ray.train.huggingface import (
            AccelerateTrainer,
            HuggingFaceCheckpoint,
            HuggingFacePredictor,
            HuggingFaceTrainer,
            LegacyTransformersCheckpoint,
            TransformersPredictor,
            TransformersTrainer,
        )

        # Real values are not needed for these tests.
        DUMMY_TRAIN_LOOP_PER_WORKER = None
        DUMMY_MODEL = None
        DUMMY_PATH = None
        DUMMY_PIPELINE = None
        DUMMY_CHECKPOINT = None
        DUMMY_TRAINER_INIT_PER_WORKER = None

        with pytest.raises(ImportError, match="accelerate"):
            AccelerateTrainer(DUMMY_TRAIN_LOOP_PER_WORKER)

        with pytest.raises(ImportError, match="transformers"):
            HuggingFaceCheckpoint.from_model(DUMMY_MODEL, path=DUMMY_PATH)

        with pytest.raises(ImportError, match="transformers"):
            HuggingFacePredictor(DUMMY_PIPELINE)

        with pytest.raises(ImportError, match="transformers"):
            HuggingFacePredictor.from_checkpoint(DUMMY_CHECKPOINT)

        with pytest.raises(ImportError, match="transformers"):
            HuggingFaceTrainer(DUMMY_TRAINER_INIT_PER_WORKER)

        with pytest.raises(ImportError, match="transformers"):
            LegacyTransformersCheckpoint.from_model(DUMMY_MODEL, path=DUMMY_PATH)

        with pytest.raises(ImportError, match="transformers"):
            TransformersPredictor(DUMMY_PIPELINE)

        with pytest.raises(ImportError, match="transformers"):
            TransformersPredictor.from_checkpoint(DUMMY_CHECKPOINT)

        with pytest.raises(ImportError, match="transformers"):
            TransformersTrainer(DUMMY_TRAINER_INIT_PER_WORKER)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
