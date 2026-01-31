import numpy as np
import pytest

import ray
from ray.tests.conftest import *  # noqa

NUM_AUDIO_FILES = 10


@pytest.fixture
def audio_uri():
    root = "s3://anonymous@air-example-data-2/6G-audio-data-LibriSpeech-train-clean-100-flac"  # noqa: E501
    return [
        f"{root}/train-clean-100/5022/29411/5022-29411-{n:04}.flac"
        for n in range(NUM_AUDIO_FILES)
    ]


def test_read_audio(ray_start_regular_shared, audio_uri):
    ds = ray.data.read_audio(audio_uri)

    # Verify basic audio properties
    assert ds.count() == NUM_AUDIO_FILES, ds.count()
    assert ds.schema().names == ["amplitude", "sample_rate"], ds.schema()

    # Check the sample rate
    assert all(row["sample_rate"] == 16000 for row in ds.take_all())

    for row in ds.take_all():
        assert row["amplitude"].ndim == 2
        assert row["amplitude"].shape[0] == 1
        assert row["amplitude"].dtype == np.float32


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
