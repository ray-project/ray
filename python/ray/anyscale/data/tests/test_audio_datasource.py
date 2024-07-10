import pyarrow as pa
import pytest

import ray
from ray.anyscale.data import AudioDatasource
from ray.tests.conftest import *  # noqa

NUM_AUDIO_FILES = 10


@pytest.fixture
def audio_uri():
    root = "s3://anonymous@air-example-data-2/6G-audio-data-LibriSpeech-train-clean-100-flac"  # noqa: E501
    return [
        f"{root}/train-clean-100/5022/29411/5022-29411-{n:04}.flac"
        for n in range(NUM_AUDIO_FILES)
    ]


def test_audio_datasource(ray_start_regular_shared, audio_uri):
    ds = ray.data.read_datasource(AudioDatasource(audio_uri))

    # Verify basic audio properties
    assert ds.count() == NUM_AUDIO_FILES, ds.count()
    assert ds.schema().names == ["amplitude", "sample_rate"], ds.schema()

    # Check dataset schema/types
    amplitude_type = ds.schema().types[0]
    assert amplitude_type.ndim == 2
    assert amplitude_type.scalar_type == pa.float32()

    # Check the sample rate
    assert all(row["sample_rate"] == 16000 for row in ds.take_all())

    # # Try a map_batches() (select_columns()) and take_all() on the dataset
    audio_data = ds.select_columns(["amplitude"]).take_all()
    for a in audio_data:
        assert a["amplitude"].shape[0] == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
