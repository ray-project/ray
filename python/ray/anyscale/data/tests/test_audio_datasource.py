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


def _verify_audio_ds(ds, expected_num_channels):
    # Verify basic audio properties
    assert ds.count() == NUM_AUDIO_FILES, ds.count()
    assert ds.schema().names == ["amplitude"], ds.schema()

    # Check dataset schema/types
    amplitude_type = ds.schema().types[0]
    assert amplitude_type.ndim == 2
    assert amplitude_type.scalar_type == pa.float32()

    # # Try a map_batches() (select_columns()) and take_all() on the dataset
    audio_data = ds.select_columns(["amplitude"]).take_all()
    for a in audio_data:
        assert a["amplitude"].shape[0] == expected_num_channels


def test_audio_datasource(ray_start_regular_shared, audio_uri):
    ds = ray.data.read_datasource(AudioDatasource(), paths=audio_uri)
    _verify_audio_ds(ds, 1)


def test_audio_datasource_custom_sample_rate(ray_start_regular_shared, audio_uri):
    # Custom sample rate of 48k hertz, common in modern video
    ds = ray.data.read_datasource(AudioDatasource(), paths=audio_uri, sample_rate=48000)
    _verify_audio_ds(ds, 1)


def test_audio_datasource_mono_audio(ray_start_regular_shared, audio_uri):
    # Mono audio
    ds = ray.data.read_datasource(AudioDatasource(), paths=audio_uri, mono_audio=True)
    _verify_audio_ds(ds, 1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
