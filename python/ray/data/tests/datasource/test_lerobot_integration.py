import importlib.util

import numpy as np
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# lerobot >=0.5 requires Python >=3.12
LEROBOT_AVAILABLE = importlib.util.find_spec("lerobot") is not None

pytestmark = pytest.mark.skipif(
    not LEROBOT_AVAILABLE,
    reason="lerobot[dataset] (Python >=3.12) not available. "
    "Install with: pip install 'lerobot[dataset]>=0.5.0' av fsspec",
)


def test_read_lerobot_integration_public_s3(ray_start_regular_shared):
    """End-to-end read against a real LeRobot v3 dataset in a public S3 bucket.

    Uses ``s3://anonymous@ray-example-data/lerobot/libero-mini`` -- a 3-episode
    (843-frame) slice of LIBERO-10 with two 256x256 video cameras
    (``observation.images.image``, ``observation.images.wrist_image``),
    ``observation.state`` (8), ``action`` (7), and 3 distinct task strings.

    This decodes the whole dataset over the network, so it is tagged
    ``data_integration`` in BUILD.bazel and runs only in the premerge
    integration lane -- never in microcheck.
    """
    ds = ray.data.read_lerobot("s3://anonymous@ray-example-data/lerobot/libero-mini")

    names = ds.schema().names
    for col in (
        "observation.images.image",
        "observation.images.wrist_image",
        "observation.state",
        "action",
        "task",
        "dataset_index",
        "stats",
    ):
        assert col in names, col

    rows = ds.take_all()
    assert len(rows) == 843

    row = rows[0]
    for cam in ("observation.images.image", "observation.images.wrist_image"):
        frame = np.asarray(row[cam])
        assert frame.shape == (256, 256, 3)
        assert frame.dtype == np.uint8
    assert np.asarray(row["observation.state"]).shape == (8,)
    assert np.asarray(row["action"]).shape == (7,)
    assert isinstance(row["task"], str) and row["task"]

    # LIBERO interleaves tasks, so the 3 episodes span 3 distinct task strings.
    assert len({r["task"] for r in rows}) == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
