"""Read-task isolation and streaming listing for files with unknown sizes."""

from typing import cast

import pytest

import ray
from ray._common.test_utils import SignalActor
from ray.actor import ActorClass, ActorHandle
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.file_reader_test_utils import (
    ReadGatedLocalFileSystem,
    UnknownSizeLocalFileSystem,
)

pytestmark = [
    pytest.mark.usefixtures("ray_start_2_cpus_shared"),
    pytest.mark.parametrize("datasource_v2", [True], indirect=True),
]


@pytest.fixture
def text_files(tmp_path):
    def write(lines):
        for index, line in enumerate(lines):
            (tmp_path / f"{index:04}.txt").write_text(f"{line}\n")
        return tmp_path

    return write


def test_unknown_size_files_are_read_in_separate_tasks(
    text_files, datasource_v2, restore_data_context
):
    restore_data_context.execution_options.preserve_order = True
    path = text_files(["0", "1", "2", "3"])

    ds = ray.data.read_text(
        str(path), filesystem=UnknownSizeLocalFileSystem(), override_num_blocks=2
    ).materialize()

    assert ds.take_all() == [{"text": str(index)} for index in range(4)]
    assert "ReadFilesTextV2: 4 tasks executed" in ds.stats()


def test_unknown_size_read_starts_before_listing_finishes(
    text_files, datasource_v2, restore_data_context
):
    restore_data_context.execution_options.preserve_order = False
    # Fill one listing manifest before traversal reaches the blocked directory.
    batch_size = max(1, NonSamplingFileIndexer._DEFAULT_MAX_PATHS_PER_OUTPUT)
    path = text_files(["ready"] * batch_size)
    later = path / "later"
    later.mkdir()
    (later / "last.txt").write_text("late\n")
    signal_actor = cast(ActorClass, SignalActor)
    listing_blocked = cast(ActorHandle, signal_actor.remote())
    first_read = cast(ActorHandle, signal_actor.remote())
    try:
        ds = ray.data.read_text(
            str(path),
            filesystem=ReadGatedLocalFileSystem(listing_blocked, first_read),
            override_num_blocks=2,
        )
        assert ds.limit(1).take_all() == [{"text": "ready"}]
    finally:
        ray.kill(listing_blocked)
        ray.kill(first_read)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
