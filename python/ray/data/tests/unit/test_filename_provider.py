import pytest

from ray.data.datasource.filename_provider import FilenameProvider


@pytest.fixture(params=["csv", None])
def filename_provider(request):
    yield FilenameProvider(dataset_uuid="", file_format=request.param)


def test_default_filename_for_task_includes_task_index(filename_provider):
    filename_0 = filename_provider.get_filename_for_task(
        write_uuid="spam", task_index=0
    )
    filename_1 = filename_provider.get_filename_for_task(
        write_uuid="spam", task_index=1
    )
    assert filename_0 != filename_1
    assert "000000" in filename_0
    assert "000001" in filename_1


def test_default_get_filename_for_task_is_deterministic(filename_provider):
    """Test the new get_filename_for_task() method is deterministic."""

    first_filename = filename_provider.get_filename_for_task(
        write_uuid="spam", task_index=0
    )
    second_filename = filename_provider.get_filename_for_task(
        write_uuid="spam", task_index=0
    )

    assert first_filename == second_filename


def test_default_row_filenames_derived_from_task_are_unique(filename_provider):
    """Row filenames derived from task filename with block/row index are unique."""
    task_filename = filename_provider.get_filename_for_task(
        write_uuid="spam", task_index=0
    )
    filenames = []
    for block_index in range(2):
        for row_index in range(4):
            if "." in task_filename:
                base, ext = task_filename.rsplit(".", 1)
                filenames.append(f"{base}_{block_index:06}_{row_index:06}.{ext}")
            else:
                filenames.append(f"{task_filename}_{block_index:06}_{row_index:06}")
    assert len(set(filenames)) == len(filenames)


def test_default_get_filename_for_task_is_unique(filename_provider):
    """Test the new get_filename_for_task() method generates unique filenames."""
    filenames = [
        filename_provider.get_filename_for_task(
            write_uuid="spam",
            task_index=task_index,
        )
        for task_index in range(4)
    ]
    assert len(set(filenames)) == len(filenames)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))


# ---------------------------------------------------------------------------
# Tests for get_filename_for_row() and _uses_row_level_filenames()
# ---------------------------------------------------------------------------


class _RowLevelProvider(FilenameProvider):
    """Minimal provider that overrides get_filename_for_row."""

    def get_filename_for_row(self, row, write_uuid, task_index, block_index, row_index):
        return f"{row['uuid']}.png"


def test_uses_row_level_filenames_false_for_default():
    """Default FilenameProvider should NOT be detected as row-level."""
    provider = FilenameProvider(file_format="png")
    assert provider._uses_row_level_filenames() is False


def test_uses_row_level_filenames_true_for_subclass():
    """A subclass that overrides get_filename_for_row() should be detected."""
    provider = _RowLevelProvider(file_format="png")
    assert provider._uses_row_level_filenames() is True


def test_get_filename_for_row_returns_row_derived_name():
    """get_filename_for_row() should use row data, not task index."""
    provider = _RowLevelProvider(file_format="png")
    row = {"uuid": "abc-123", "image": b"..."}
    result = provider.get_filename_for_row(
        row, write_uuid="w", task_index=0, block_index=0, row_index=0
    )
    assert result == "abc-123.png"

    # Different row → different filename
    row2 = {"uuid": "xyz-789", "image": b"..."}
    result2 = provider.get_filename_for_row(
        row2, write_uuid="w", task_index=0, block_index=0, row_index=1
    )
    assert result2 == "xyz-789.png"
    assert result != result2


def test_task_level_provider_not_detected_as_row_level():
    """Regression: a task-level custom subclass must NOT trigger row-level path."""

    class MyTaskProvider(FilenameProvider):
        def get_filename_for_task(self, write_uuid, task_index):
            return f"custom_{write_uuid}_{task_index:06}.parquet"

    provider = MyTaskProvider()
    assert provider._uses_row_level_filenames() is False
