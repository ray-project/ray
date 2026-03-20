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
