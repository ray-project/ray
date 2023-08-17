import pytest
from ray.train._internal.storage import StorageContext


@pytest.mark.parametrize(
    "storage_path_expected",
    [
        ("/foo/bar", "/foo/bar/{sub}"),
        (
            "s3://foo/bar?endpoint_override=none",
            "s3://foo/bar/{sub}?endpoint_override=none",
        ),
    ],
)
def test_storage_context_storage_prefix(monkeypatch, storage_path_expected):
    storage_path, expected = storage_path_expected

    with monkeypatch.context() as mp:
        mp.setattr(StorageContext, "_create_validation_file", lambda sc: None)
        mp.setattr(StorageContext, "_check_validation_file", lambda sc: None)
        context = StorageContext(storage_path=storage_path, experiment_dir_name="exp")

        assert str(
            context.storage_prefix / context.storage_fs_path / "test"
        ) == expected.format(sub="test")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
