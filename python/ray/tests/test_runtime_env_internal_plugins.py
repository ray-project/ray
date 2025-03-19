import os
import pytest
import sys
import tempfile
import zipfile
import logging
from ray._private.runtime_env.utils import check_output_cmd
import ray
from ray._private.runtime_env.archive import get_context as get_archives_context
from ray._private.test_utils import wait_for_condition
from ray._private.runtime_env.packaging import get_local_dir_from_uri

ARCHIVE_PLUGIN_CLASS_PATH = (
    "ray._private.runtime_env.archive.DownloadAndUnpackArchivePlugin"  # noqa: E501
)
ARCHIVE_PLUGIN_NAME = "archives"

default_logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + ARCHIVE_PLUGIN_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
def test_archive_plugin_with_single_package(set_runtime_env_plugins, start_cluster):
    # test_internal_plugins.zip
    #     - test_1.txt
    #     - test_1:
    #         - test_1.txt
    archive_url = "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_plugins.zip"  # noqa: E501

    @ray.remote
    def check_file():
        archive_path = get_archives_context()
        assert isinstance(archive_path, str)
        test_1 = os.path.join(archive_path, "test_1.txt")
        test_2 = os.path.join(archive_path, "test_1", "test_1.txt")
        assert os.path.exists(test_1), test_1
        assert os.path.exists(test_2), test_2
        with open(test_1, "rt") as f:
            message = f.read()
            assert message == "test_1\n", message
        with open(test_2, "rt") as f:
            message = f.read()
            assert message == "test_2\n", message
        return True

    _, address = start_cluster
    with tempfile.TemporaryDirectory() as working_dir:
        ray.init(address, runtime_env={"working_dir": working_dir})
        output = ray.get(
            check_file.options(runtime_env={ARCHIVE_PLUGIN_NAME: archive_url}).remote()
        )
        assert output


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + ARCHIVE_PLUGIN_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
def test_archive_plugin_with_mutiple_packages(set_runtime_env_plugins, start_cluster):
    # test_internal_plugins.zip
    #     - test_1.txt
    #     - test_1:
    #         - test_1.txt
    archive_url_1 = "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_plugins.zip"  # noqa: E501

    # test_internal_plugins_2.zip
    #     - test_2.txt
    #     - test_2:
    #         - test_2.txt
    archive_url_2 = "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_plugins_2.zip"  # noqa: E501

    @ray.remote
    def check_file():
        archive_uris = get_archives_context()
        assert isinstance(archive_uris, dict)
        assert len(archive_uris) == 2
        assert "url1" in archive_uris
        assert "url2" in archive_uris

        # check url1:
        archive_path_1 = archive_uris["url1"]
        test_1 = os.path.join(archive_path_1, "test_1.txt")
        test_2 = os.path.join(archive_path_1, "test_1", "test_1.txt")
        assert os.path.exists(test_1), test_1
        assert os.path.exists(test_2), test_2
        with open(test_1, "rt") as f:
            message = f.read()
            assert message == "test_1\n", message
        with open(test_2, "rt") as f:
            message = f.read()
            assert message == "test_2\n", message

        # check url2
        archive_path_2 = archive_uris["url2"]
        test_1 = os.path.join(archive_path_2, "test_2.txt")
        test_2 = os.path.join(archive_path_2, "test_2", "test_2.txt")
        assert os.path.exists(test_1), test_1
        assert os.path.exists(test_2), test_2
        with open(test_1, "rt") as f:
            message = f.read()
            assert message == "test_1\n", message
        with open(test_2, "rt") as f:
            message = f.read()
            assert message == "test_2\n", message

        return True

    _, address = start_cluster
    # We need a working dir to verify the symbol link of archive files
    with tempfile.TemporaryDirectory() as working_dir:
        ray.init(address, runtime_env={"working_dir": working_dir})

        output = ray.get(
            check_file.options(
                runtime_env={
                    ARCHIVE_PLUGIN_NAME: {
                        "url1": archive_url_1,
                        "url2": archive_url_2,
                    }
                }
            ).remote()
        )
        assert output


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
