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
from ray._private.runtime_env.packaging import get_local_dir_from_uri, is_tar_uri

ARCHIVE_PLUGIN_CLASS_PATH = (
    "ray._private.runtime_env.archive.DownloadAndUnpackArchivePlugin"  # noqa: E501
)
ARCHIVE_PLUGIN_NAME = "archives"

NATIVE_LIBRARIES_CLASS_PATH = (
    "ray._private.runtime_env.native_libraries.NativeLibrariesPlugin"  # noqa: E501
)
NATIVE_LIBRARIES_PLUGIN_NAME = "native_libraries"

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


test_local_dir = None


@pytest.mark.parametrize(
    "set_url",
    [
        "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_native_libraries.tar",  # noqa: E501
        "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_native_libraries.tar.bz",  # noqa: E501
        "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_native_libraries.tar.gz",  # noqa: E501
        "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_native_libraries.tar.xz",  # noqa: E501
    ],
)
def test_get_local_dir_from_tar_url(set_url):
    url = set_url
    assert is_tar_uri(url)
    per_local_dir = get_local_dir_from_uri(url, "/tmp/ray/runtime_resources")
    global test_local_dir
    if test_local_dir is None:
        test_local_dir = per_local_dir
    else:
        assert test_local_dir == per_local_dir


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + ARCHIVE_PLUGIN_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "set_url",
    [
        "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_native_libraries.tar",  # noqa: E501
        "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_native_libraries.tar.bz",  # noqa: E501
        "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_native_libraries.tar.gz",  # noqa: E501
        "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_native_libraries.tar.xz",  # noqa: E501
    ],
)
def test_tar_package_for_runtime_env(
    set_runtime_env_plugins, set_url, ray_start_regular
):
    @ray.remote
    class Test_Actor:
        def __init__(self):
            self._count = 0

        def get_count(self):
            return self._count

        def get_archive_path(self):
            return get_archives_context()

    session_dir = ray_start_regular.address_info["session_dir"]
    url = set_url
    a = Test_Actor.options(
        runtime_env={
            ARCHIVE_PLUGIN_NAME: set_url,
        }
    ).remote()
    assert ray.get(a.get_count.remote()) == 0
    archive_path = ray.get(a.get_archive_path.remote())
    archive_file_dir = os.path.join(session_dir, "runtime_resources/archive_files")
    local_dir = get_local_dir_from_uri(url, archive_file_dir)
    assert os.path.exists(local_dir), local_dir
    assert str(local_dir) == archive_path


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"' + NATIVE_LIBRARIES_CLASS_PATH + '"}]',
    ],
    indirect=True,
)
def test_native_libraries_for_runtime_env(set_runtime_env_plugins, ray_start_regular):

    # test_internal_native_libraries.tar
    #     - libha3.so
    native_libraries_url = "https://github.com/antgroup/ant-ray/raw/refs/heads/ci_deps/runtime_env/test_internal_native_libraries.tar.gz"  # noqa: E501

    @ray.remote
    class Test_Actor:
        def __init__(self):
            self._count = 0

        def get_count(self):
            return self._count

    session_dir = ray_start_regular.address_info["session_dir"]
    url = native_libraries_url
    a = Test_Actor.options(
        runtime_env={
            NATIVE_LIBRARIES_PLUGIN_NAME: [
                {
                    "url": url,
                    "lib_path": ["./"],
                    "code_search_path": ["./"],
                }
            ]
        }
    ).remote()
    assert ray.get(a.get_count.remote()) == 0
    native_libraries_dir = os.path.join(
        session_dir, "runtime_resources/native_libraries_files"
    )
    local_dir = get_local_dir_from_uri(url, native_libraries_dir)
    assert os.path.exists(local_dir), local_dir
    local_file_path = os.path.join(local_dir, "libha3.so")
    assert os.path.exists(local_file_path), local_file_path


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
