import os
import re
import shutil
import tempfile

from ray.ml.storage import Storage


class LocalTestStorage(Storage):
    """Local test storage to be used in tests.

    Uploads and downloads data to/from a local temporary directory.

    Example:

        from ray.ml.storage import register_storage, upload_to_bucket
        from ray.ml.utils.test_utils import LocalTestStorage

        register_storage("test://", LocalTestStorage())
        upload_to_bucket("local/file", "test://path/in/storage")

    """

    def __init__(self):
        self.tempdir = tempfile.mkdtemp()

    def _strip_prefix(self, remote_path: str) -> str:
        """Strip any kind of prefix://-prefix from remote path."""
        matched = re.match(r"\w+://(.+)", remote_path)
        if not matched:
            raise RuntimeError(f"Invalid URI: {matched}")
        return matched.group(1)

    def upload(self, local_source: str, remote_target: str) -> None:
        if not os.path.exists(local_source):
            raise RuntimeError(
                f"Cannot upload local_source as it does not exist on disk: "
                f"{local_source}"
            )

        relative_target = self._strip_prefix(remote_target)
        relative_target = relative_target.strip(" /")

        absolute_target = os.path.join(self.tempdir, relative_target)
        if os.path.isdir(local_source):
            if os.path.exists(absolute_target):
                shutil.rmtree(absolute_target)
            shutil.copytree(local_source, absolute_target)
        else:
            basedir = os.path.dirname(absolute_target)
            if not os.path.exists(basedir):
                os.makedirs(basedir, mode=0o755, exist_ok=True)
            if os.path.exists(absolute_target):
                os.remove(absolute_target)
            shutil.copyfile(local_source, absolute_target)

    def download(self, remote_source: str, local_target: str) -> None:
        relative_source = self._strip_prefix(remote_source)
        relative_source = relative_source.strip(" /")

        absolute_source = os.path.join(self.tempdir, relative_source)

        if not os.path.exists(absolute_source):
            raise RuntimeError(
                f"Cannot download remote_source as it does not exist on disk: "
                f"{remote_source}"
            )

        if os.path.isdir(absolute_source):
            if os.path.exists(local_target):
                shutil.rmtree(local_target)
            shutil.copytree(absolute_source, local_target)
        else:
            if os.path.exists(local_target):
                os.remove(local_target)
            shutil.copyfile(absolute_source, local_target)

    def delete(self, remote_target: str) -> None:
        relative_target = self._strip_prefix(remote_target)
        relative_target = relative_target.strip(" /")

        absolute_target = os.path.join(self.tempdir, relative_target)

        if not os.path.exists(absolute_target):
            raise RuntimeError(
                f"Cannot delete remote_target as it does not exist on disk: "
                f"{remote_target}"
            )

        shutil.rmtree(absolute_target)
