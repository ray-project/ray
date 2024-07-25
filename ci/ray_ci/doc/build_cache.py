import tempfile
import subprocess
import os
from typing import Set

import boto3

from ci.ray_ci.utils import logger
from ray_release.util import get_write_state_machine_aws_bucket


AWS_CACHE_KEY = "doc_build"


class BuildCache:
    """
    BuildCache represents the build artifacts generated from the doc build process,
    massaged to be used as a cache for the next build process
    """

    def __init__(self, cache_dir: str):
        """
        Args:
            cache_dir: The directory where the build artifacts are stored
        """
        self._cache_dir = cache_dir

    def upload(self) -> None:
        """
        Upload the build artifacts to S3
        """
        logger.info("Obtaining the list of cache files.")
        cache_files = self._get_cache()

        logger.info("Creating a tarball of the cache files.")
        doc_tarball = self._zip_cache(cache_files)

        logger.info("Upload the tarball to S3.")
        self._upload_cache(doc_tarball)

        logger.info(f"Successfully uploaded {doc_tarball} to S3.")

    def _get_cache(self) -> Set[str]:
        """
        Get the list of cache files
        """
        untracked_files = (
            subprocess.check_output(
                ["git", "ls-files", "--others", "-z"],
                cwd=self._cache_dir,
            )
            .decode("utf-8")
            .split(os.linesep)
        )

        return {file for file in untracked_files if file}

    def _zip_cache(self, cache_files: Set[str]) -> str:
        """
        Create a tarball of the cache files
        """
        with tempfile.NamedTemporaryFile(mode="w+t") as temp_file:
            temp_file.write("\n".join(cache_files))
            doc_tarball = f'{os.environ["BUILDKITE_COMMIT"]}.tgz'
            doc_tarball_path = os.path.join(self._cache_dir, doc_tarball)
            subprocess.run(
                ["tar", "-cvzf", doc_tarball_path, "-T", temp_file.name],
                cwd=self._cache_dir,
            )

            return doc_tarball

    def _upload_cache(self, doc_tarball: str) -> None:
        boto3.client("s3").upload_file(
            os.path.join(self._cache_dir, doc_tarball),
            get_write_state_machine_aws_bucket(),
            f"{AWS_CACHE_KEY}/{doc_tarball}",
        )
