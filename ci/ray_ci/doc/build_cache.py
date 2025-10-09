import os
import pickle
import subprocess
import tempfile
from typing import Set

import boto3
from ray_release.util import get_write_state_machine_aws_bucket

from ci.ray_ci.utils import logger

AWS_CACHE_KEY = "doc_build"
ENVIRONMENT_PICKLE = "_build/doctrees/environment.pickle"


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

    def upload(self, dry_run: bool) -> None:
        """
        Upload the build artifacts to S3
        """
        logger.info("Massage the build artifacts to be used as a cache.")
        self._massage_cache(ENVIRONMENT_PICKLE)

        logger.info("Obtaining the list of cache files.")
        cache_files = self._get_cache()

        logger.info("Creating a tarball of the cache files.")
        doc_tarball = self._zip_cache(cache_files)

        if dry_run:
            logger.info(f"Skipping upload of {doc_tarball} to S3.")
            return

        logger.info("Upload the tarball to S3.")
        self._upload_cache(doc_tarball)

        logger.info(f"Successfully uploaded {doc_tarball} to S3.")

    def _massage_cache(self, environment_cache_file: str) -> None:
        """
        Massage the build artifacts, remove the unnecessary files so that they can
        be used as a global cache
        """
        environment_cache_path = os.path.join(self._cache_dir, environment_cache_file)
        environment_cache = None

        with open(environment_cache_path, "rb") as f:
            environment_cache = pickle.load(f)
            for doc, dependencies in environment_cache.dependencies.items():
                # Remove the site-packages dependencies because they are local to the
                # build environment and cannot be used as a global cache
                local_dependencies = [d for d in dependencies if "site-packages" in d]
                for dependency in local_dependencies:
                    environment_cache.dependencies[doc].remove(dependency)

        with open(environment_cache_path, "wb+") as f:
            pickle.dump(environment_cache, f, pickle.HIGHEST_PROTOCOL)

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
